<?php

declare(strict_types=1);

namespace App\Jobs\Enrichment;

use App\Jobs\Enrichment\Traits\CategorizesVideoTypes;
use App\Jobs\Enrichment\Traits\GuessesCurrency;
use App\Models\Country;
use App\Models\Image;
use App\Models\Video;
use App\Models\VideoGame;
use Illuminate\Bus\Queueable;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Bus\Dispatchable;
use Illuminate\Queue\InteractsWithQueue;
use Illuminate\Queue\Middleware\RateLimited;
use Illuminate\Queue\SerializesModels;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Http;
use Illuminate\Support\Facades\Log;

/**
 * CONCURRENT multi-region Steam data fetcher.
 *
 * Uses HTTP::pool() for parallel requests across all regions.
 * Maximum throughput: 7 regions fetched concurrently in ~1 API round-trip time.
 *
 * Performance characteristics:
 * - One job handles ALL regions concurrently via HTTP pooling
 * - Single DB transaction for all writes (prices + media)
 * - ~7x faster than sequential region fetching
 */
class ConcurrentFetchSteamDataJob implements ShouldQueue
{
    use CategorizesVideoTypes, Dispatchable, GuessesCurrency, InteractsWithQueue, Queueable, SerializesModels;

    private const FALLBACK_REGIONS = ['US', 'GB', 'DE', 'JP', 'BR', 'CA', 'AU'];

    private const STEAM_API_URL = 'https://store.steampowered.com/api/appdetails';

    public int $tries = 3;

    /** @var array<int, int> */
    public array $backoff = [30, 120, 300];

    public function __construct(
        public int $videoGameId,
        public int $steamAppId,
        public array $regions = []
    ) {
        if (! empty($regions)) {
            $this->regions = $regions;

            return;
        }

        $cfg = (string) config('services.steam.regions', '');
        $fromCfg = array_values(array_filter(array_map('trim', explode(',', $cfg))));
        $fromCfg = array_values(array_unique(array_map(static function (string $value): string {
            $value = trim($value);
            if ($value === '') {
                return '';
            }

            // Accept locale style (en-us) and country style (US).
            if (str_contains($value, '-')) {
                $parts = explode('-', $value);
                $value = (string) end($parts);
            }

            return strtoupper($value);
        }, $fromCfg)));
        $fromCfg = array_values(array_filter($fromCfg));

        if ($fromCfg !== []) {
            $this->regions = $fromCfg;

            return;
        }

        // Default: pull first 15 ISO2 country codes from DB.
        try {
            $iso2 = Country::query()
                ->select(['code'])
                ->whereRaw('length(code) = 2')
                ->orderBy('code')
                ->limit(15)
                ->pluck('code')
                ->map(fn (string $cc) => strtoupper($cc))
                ->all();

            $this->regions = $iso2 !== [] ? $iso2 : self::FALLBACK_REGIONS;
        } catch (\Throwable) {
            $this->regions = self::FALLBACK_REGIONS;
        }
    }

    /**
     * @return array<int, object>
     */
    public function middleware(): array
    {
        return [new RateLimited('steam')];
    }

    public function handle(): void
    {
        $game = VideoGame::with('title.product')->find($this->videoGameId);

        if (! $game) {
            return;
        }

        $retailerName = 'Steam';
        $productId = $game->title?->product_id;

        // CONCURRENT HTTP requests for ALL regions at once
        $responses = Http::pool(fn ($pool) => collect($this->regions)->map(
            fn ($region) => $pool
                ->as($region)
                ->timeout(30)
                ->get(self::STEAM_API_URL, [
                    'appids' => $this->steamAppId,
                    'cc' => $region,
                ])
        )->all());

        $priceRows = [];
        $mediaExtracted = false;
        $imageData = null;
        $videoData = null;
        $now = now();
        $appIdStr = (string) $this->steamAppId;

        foreach ($this->regions as $region) {
            $response = $responses[$region] ?? null;

            if (! $response || ! $response->successful()) {
                continue;
            }

            $data = $response->json();

            if (! isset($data[$appIdStr]) || empty($data[$appIdStr]['success'])) {
                continue;
            }

            $gameData = $data[$appIdStr]['data'] ?? [];

            // Extract price for this region
            $price = $this->extractPrice($gameData, $region);
            if ($price) {
                $priceRows[] = [
                    'video_game_id' => $this->videoGameId,
                    'product_id' => $productId,
                    'currency' => $price['currency'],
                    'country_code' => $region,
                    'region_code' => $region,
                    'condition' => 'digital',
                    'sku' => null,
                    'amount_minor' => $price['amount_minor'],
                    'retailer' => $retailerName,
                    'url' => "https://store.steampowered.com/app/{$this->steamAppId}/",
                    'recorded_at' => $now,
                    'is_active' => true,
                    'bucket' => 'snapshot',
                    'metadata' => json_encode([
                        'steam_app_id' => $this->steamAppId,
                        'discount_percent' => $price['discount_percent'] ?? 0,
                    ]),
                    'created_at' => $now,
                    'updated_at' => $now,
                ];
            }

            // Extract media only once (same across regions)
            if (! $mediaExtracted) {
                $media = $this->extractMedia($gameData);
                $imageData = $media['image'];
                $videoData = $media['video'];
                $mediaExtracted = true;
            }
        }

        // Single transaction for ALL writes
        if (! empty($priceRows) || $imageData || $videoData) {
            DB::transaction(function () use ($game, $priceRows, $imageData, $videoData) {
                if (! empty($priceRows)) {
                    DB::table('video_game_prices')->upsert(
                        $priceRows,
                        ['video_game_id', 'retailer', 'country_code'],
                        ['product_id', 'region_code', 'condition', 'sku', 'currency', 'amount_minor', 'recorded_at', 'is_active', 'bucket', 'metadata', 'updated_at']
                    );
                }

                if ($imageData) {
                    Image::updateOrCreate(
                        [
                            'video_game_id' => $game->id,
                            'imageable_type' => VideoGame::class,
                            'imageable_id' => $game->id,
                        ],
                        $imageData
                    );
                }

                if ($videoData) {
                    Video::updateOrCreate(
                        [
                            'video_game_id' => $game->id,
                            'videoable_type' => VideoGame::class,
                            'videoable_id' => $game->id,
                        ],
                        $videoData
                    );
                }
            });
        }

        if ($game->title?->product) {
            $game->title->product->refreshPricingSnapshot();
        }

        Log::info('ConcurrentFetchSteamDataJob: Complete', [
            'game_id' => $this->videoGameId,
            'steam_app_id' => $this->steamAppId,
            'prices_stored' => count($priceRows),
            'regions' => $this->regions,
        ]);
    }

    private function extractPrice(array $gameData, string $region): ?array
    {
        $priceOverview = $gameData['price_overview'] ?? null;

        if ($priceOverview) {
            return [
                'amount_minor' => (int) $priceOverview['final'],
                'currency' => $priceOverview['currency'],
                'discount_percent' => $priceOverview['discount_percent'] ?? 0,
            ];
        }

        if (! empty($gameData['is_free'])) {
            return [
                'amount_minor' => 0,
                'currency' => $this->guessCurrencyFromRegion($region),
                'discount_percent' => 0,
            ];
        }

        return null;
    }

    private function extractMedia(array $gameData): array
    {
        $screenshots = $gameData['screenshots'] ?? [];
        $movies = $gameData['movies'] ?? [];
        $headerImage = $gameData['header_image'] ?? null;

        $imageData = null;
        $videoData = null;

        // Build comprehensive image data
        if ($headerImage || ! empty($screenshots)) {
            $urls = [];
            $details = [];
            $collections = [];

            if ($headerImage) {
                $collections[] = 'cover_images';
                $urls[] = $headerImage;
                $details[] = ['collection' => 'cover_images', 'url' => $headerImage];
            }

            foreach ($screenshots as $screenshot) {
                $collections[] = 'screenshots';
                $urls[] = $screenshot['path_full'] ?? $screenshot['path_thumbnail'];
                $details[] = [
                    'collection' => 'screenshots',
                    'url' => $screenshot['path_full'] ?? null,
                    'thumbnail' => $screenshot['path_thumbnail'] ?? null,
                ];
            }

            $collections = array_values(array_unique($collections));

            $imageData = [
                'provider' => 'steam',
                'primary_collection' => in_array('cover_images', $collections, true) ? 'cover_images' : ($collections[0] ?? 'misc'),
                'collection_names' => $collections,
                'url' => $urls[0] ?? null,
                'urls' => $urls,
                'metadata' => [
                    'source' => 'steam_concurrent',
                    'steam_app_id' => $this->steamAppId,
                    'details' => $details,
                ],
            ];
        }

        // Build comprehensive video data - capture ALL formats and categorize
        if (! empty($movies)) {
            $videoUrls = [];
            $videoDetails = [];
            $collections = [];

            foreach ($movies as $movie) {
                // Capture all available formats
                $formats = [
                    'mp4_max' => $movie['mp4']['max'] ?? null,
                    'mp4_480' => $movie['mp4']['480'] ?? null,
                    'webm_max' => $movie['webm']['max'] ?? null,
                    'webm_480' => $movie['webm']['480'] ?? null,
                ];

                // Use best available format
                $videoUrl = $formats['mp4_max'] ?? $formats['webm_max'] ?? $formats['mp4_480'] ?? $formats['webm_480'];

                if (! $videoUrl) {
                    continue;
                }

                // Categorize by video name/type
                $name = $movie['name'] ?? '';
                $videoType = $this->categorizeVideoType($name);
                $collections[] = $videoType;

                $videoUrls[] = $videoUrl;
                $videoDetails[] = [
                    'steam_id' => $movie['id'] ?? null,
                    'name' => $name,
                    'type' => $videoType,
                    'thumbnail' => $movie['thumbnail'] ?? null,
                    'highlight' => $movie['highlight'] ?? false,
                    'formats' => array_filter($formats),
                ];
            }

            if (! empty($videoUrls)) {
                $collections = array_values(array_unique($collections));

                $videoData = [
                    'provider' => 'steam',
                    'primary_collection' => $collections[0] ?? 'trailers',
                    'collection_names' => $collections,
                    'url' => $videoUrls[0] ?? null,
                    'urls' => $videoUrls,
                    'video_id' => (string) ($movies[0]['id'] ?? ''),
                    'thumbnail_url' => $movies[0]['thumbnail'] ?? null,
                    'title' => $movies[0]['name'] ?? null,
                    'metadata' => [
                        'source' => 'steam_concurrent',
                        'steam_app_id' => $this->steamAppId,
                        'total_videos' => count($movies),
                        'videos' => $videoDetails,
                    ],
                ];
            }
        }

        return ['image' => $imageData, 'video' => $videoData];
    }
}
