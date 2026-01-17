<?php

declare(strict_types=1);

namespace App\Jobs\Enrichment;

use App\Models\Image;
use App\Models\Retailer;
use App\Models\Video;
use App\Models\VideoGame;
use App\Services\Price\Steam\SteamStoreService;
use Illuminate\Bus\Queueable;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Bus\Dispatchable;
use Illuminate\Queue\InteractsWithQueue;
use Illuminate\Queue\Middleware\RateLimited;
use Illuminate\Queue\SerializesModels;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Log;

/**
 * Fetch combined price + media data from Steam in a single efficient API call.
 *
 * Supports both single and batch processing for maximum performance.
 * One API call returns: price, screenshots, movies, header image.
 */
class FetchSteamDataJob implements ShouldQueue
{
    use Dispatchable, InteractsWithQueue, Queueable, SerializesModels;

    private const TARGET_REGIONS = ['US', 'GB', 'DE', 'JP', 'BR', 'CA', 'AU'];

    public int $tries = 3;

    /** @var array<int, int> */
    public array $backoff = [30, 120, 300];

    /**
     * @param  int  $videoGameId  The video game to enrich
     * @param  int  $steamAppId  The Steam app ID
     * @param  array<string>  $regions  Target regions for pricing (empty = all)
     */
    public function __construct(
        public int $videoGameId,
        public int $steamAppId,
        public array $regions = []
    ) {
        $this->regions = empty($regions) ? self::TARGET_REGIONS : $regions;
    }

    /**
     * @return array<int, object>
     */
    public function middleware(): array
    {
        return [new RateLimited('steam')];
    }

    public function handle(SteamStoreService $steam): void
    {
        $game = VideoGame::find($this->videoGameId);

        if (! $game) {
            Log::warning('FetchSteamDataJob: Game not found', ['game_id' => $this->videoGameId]);

            return;
        }

        // Fetch full data from Steam (price + media) - ONE API call
        $data = $steam->getFullDetails((string) $this->steamAppId, $this->regions[0] ?? 'US');

        if (! $data) {
            Log::info('FetchSteamDataJob: No data returned from Steam', [
                'game_id' => $this->videoGameId,
                'steam_app_id' => $this->steamAppId,
            ]);

            return;
        }

        // Store everything in parallel using DB transaction for consistency
        DB::transaction(function () use ($game, $data, $steam) {
            // 1. Store prices for all target regions (batch if price exists)
            if ($data['price']) {
                $this->storePrices($game, $data['price'], $steam);
            }

            // 2. Store media (images + videos)
            $this->storeMedia($game, $data['media']);
        });

        Log::info('FetchSteamDataJob: Complete', [
            'game_id' => $this->videoGameId,
            'steam_app_id' => $this->steamAppId,
            'has_price' => $data['price'] !== null,
            'screenshots' => count($data['media']['screenshots'] ?? []),
            'movies' => count($data['media']['movies'] ?? []),
        ]);
    }

    /**
     * Store prices for all target regions.
     * Uses bulk upsert for efficiency.
     */
    private function storePrices(VideoGame $game, array $priceData, SteamStoreService $steam): void
    {
        $retailer = Retailer::where('slug', 'steam')->first();

        if (! $retailer) {
            return;
        }

        $url = "https://store.steampowered.com/app/{$this->steamAppId}/";
        $now = now();

        // For first region, we already have the data
        $priceRows = [];
        $firstRegion = $this->regions[0] ?? 'US';

        $priceRows[] = [
            'video_game_id' => $game->id,
            'currency' => $priceData['currency'],
            'country_code' => $firstRegion,
            'amount_minor' => $priceData['amount_minor'],
            'retailer' => $retailer->name,
            'url' => $url,
            'recorded_at' => $now,
            'is_active' => true,
            'metadata' => json_encode([
                'discount_percent' => $priceData['discount_percent'] ?? 0,
                'initial_amount_minor' => $priceData['initial_amount_minor'] ?? null,
                'steam_app_id' => $this->steamAppId,
            ]),
            'updated_at' => $now,
        ];

        // Fetch prices for remaining regions (if multiple regions requested)
        foreach (array_slice($this->regions, 1) as $region) {
            $regionalPrice = $steam->getPrice((string) $this->steamAppId, $region);

            if ($regionalPrice) {
                $priceRows[] = [
                    'video_game_id' => $game->id,
                    'currency' => $regionalPrice['currency'],
                    'country_code' => $region,
                    'amount_minor' => $regionalPrice['amount_minor'],
                    'retailer' => $retailer->name,
                    'url' => $url,
                    'recorded_at' => $now,
                    'is_active' => true,
                    'metadata' => json_encode(['steam_app_id' => $this->steamAppId]),
                    'updated_at' => $now,
                ];
            }
        }

        // Bulk upsert all prices
        if (! empty($priceRows)) {
            DB::table('video_game_prices')->upsert(
                $priceRows,
                ['video_game_id', 'retailer', 'country_code'], // unique keys
                ['currency', 'amount_minor', 'recorded_at', 'is_active', 'metadata', 'updated_at']
            );
        }
    }

    /**
     * Store media (screenshots + movies) in a single batch.
     */
    private function storeMedia(VideoGame $game, array $media): void
    {
        $screenshots = $media['screenshots'] ?? [];
        $movies = $media['movies'] ?? [];
        $headerImage = $media['header_image'] ?? null;
        $capsuleImage = $media['capsule_image'] ?? null;

        // Build image URLs and details
        $urls = [];
        $details = [];
        $collections = [];

        // Add header/capsule as cover
        if ($headerImage) {
            $collections[] = 'cover_images';
            $urls[] = $headerImage;
            $details[] = [
                'collection' => 'cover_images',
                'url' => $headerImage,
                'type' => 'header_image',
            ];
        }

        if ($capsuleImage) {
            $collections[] = 'cover_images';
            $urls[] = $capsuleImage;
            $details[] = [
                'collection' => 'cover_images',
                'url' => $capsuleImage,
                'type' => 'capsule_image',
            ];
        }

        // Add screenshots
        foreach ($screenshots as $screenshot) {
            $collections[] = 'screenshots';
            $urls[] = $screenshot['full'];
            $details[] = [
                'collection' => 'screenshots',
                'url' => $screenshot['full'],
                'thumbnail' => $screenshot['thumbnail'],
                'steam_id' => $screenshot['id'],
            ];
        }

        $collections = array_values(array_unique($collections));

        // Upsert Image record if we have any images
        if (! empty($urls)) {
            Image::updateOrCreate(
                [
                    'video_game_id' => $game->id,
                    'imageable_type' => VideoGame::class,
                    'imageable_id' => $game->id,
                ],
                [
                    'provider' => 'steam',
                    'primary_collection' => in_array('cover_images', $collections, true) ? 'cover_images' : ($collections[0] ?? 'misc'),
                    'collection_names' => $collections,
                    'url' => $urls[0] ?? null,
                    'urls' => $urls,
                    'metadata' => [
                        'source' => 'steam_enrichment',
                        'steam_app_id' => $this->steamAppId,
                        'details' => $details,
                    ],
                ]
            );
        }

        // Store videos if present
        if (! empty($movies)) {
            $videoUrls = [];
            $videoDetails = [];

            foreach ($movies as $movie) {
                // Prefer MP4 max quality
                $videoUrl = $movie['mp4_max'] ?? $movie['webm_max'] ?? $movie['mp4_480'] ?? $movie['webm_480'];
                if ($videoUrl) {
                    $videoUrls[] = $videoUrl;
                    $videoDetails[] = [
                        'name' => $movie['name'],
                        'thumbnail' => $movie['thumbnail'],
                        'steam_id' => $movie['id'],
                        'webm_480' => $movie['webm_480'],
                        'webm_max' => $movie['webm_max'],
                        'mp4_480' => $movie['mp4_480'],
                        'mp4_max' => $movie['mp4_max'],
                    ];
                }
            }

            if (! empty($videoUrls)) {
                Video::updateOrCreate(
                    [
                        'video_game_id' => $game->id,
                        'videoable_type' => VideoGame::class,
                        'videoable_id' => $game->id,
                    ],
                    [
                        'provider' => 'steam',
                        'primary_collection' => 'trailers',
                        'collection_names' => ['trailers'],
                        'url' => $videoUrls[0] ?? null,
                        'urls' => $videoUrls,
                        'video_id' => (string) ($movies[0]['id'] ?? ''),
                        'thumbnail_url' => $movies[0]['thumbnail'] ?? null,
                        'title' => $movies[0]['name'] ?? null,
                        'metadata' => [
                            'source' => 'steam_enrichment',
                            'steam_app_id' => $this->steamAppId,
                            'videos' => $videoDetails,
                        ],
                    ]
                );
            }
        }
    }
}
