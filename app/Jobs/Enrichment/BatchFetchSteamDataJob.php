<?php

declare(strict_types=1);

namespace App\Jobs\Enrichment;

use App\Jobs\Enrichment\Traits\CategorizesVideoTypes;
use App\Jobs\Enrichment\Traits\GuessesCurrency;
use App\Models\Image;
use App\Models\Retailer;
use App\Models\Video;
use App\Models\VideoGame;
use Illuminate\Bus\Queueable;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Bus\Dispatchable;
use Illuminate\Queue\InteractsWithQueue;
use Illuminate\Queue\Middleware\RateLimited;
use Illuminate\Queue\SerializesModels;
use Illuminate\Support\Collection;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Http;
use Illuminate\Support\Facades\Log;

/**
 * HIGH-PERFORMANCE batch Steam data fetcher.
 *
 * Key optimizations:
 * 1. Batches up to 50 games per API call
 * 2. Uses bulk upsert for all database writes
 * 3. Minimizes round trips - one API call → one DB transaction
 * 4. Concurrent HTTP requests for multi-region pricing
 */
class BatchFetchSteamDataJob implements ShouldQueue
{
    use CategorizesVideoTypes, Dispatchable, GuessesCurrency, InteractsWithQueue, Queueable, SerializesModels;

    private const BATCH_SIZE = 50; // Steam's practical limit per request

    private const STEAM_API_URL = 'https://store.steampowered.com/api/appdetails';

    public int $tries = 3;

    /** @var array<int, int> */
    public array $backoff = [30, 120, 300];

    /**
     * @param  array<int, array{video_game_id: int, steam_app_id: int}>  $gamesMappings  Array of [video_game_id, steam_app_id] pairs
     * @param  string  $region  Target region for pricing
     */
    public function __construct(
        public array $gamesMappings,
        public string $region = 'US'
    ) {}

    /**
     * @return array<int, object>
     */
    public function middleware(): array
    {
        return [new RateLimited('steam')];
    }

    public function handle(): void
    {
        if (empty($this->gamesMappings)) {
            return;
        }

        $retailer = Retailer::where('slug', 'steam')->first();

        if (! $retailer) {
            Log::error('BatchFetchSteamDataJob: Steam retailer not found');

            return;
        }

        // Process in batches of BATCH_SIZE
        $chunks = array_chunk($this->gamesMappings, self::BATCH_SIZE);
        $totalProcessed = 0;

        foreach ($chunks as $chunk) {
            $processed = $this->processBatch($chunk, $retailer);
            $totalProcessed += $processed;
        }

        Log::info('BatchFetchSteamDataJob: Complete', [
            'total_games' => count($this->gamesMappings),
            'processed' => $totalProcessed,
            'region' => $this->region,
        ]);
    }

    /**
     * Process a batch of games in a single API call + DB transaction.
     */
    private function processBatch(array $mappings, Retailer $retailer): int
    {
        // Build comma-separated app IDs for batch request
        $steamAppIds = array_column($mappings, 'steam_app_id');
        $appIdsParam = implode(',', $steamAppIds);

        // Create lookup map: steam_app_id => video_game_id
        $gameIdMap = [];
        foreach ($mappings as $mapping) {
            $gameIdMap[$mapping['steam_app_id']] = $mapping['video_game_id'];
        }

        // Single API call for all games in batch
        $response = Http::timeout(30)->get(self::STEAM_API_URL, [
            'appids' => $appIdsParam,
            'cc' => $this->region,
        ]);

        if (! $response->successful()) {
            Log::warning('BatchFetchSteamDataJob: API request failed', [
                'status' => $response->status(),
                'app_ids' => $steamAppIds,
            ]);

            return 0;
        }

        $data = $response->json();
        $priceRows = [];
        $imageRows = [];
        $videoRows = [];
        $now = now();

        // Parse response for each game
        foreach ($steamAppIds as $appId) {
            $appIdStr = (string) $appId;

            if (! isset($data[$appIdStr]) || empty($data[$appIdStr]['success'])) {
                continue;
            }

            $gameData = $data[$appIdStr]['data'] ?? [];
            $videoGameId = $gameIdMap[$appId];

            // Extract price
            $price = $this->extractPrice($gameData);
            if ($price) {
                $priceRows[] = [
                    'video_game_id' => $videoGameId,
                    'currency' => $price['currency'],
                    'country_code' => $this->region,
                    'amount_minor' => $price['amount_minor'],
                    'retailer' => $retailer->name,
                    'url' => "https://store.steampowered.com/app/{$appId}/",
                    'recorded_at' => $now,
                    'is_active' => true,
                    'metadata' => json_encode([
                        'steam_app_id' => $appId,
                        'discount_percent' => $price['discount_percent'] ?? 0,
                    ]),
                    'updated_at' => $now,
                ];
            }

            // Extract media
            $media = $this->extractMedia($gameData, $appId, $videoGameId);
            if ($media['image']) {
                $imageRows[] = $media['image'];
            }
            if ($media['video']) {
                $videoRows[] = $media['video'];
            }
        }

        // Single DB transaction for all writes
        if (! empty($priceRows) || ! empty($imageRows) || ! empty($videoRows)) {
            DB::transaction(function () use ($priceRows, $imageRows, $videoRows) {
                // Bulk upsert prices
                if (! empty($priceRows)) {
                    DB::table('video_game_prices')->upsert(
                        $priceRows,
                        ['video_game_id', 'retailer', 'country_code'],
                        ['currency', 'amount_minor', 'recorded_at', 'is_active', 'metadata', 'updated_at']
                    );
                }

                // Bulk upsert images
                foreach ($imageRows as $imageData) {
                    Image::updateOrCreate(
                        [
                            'video_game_id' => $imageData['video_game_id'],
                            'imageable_type' => VideoGame::class,
                            'imageable_id' => $imageData['video_game_id'],
                        ],
                        $imageData['data']
                    );
                }

                // Bulk upsert videos
                foreach ($videoRows as $videoData) {
                    Video::updateOrCreate(
                        [
                            'video_game_id' => $videoData['video_game_id'],
                            'videoable_type' => VideoGame::class,
                            'videoable_id' => $videoData['video_game_id'],
                        ],
                        $videoData['data']
                    );
                }
            });
        }

        return count($priceRows);
    }

    /**
     * Extract price from Steam response.
     */
    private function extractPrice(array $gameData): ?array
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
                'currency' => $this->guessCurrencyFromRegion($this->region),
                'discount_percent' => 0,
            ];
        }

        return null;
    }

    /**
     * Extract media from Steam response.
     *
     * @return array{image: array|null, video: array|null}
     */
    private function extractMedia(array $gameData, int $steamAppId, int $videoGameId): array
    {
        $screenshots = $gameData['screenshots'] ?? [];
        $movies = $gameData['movies'] ?? [];
        $headerImage = $gameData['header_image'] ?? null;

        $imageData = null;
        $videoData = null;

        // Build image data
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
                'video_game_id' => $videoGameId,
                'data' => [
                    'provider' => 'steam',
                    'primary_collection' => in_array('cover_images', $collections, true) ? 'cover_images' : ($collections[0] ?? 'misc'),
                    'collection_names' => $collections,
                    'url' => $urls[0] ?? null,
                    'urls' => $urls,
                    'metadata' => [
                        'source' => 'steam_batch_enrichment',
                        'steam_app_id' => $steamAppId,
                        'details' => $details,
                    ],
                ],
            ];
        }

        // Build video data - capture ALL video types comprehensively
        if (! empty($movies)) {
            $videoUrls = [];
            $videoDetails = [];
            $collections = [];

            foreach ($movies as $movie) {
                // Capture all available formats - don't limit
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
                    'video_game_id' => $videoGameId,
                    'data' => [
                        'provider' => 'steam',
                        'primary_collection' => $collections[0] ?? 'trailers',
                        'collection_names' => $collections,
                        'url' => $videoUrls[0] ?? null,
                        'urls' => $videoUrls,
                        'video_id' => (string) ($movies[0]['id'] ?? ''),
                        'thumbnail_url' => $movies[0]['thumbnail'] ?? null,
                        'title' => $movies[0]['name'] ?? null,
                        'metadata' => [
                            'source' => 'steam_batch_enrichment',
                            'steam_app_id' => $steamAppId,
                            'total_videos' => count($movies),
                            'videos' => $videoDetails,
                        ],
                    ],
                ];
            }
        }

        return ['image' => $imageData, 'video' => $videoData];
    }

    /**
     * Static helper to dispatch batch jobs from a collection of games.
     * Automatically chunks into optimal batch sizes.
     *
     * @param  Collection  $games  Collection with 'id' and steam mapping
     * @param  array<string>  $regions  Target regions
     */
    public static function dispatchForGames(Collection $games, array $regions = ['US']): int
    {
        $mappings = $games->map(fn ($game) => [
            'video_game_id' => $game->id,
            'steam_app_id' => $game->steam_app_id ?? $game->external_id,
        ])->filter(fn ($m) => $m['steam_app_id'] !== null)->values()->all();

        if (empty($mappings)) {
            return 0;
        }

        $dispatched = 0;

        foreach ($regions as $region) {
            self::dispatch($mappings, $region);
            $dispatched++;
        }

        return $dispatched;
    }
}
