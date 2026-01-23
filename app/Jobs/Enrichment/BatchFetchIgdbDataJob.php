<?php

declare(strict_types=1);

namespace App\Jobs\Enrichment;

use App\Jobs\Enrichment\Traits\BuildsIgdbImageUrls;
use App\Jobs\Enrichment\Traits\CategorizesVideoTypes;
use App\Jobs\Enrichment\Traits\ExtractsStoreUrls;
use App\Models\Image;
use App\Models\Video;
use App\Models\VideoGame;
use App\Models\VideoGameTitleSource;
use Illuminate\Bus\Queueable;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Bus\Dispatchable;
use Illuminate\Queue\InteractsWithQueue;
use Illuminate\Queue\Middleware\RateLimited;
use Illuminate\Queue\SerializesModels;
use Illuminate\Support\Collection;
use Illuminate\Support\Facades\Cache;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Http;
use Illuminate\Support\Facades\Log;

/**
 * HIGH-PERFORMANCE batch IGDB data fetcher.
 *
 * Leverages IGDB business partnership for bulk data access.
 * Single API call returns data for up to 500 games.
 *
 * Key optimizations:
 * 1. Batches up to 500 games per API call (IGDB limit)
 * 2. Uses bulk upsert for all database writes
 * 3. Extracts store URLs for cascading price discovery
 * 4. Processes all media types: covers, screenshots, artworks, videos
 */
class BatchFetchIgdbDataJob implements ShouldQueue
{
    use BuildsIgdbImageUrls, CategorizesVideoTypes, Dispatchable, ExtractsStoreUrls, InteractsWithQueue, Queueable, SerializesModels;

    private const BATCH_SIZE = 500; // IGDB's limit per request

    private const BASE_URL = 'https://api.igdb.com/v4';

    private const TOKEN_CACHE_KEY = 'igdb_oauth_token';

    public int $tries = 3;

    /** @var array<int, int> */
    public array $backoff = [30, 120, 300];

    /**
     * @param  array<int, array{video_game_id: int, igdb_id: int}>  $gamesMappings
     */
    public function __construct(
        public array $gamesMappings
    ) {}

    /**
     * @return array<int, object>
     */
    public function middleware(): array
    {
        return [new RateLimited('igdb')];
    }

    public function handle(): void
    {
        if (empty($this->gamesMappings)) {
            return;
        }

        $token = $this->getAccessToken();

        if (! $token) {
            Log::error('BatchFetchIgdbDataJob: Failed to obtain access token');

            return;
        }

        // Process in batches of BATCH_SIZE
        $chunks = array_chunk($this->gamesMappings, self::BATCH_SIZE);
        $totalProcessed = 0;
        $storesDiscovered = 0;

        foreach ($chunks as $chunk) {
            [$processed, $stores] = $this->processBatch($chunk, $token);
            $totalProcessed += $processed;
            $storesDiscovered += $stores;
        }

        Log::info('BatchFetchIgdbDataJob: Complete', [
            'total_games' => count($this->gamesMappings),
            'processed' => $totalProcessed,
            'stores_discovered' => $storesDiscovered,
        ]);
    }

    /**
     * Process a batch of games in a single API call + DB transaction.
     *
     * @return array{0: int, 1: int} [processed_count, stores_discovered]
     */
    private function processBatch(array $mappings, string $token): array
    {
        // Build IGDB IDs for batch request
        $igdbIds = array_column($mappings, 'igdb_id');

        // Create lookup map: igdb_id => video_game_id
        $gameIdMap = [];
        foreach ($mappings as $mapping) {
            $gameIdMap[$mapping['igdb_id']] = $mapping['video_game_id'];
        }

        // Build and execute query
        $query = $this->buildBatchQuery($igdbIds);

        $response = Http::withHeaders([
            'Client-ID' => config('services.igdb.client_id'),
            'Authorization' => "Bearer {$token}",
        ])->withBody($query, 'text/plain')->post(self::BASE_URL.'/games');

        if (! $response->successful()) {
            Log::warning('BatchFetchIgdbDataJob: API request failed', [
                'status' => $response->status(),
                'igdb_ids_count' => count($igdbIds),
            ]);

            return [0, 0];
        }

        $games = $response->json();

        if (empty($games)) {
            return [0, 0];
        }

        $imageRows = [];
        $videoRows = [];
        $sourceRows = [];
        $priceJobsToDispatch = [];

        // Parse response for each game
        foreach ($games as $gameData) {
            $igdbId = $gameData['id'] ?? null;

            if (! $igdbId || ! isset($gameIdMap[$igdbId])) {
                continue;
            }

            $videoGameId = $gameIdMap[$igdbId];

            // Extract media
            $media = $this->extractMedia($gameData, $igdbId, $videoGameId);

            if ($media['image']) {
                $imageRows[] = $media['image'];
            }
            if ($media['video']) {
                $videoRows[] = $media['video'];
            }

            // Extract store URLs for price discovery
            $stores = $this->extractStores($gameData, $videoGameId);
            foreach ($stores as $store) {
                $sourceRows[] = $store['source'];
                if ($store['price_job']) {
                    $priceJobsToDispatch[] = $store['price_job'];
                }
            }
        }

        // Single DB transaction for all writes
        if (! empty($imageRows) || ! empty($videoRows) || ! empty($sourceRows)) {
            DB::transaction(function () use ($imageRows, $videoRows, $sourceRows) {
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

                // Bulk upsert discovered sources
                foreach ($sourceRows as $source) {
                    VideoGameTitleSource::firstOrCreate(
                        [
                            'video_game_title_id' => $source['video_game_title_id'],
                            'provider' => $source['provider'],
                        ],
                        $source['data']
                    );
                }
            });
        }

        // Dispatch price jobs outside transaction
        foreach ($priceJobsToDispatch as $job) {
            match ($job['type']) {
                'steam' => ConcurrentFetchSteamDataJob::dispatch($job['video_game_id'], (int) $job['app_id']),
                default => null,
            };
        }

        return [count($games), count($priceJobsToDispatch)];
    }

    /**
     * Build IGDB API query for batch fetching.
     */
    private function buildBatchQuery(array $igdbIds): string
    {
        $ids = implode(',', $igdbIds);

        $fields = implode(',', [
            'id',
            'name',
            'cover.image_id',
            'cover.width',
            'cover.height',
            'screenshots.image_id',
            'screenshots.width',
            'screenshots.height',
            'artworks.image_id',
            'artworks.width',
            'artworks.height',
            'videos.video_id',
            'videos.name',
            'websites.category',
            'websites.url',
        ]);

        return "fields {$fields}; where id = ({$ids}); limit 500;";
    }

    /**
     * Extract media from IGDB response.
     *
     * @return array{image: array|null, video: array|null}
     */
    private function extractMedia(array $gameData, int $igdbId, int $videoGameId): array
    {
        $cover = $gameData['cover'] ?? null;
        $screenshots = $gameData['screenshots'] ?? [];
        $artworks = $gameData['artworks'] ?? [];
        $videos = $gameData['videos'] ?? [];

        $imageData = null;
        $videoData = null;

        // Build comprehensive image data
        if ($cover || ! empty($screenshots) || ! empty($artworks)) {
            $urls = [];
            $details = [];
            $collections = [];

            // Cover
            if ($cover && isset($cover['image_id'])) {
                $coverUrl = $this->buildIgdbImageUrl($cover['image_id'], 'cover_big');
                $collections[] = 'cover_images';
                $urls[] = $coverUrl;
                $details[] = [
                    'collection' => 'cover_images',
                    'image_id' => $cover['image_id'],
                    'url' => $coverUrl,
                    'size_variants' => $this->buildIgdbSizeVariants($cover['image_id']),
                    'width' => $cover['width'] ?? null,
                    'height' => $cover['height'] ?? null,
                ];
            }

            // Screenshots
            foreach ($screenshots as $screenshot) {
                if (! isset($screenshot['image_id'])) {
                    continue;
                }
                $url = $this->buildIgdbImageUrl($screenshot['image_id'], 'screenshot_huge');
                $collections[] = 'screenshots';
                $urls[] = $url;
                $details[] = [
                    'collection' => 'screenshots',
                    'image_id' => $screenshot['image_id'],
                    'url' => $url,
                    'size_variants' => $this->buildIgdbSizeVariants($screenshot['image_id']),
                    'width' => $screenshot['width'] ?? null,
                    'height' => $screenshot['height'] ?? null,
                ];
            }

            // Artworks
            foreach ($artworks as $artwork) {
                if (! isset($artwork['image_id'])) {
                    continue;
                }
                $url = $this->buildIgdbImageUrl($artwork['image_id'], '1080p');
                $collections[] = 'artworks';
                $urls[] = $url;
                $details[] = [
                    'collection' => 'artworks',
                    'image_id' => $artwork['image_id'],
                    'url' => $url,
                    'size_variants' => $this->buildIgdbSizeVariants($artwork['image_id']),
                    'width' => $artwork['width'] ?? null,
                    'height' => $artwork['height'] ?? null,
                ];
            }

            $collections = array_values(array_unique($collections));

            $imageData = [
                'video_game_id' => $videoGameId,
                'data' => [
                    'provider' => 'igdb',
                    'primary_collection' => in_array('cover_images', $collections, true) ? 'cover_images' : ($collections[0] ?? 'misc'),
                    'collection_names' => $collections,
                    'url' => $urls[0] ?? null,
                    'urls' => $urls,
                    'metadata' => [
                        'source' => 'igdb_batch_enrichment',
                        'igdb_id' => $igdbId,
                        'details' => $details,
                    ],
                ],
            ];
        }

        // Build video data (YouTube links)
        if (! empty($videos)) {
            $videoUrls = [];
            $videoDetails = [];
            $collections = [];

            foreach ($videos as $video) {
                if (! isset($video['video_id'])) {
                    continue;
                }

                $name = $video['name'] ?? '';
                $videoType = $this->categorizeVideoType($name);
                $collections[] = $videoType;

                $url = 'https://www.youtube.com/watch?v='.$video['video_id'];
                $thumbnail = 'https://img.youtube.com/vi/'.$video['video_id'].'/maxresdefault.jpg';

                $videoUrls[] = $url;
                $videoDetails[] = [
                    'video_id' => $video['video_id'],
                    'name' => $name,
                    'type' => $videoType,
                    'url' => $url,
                    'thumbnail_url' => $thumbnail,
                    'provider' => 'youtube',
                ];
            }

            if (! empty($videoUrls)) {
                $collections = array_values(array_unique($collections));

                $videoData = [
                    'video_game_id' => $videoGameId,
                    'data' => [
                        'provider' => 'igdb',
                        'primary_collection' => $collections[0] ?? 'trailers',
                        'collection_names' => $collections,
                        'url' => $videoUrls[0] ?? null,
                        'urls' => $videoUrls,
                        'video_id' => $videos[0]['video_id'] ?? null,
                        'thumbnail_url' => $videoDetails[0]['thumbnail_url'] ?? null,
                        'title' => $videos[0]['name'] ?? null,
                        'metadata' => [
                            'source' => 'igdb_batch_enrichment',
                            'igdb_id' => $igdbId,
                            'total_videos' => count($videos),
                            'videos' => $videoDetails,
                        ],
                    ],
                ];
            }
        }

        return ['image' => $imageData, 'video' => $videoData];
    }

    /**
     * Extract store URLs from IGDB websites for price discovery.
     *
     * @return array<int, array{source: array, price_job: array|null}>
     */
    private function extractStores(array $gameData, int $videoGameId): array
    {
        $websites = $gameData['websites'] ?? [];
        $stores = [];

        // We need the title_id - fetch it
        $game = VideoGame::with('title')->find($videoGameId);

        if (! $game || ! $game->title) {
            return [];
        }

        foreach ($websites as $website) {
            $category = $website['category'] ?? 0;
            $url = $website['url'] ?? null;

            if (! $url || ! $this->isStoreCategory($category)) {
                continue;
            }

            $storeName = $this->getStoreFromCategory($category);
            $appId = $this->extractStoreAppId($url, $storeName);
            $provider = $this->normalizeStoreProvider($storeName);

            if (! $provider) {
                continue;
            }

            $store = [
                'source' => [
                    'video_game_title_id' => $game->title->id,
                    'provider' => $provider,
                    'data' => [
                        'external_id' => $appId,
                        'provider_item_id' => $appId,
                        'provider_url' => $url,
                        'raw_payload' => [
                            'discovered_via' => 'igdb_batch',
                            'igdb_id' => $gameData['id'] ?? null,
                            'original_url' => $url,
                        ],
                    ],
                ],
                'price_job' => null,
            ];

            // Queue price job for Steam
            if ($storeName === 'steam' && $appId) {
                $store['price_job'] = [
                    'type' => 'steam',
                    'video_game_id' => $videoGameId,
                    'app_id' => $appId,
                ];
            }

            $stores[] = $store;
        }

        return $stores;
    }

    /**
     * Get or refresh OAuth access token.
     */
    private function getAccessToken(): ?string
    {
        $cached = Cache::get(self::TOKEN_CACHE_KEY);

        if ($cached) {
            return $cached;
        }

        $clientId = config('services.igdb.client_id');
        $clientSecret = config('services.igdb.client_secret');

        if (! $clientId || ! $clientSecret) {
            Log::error('BatchFetchIgdbDataJob: Missing IGDB credentials');

            return null;
        }

        $response = Http::asForm()->post('https://id.twitch.tv/oauth2/token', [
            'client_id' => $clientId,
            'client_secret' => $clientSecret,
            'grant_type' => 'client_credentials',
        ]);

        if (! $response->successful()) {
            Log::error('BatchFetchIgdbDataJob: OAuth request failed', [
                'status' => $response->status(),
            ]);

            return null;
        }

        $token = $response->json('access_token');

        if ($token) {
            $expiresIn = $response->json('expires_in', 3600) - 300;
            Cache::put(self::TOKEN_CACHE_KEY, $token, max(60, $expiresIn));
        }

        return $token;
    }

    /**
     * Static helper to dispatch batch jobs from a collection of games.
     *
     * @param  Collection  $games  Collection with 'id' and igdb mapping
     */
    public static function dispatchForGames(Collection $games): int
    {
        $mappings = $games->map(fn ($game) => [
            'video_game_id' => $game->id,
            'igdb_id' => $game->igdb_id ?? $game->external_id,
        ])->filter(fn ($m) => $m['igdb_id'] !== null)->values()->all();

        if (empty($mappings)) {
            return 0;
        }

        self::dispatch($mappings);

        return 1;
    }
}
