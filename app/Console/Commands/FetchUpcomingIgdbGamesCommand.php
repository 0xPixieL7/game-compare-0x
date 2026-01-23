<?php

declare(strict_types=1);

namespace App\Console\Commands;

use App\Jobs\Enrichment\Traits\CategorizesVideoTypes;
use App\Models\Image;
use App\Models\Product;
use App\Models\Video;
use App\Models\VideoGame;
use App\Models\VideoGameSource;
use App\Models\VideoGameTitle;
use App\Models\VideoGameTitleSource;
use App\Services\Normalization\IgdbRatingHelper;
use App\Services\Normalization\PlatformNormalizer;
use Illuminate\Console\Command;
use Illuminate\Http\Client\Response;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Http;
use Illuminate\Support\Facades\Log;
use Symfony\Component\Console\Helper\ProgressBar;

class FetchUpcomingIgdbGamesCommand extends Command
{
    use CategorizesVideoTypes;

    protected $signature = 'igdb:fetch-upcoming
                           {--from-date= : Start date (Y-m-d), defaults to today}
                           {--to-date= : End date (Y-m-d), defaults to 10 years from now}
                           {--limit=50000 : Maximum games to fetch}
                           {--min-rating=0 : Minimum IGDB rating (0-100)}
                           {--min-hypes=0 : Minimum hypes (pre-release follows)}
                           {--sort-by=first_release_date : Sort field (hypes, rating, first_release_date)}
                           {--batch-size=100 : Items per API request (IGDB max: 500)}
                           {--provider=igdb : Provider key for video_game_sources}
                           {--upsert-media-for-existing : If a game already exists, still upsert nested media instead of skipping}';

    protected $description = 'Fetch upcoming/unreleased IGDB games via live API (future release dates only)';

    private string $baseUrl = 'https://api.igdb.com/v4';

    private ?string $accessToken = null;

    private int $requestCount = 0;

    private int $maxRequestsPerSecond = 4;

    private ?PlatformNormalizer $platformNormalizer = null;

    private ?IgdbRatingHelper $igdbRatingHelper = null;

    private string $providerKey = 'igdb';

    // In-memory caches
    private array $productCache = [];

    private array $sourceCache = [];

    private array $titleCache = [];

    // Batch queues
    private array $videoGameBatch = [];

    private array $videoGameTitleSourceBatch = [];

    private array $imageBatch = [];

    private array $videoBatch = [];

    private int $imageUrlCount = 0;

    private int $videoUrlCount = 0;

    private ?\Carbon\Carbon $batchTimestamp = null;

    private ?ProgressBar $uploadProgressBar = null;

    private const BATCH_SIZE = 200;

    private const MEDIA_BATCH_SIZE = 500;

    public function handle(): int
    {
        if (! $this->obtainAccessToken()) {
            return self::FAILURE;
        }

        $this->resetBatchState();
        $this->batchTimestamp = now();

        // Default: today to 10 years from now
        $fromDate = $this->option('from-date')
            ? strtotime($this->option('from-date'))
            : time();

        $toDate = $this->option('to-date')
            ? strtotime($this->option('to-date'))
            : strtotime('+10 years');

        $limit = (int) $this->option('limit');
        $minRating = (int) $this->option('min-rating');
        $minHypes = (int) $this->option('min-hypes');
        $sortBy = $this->option('sort-by');
        $batchSize = min((int) $this->option('batch-size'), 500);
        $this->providerKey = $this->option('provider');
        $upsertMediaForExisting = (bool) $this->option('upsert-media-for-existing');

        if (! $fromDate || ! $toDate) {
            $this->error('Invalid date format. Use Y-m-d (e.g., 2026-01-19)');

            return self::FAILURE;
        }

        if ($fromDate < time()) {
            $this->warn('⚠️  From date is in the past. Setting to today to fetch only upcoming games.');
            $fromDate = time();
        }

        $this->platformNormalizer = new PlatformNormalizer;
        $this->igdbRatingHelper = new IgdbRatingHelper;

        $this->info('🎮 Fetching UPCOMING IGDB games via live API → database');
        $this->info('   Date range: '.date('Y-m-d', $fromDate).' to '.date('Y-m-d', $toDate));
        if ($minRating > 0) {
            $this->info("   Min rating: {$minRating}");
        }
        if ($minHypes > 0) {
            $this->info("   Min hypes: {$minHypes}");
        }
        $this->info("   Sort: {$sortBy} asc (earliest first)");
        $this->info("   Batch size: {$batchSize} games/request");
        if ($upsertMediaForExisting) {
            $this->info('   Existing games: upsert media (no skipping)');
        }
        $this->newLine();

        // Initialize provider source
        $providerSource = $this->getOrCreateProviderSource($this->providerKey);

        $offset = 0;
        $totalProcessed = 0;
        $totalSkipped = 0;

        $bar = $this->output->createProgressBar($limit);
        $bar->setFormat(' %current%/%max% [%bar%] %percent:3s%% | ⏱ %elapsed:6s% | 📊 Processed: %message%');
        $bar->setMessage('0');

        // Progress bar for batch uploads (DB upserts)
        $maxUploadBatches = (int) ceil($limit / self::BATCH_SIZE);
        $this->uploadProgressBar = $this->output->createProgressBar($maxUploadBatches);
        $this->uploadProgressBar->setFormat(' Batches: %current%/%max% [%bar%] %percent:3s%%');
        $this->uploadProgressBar->start();
        $this->newLine();

        try {
            while ($offset < $limit) {
                $this->rateLimit();

                $games = $this->fetchGamesBatch($fromDate, $toDate, $batchSize, $minRating, $minHypes, $sortBy, $offset);

                if ($games->isEmpty()) {
                    break;
                }

                // Process batch directly to database
                $batchStats = $this->processBatch($games, $this->providerKey, $providerSource, $upsertMediaForExisting);
                $totalProcessed += $batchStats['processed'];
                $totalSkipped += $batchStats['skipped'];

                $bar->advance($games->count());
                $bar->setMessage((string) $totalProcessed);

                $offset += $batchSize;

                if ($games->count() < $batchSize) {
                    break; // Last page
                }
            }

            // Flush any remaining batches
            $this->flushAllBatches();

            $bar->finish();
            $this->newLine(2);

            $this->info('✅ Upcoming games fetch complete!');
            $this->table(
                ['Metric', 'Count'],
                [
                    ['Total fetched', $offset],
                    ['Games processed', $totalProcessed],
                    ['Skipped', $totalSkipped],
                    ['API requests', $this->requestCount],
                ]
            );

            return self::SUCCESS;
        } catch (\Exception $e) {
            $bar->finish();
            $this->newLine();
            $this->error('Error: '.$e->getMessage());
            Log::error('IGDB upcoming games fetch failed', [
                'error' => $e->getMessage(),
                'trace' => $e->getTraceAsString(),
            ]);

            return self::FAILURE;
        }
    }

    private function resetBatchState(): void
    {
        $this->videoGameBatch = [];
        $this->videoGameTitleSourceBatch = [];
        $this->imageBatch = [];
        $this->videoBatch = [];
        $this->imageUrlCount = 0;
        $this->videoUrlCount = 0;
        $this->uploadProgressBar = null;
        $this->requestCount = 0;
    }

    private function obtainAccessToken(): bool
    {
        $clientId = config('services.igdb.client_id');
        $clientSecret = config('services.igdb.client_secret');

        if (! $clientId || ! $clientSecret) {
            $this->error('IGDB_CLIENT_ID and IGDB_CLIENT_SECRET must be set in .env');

            return false;
        }

        $this->info('🔑 Obtaining OAuth token from Twitch...');

        /** @var Response $response */
        $response = Http::asForm()->post('https://id.twitch.tv/oauth2/token', [
            'client_id' => $clientId,
            'client_secret' => $clientSecret,
            'grant_type' => 'client_credentials',
        ]);

        if (! $response->successful()) {
            $this->error('Failed to obtain OAuth token: '.$response->body());

            return false;
        }

        $this->accessToken = $response->json('access_token');
        $this->info('✓ OAuth token obtained');
        $this->newLine();

        return true;
    }

    private function fetchGamesBatch(
        int $fromDate,
        int $toDate,
        int $batchSize,
        int $minRating,
        int $minHypes,
        string $sortBy,
        int $offset
    ): \Illuminate\Support\Collection {
        $fields = implode(',', [
            'id', 'name', 'slug', 'summary', 'storyline',
            'first_release_date', 'rating', 'rating_count',
            'aggregated_rating', 'aggregated_rating_count',
            'hypes', 'follows', 'total_rating', 'total_rating_count',
            'platforms', 'genres', 'themes', 'keywords',
            'category', 'status', 'url', 'checksum',
            // Nested media (ALL in one request!)
            'cover.image_id',
            'screenshots.image_id',
            'artworks.image_id',
            'videos.*',
            'artworks.*',
            'websites.*',
        ]);

        // Build WHERE clause - ONLY future games
        $whereConditions = [
            "first_release_date >= {$fromDate}",
            "first_release_date <= {$toDate}",
        ];

        if ($minRating > 0) {
            $whereConditions[] = "rating >= {$minRating}";
        }

        if ($minHypes > 0) {
            $whereConditions[] = "hypes >= {$minHypes}";
        }

        $whereClause = implode(' & ', $whereConditions);

        // Sort by earliest first for upcoming games
        $sortDirection = $sortBy === 'first_release_date' ? 'asc' : 'desc';

        $query = "fields {$fields}; ".
                 "where {$whereClause}; ".
                 "sort {$sortBy} {$sortDirection}; ".
                 "limit {$batchSize}; ".
                 "offset {$offset};";

        /** @var Response $response */
        $response = Http::withHeaders([
            'Client-ID' => config('services.igdb.client_id'),
            'Authorization' => 'Bearer '.$this->accessToken,
        ])->withBody($query, 'text/plain')->post("{$this->baseUrl}/games");

        if (! $response->successful()) {
            $this->warn("API request failed at offset {$offset}: ".$response->body());

            return collect();
        }

        return collect($response->json());
    }

    private function processBatch(\Illuminate\Support\Collection $games, string $provider, VideoGameSource $providerSource, bool $upsertMediaForExisting): array
    {
        $processed = 0;
        $skipped = 0;

        // Extract all external IDs from this batch
        $externalIds = $games->pluck('id')->filter()->map(fn ($id) => (string) $id)->values()->all();

        // Check which games already exist in the database via video_game_title_sources
        $existingTitleIdsByExternalId = VideoGameTitleSource::query()
            ->where('video_game_source_id', $providerSource->id)
            ->whereIn('provider_item_id', $externalIds)
            ->get(['provider_item_id', 'video_game_title_id'])
            ->mapWithKeys(fn (VideoGameTitleSource $row) => [(string) $row->provider_item_id => (int) $row->video_game_title_id])
            ->all();

        foreach ($games as $gameData) {
            try {
                $externalId = (string) ($gameData['id'] ?? '');
                $exists = array_key_exists($externalId, $existingTitleIdsByExternalId);
                if ($exists && ! $upsertMediaForExisting) {
                    $skipped++;

                    continue;
                }

                $title = null;
                if ($exists) {
                    $titleId = $existingTitleIdsByExternalId[$externalId];
                    $title = $this->getTitleById($titleId);
                } else {
                    // Create/get product + title
                    $product = $this->getOrCreateProduct($gameData);
                    if (! $product) {
                        $skipped++;

                        continue;
                    }

                    $title = $this->getOrCreateTitle($product, $gameData);
                }

                if (! $title) {
                    $skipped++;

                    continue;
                }

                // Queue video game for batch insert
                $this->queueVideoGame($gameData, $title, $provider);

                // Queue title source for batch insert
                $this->queueTitleSource($gameData, $title, $providerSource);

                // Extract and queue nested media
                $this->queueNestedMedia($gameData);

                $processed++;

                // Flush batches periodically
                if (count($this->videoGameBatch) >= self::BATCH_SIZE) {
                    $this->flushVideoGameBatch();
                }

                if ($this->imageUrlCount >= self::MEDIA_BATCH_SIZE) {
                    $this->flushImageBatch();
                }

                if ($this->videoUrlCount >= self::MEDIA_BATCH_SIZE) {
                    $this->flushVideoBatch();
                }
            } catch (\Exception $e) {
                $this->warn("Error processing game {$gameData['id']}: ".$e->getMessage());
                Log::error('Game processing error', [
                    'game_id' => $gameData['id'],
                    'error' => $e->getMessage(),
                ]);
                $skipped++;
            }
        }

        return ['processed' => $processed, 'skipped' => $skipped];
    }

    private function getOrCreateProduct(array $gameData): ?Product
    {
        $name = $gameData['name'] ?? null;
        if (! $name) {
            return null;
        }

        // Check cache first
        $cacheKey = $name;
        if (isset($this->productCache[$cacheKey])) {
            return $this->productCache[$cacheKey];
        }

        // Products are unique by name
        $product = Product::query()->where('name', $name)->first();

        if (! $product) {
            try {
                $slug = \Illuminate\Support\Str::slug($name);
                $product = Product::create([
                    'name' => $name,
                    'slug' => $slug,
                ]);
            } catch (\Illuminate\Database\QueryException $e) {
                // Handle race condition
                if ($e->getCode() === '23505') {
                    $product = Product::where('name', $name)->first();
                    if (! $product) {
                        throw $e;
                    }
                } else {
                    throw $e;
                }
            }
        }
        $this->productCache[$cacheKey] = $product;

        return $product;
    }

    private function getOrCreateTitle(Product $product, array $gameData): ?VideoGameTitle
    {
        $name = $gameData['name'] ?? null;
        if (! $name) {
            return null;
        }

        $slug = \Illuminate\Support\Str::slug($name);
        $cacheKey = "{$product->id}:{$slug}";

        if (isset($this->titleCache[$cacheKey])) {
            return $this->titleCache[$cacheKey];
        }

        $title = VideoGameTitle::query()->firstOrCreate(
            ['product_id' => $product->id, 'slug' => $slug],
            ['name' => $name]
        );

        $this->titleCache[$cacheKey] = $title;

        return $title;
    }

    private function getTitleById(int $titleId): ?VideoGameTitle
    {
        $cacheKey = "id:{$titleId}";

        if (isset($this->titleCache[$cacheKey])) {
            return $this->titleCache[$cacheKey];
        }

        $title = VideoGameTitle::query()->find($titleId);
        if (! $title) {
            return null;
        }

        $this->titleCache[$cacheKey] = $title;

        return $title;
    }

    private function getOrCreateProviderSource(string $provider): VideoGameSource
    {
        if (isset($this->sourceCache[$provider])) {
            return $this->sourceCache[$provider];
        }

        $source = VideoGameSource::firstOrCreate(
            ['provider' => $provider],
            ['name' => ucfirst($provider), 'base_url' => 'https://www.igdb.com']
        );

        $this->sourceCache[$provider] = $source;

        return $source;
    }

    private function queueVideoGame(array $gameData, VideoGameTitle $title, string $provider): void
    {
        $externalId = (string) $gameData['id'];
        $rating = $this->igdbRatingHelper?->extractPercentage($gameData);
        $ratingCount = $this->igdbRatingHelper?->extractRatingCount($gameData);

        $this->videoGameBatch[] = [
            'video_game_title_id' => $title->id,
            'provider' => $provider,
            'external_id' => $externalId,
            'name' => $gameData['name'] ?? '',
            'slug' => $gameData['slug'] ?? \Illuminate\Support\Str::slug($gameData['name'] ?? ''),
            'summary' => $gameData['summary'] ?? null,
            'storyline' => $gameData['storyline'] ?? null,
            'rating' => $rating,
            'rating_count' => $ratingCount,
            'url' => $gameData['url'] ?? null,
            'release_date' => isset($gameData['first_release_date'])
                ? date('Y-m-d H:i:s', $gameData['first_release_date'])
                : null,
            'created_at' => $this->batchTimestamp,
            'updated_at' => $this->batchTimestamp,
        ];
    }

    private function queueTitleSource(array $gameData, VideoGameTitle $title, VideoGameSource $providerSource): void
    {
        $this->videoGameTitleSourceBatch[] = [
            'video_game_title_id' => $title->id,
            'video_game_source_id' => $providerSource->id,
            'provider' => $providerSource->provider,
            'external_id' => (int) $gameData['id'],
            'provider_item_id' => (string) $gameData['id'],
            'raw_payload' => json_encode($gameData),
            'created_at' => $this->batchTimestamp,
            'updated_at' => $this->batchTimestamp,
        ];
    }

    private function queueNestedMedia(array $gameData): void
    {
        $externalId = (string) $gameData['id'];

        if ($externalId === '') {
            return;
        }

        // Queue cover image
        if (isset($gameData['cover']['image_id'])) {
            $imageId = $gameData['cover']['image_id'];
            $url = "https://images.igdb.com/igdb/image/upload/t_cover_big/{$imageId}.jpg";

            $this->imageBatch[$externalId][] = [
                'url' => $url,
                'collection' => 'cover',
                'image_id' => $imageId,
            ];
            $this->imageUrlCount++;
        }

        // Queue screenshots
        if (isset($gameData['screenshots']) && is_array($gameData['screenshots'])) {
            foreach ($gameData['screenshots'] as $screenshot) {
                if (isset($screenshot['image_id'])) {
                    $imageId = $screenshot['image_id'];
                    $url = "https://images.igdb.com/igdb/image/upload/t_screenshot_huge/{$imageId}.jpg";

                    $this->imageBatch[$externalId][] = [
                        'url' => $url,
                        'collection' => 'screenshot',
                        'image_id' => $imageId,
                    ];
                    $this->imageUrlCount++;
                }
            }
        }
        // Queue artworks
        if (isset($gameData['artworks']) && is_array($gameData['artworks'])) {
            foreach ($gameData['artworks'] as $artwork) {
                if (isset($artwork['image_id'])) {
                    $imageId = $artwork['image_id'];
                    $url = "https://images.igdb.com/igdb/image/upload/t_720p/{$imageId}.jpg";

                    $this->imageBatch[$externalId][] = [
                        'url' => $url,
                        'collection' => 'artwork',
                        'image_id' => $imageId,
                    ];
                    $this->imageUrlCount++;
                }
            }
        }

        // Queue videos
        if (isset($gameData['videos']) && is_array($gameData['videos'])) {
            foreach ($gameData['videos'] as $video) {
                if (isset($video['video_id'])) {
                    $videoId = $video['video_id'];
                    $url = "https://www.youtube.com/watch?v={$videoId}";

                    $this->videoBatch[$externalId][] = [
                        'url' => $url,
                        'video_id' => $videoId,
                        'name' => $video['name'] ?? null,
                    ];
                    $this->videoUrlCount++;
                }
            }
        }
    }

    private function flushVideoGameBatch(): void
    {
        if (empty($this->videoGameBatch)) {
            return;
        }

        $externalIds = array_values(array_unique(array_map(
            fn (array $row) => (string) ($row['external_id'] ?? ''),
            $this->videoGameBatch
        )));

        DB::transaction(function () {
            VideoGame::upsert(
                $this->videoGameBatch,
                ['provider', 'external_id'],
                ['name', 'slug', 'summary', 'storyline', 'rating', 'rating_count', 'url', 'release_date', 'updated_at']
            );

            // Also flush title sources
            if (! empty($this->videoGameTitleSourceBatch)) {
                VideoGameTitleSource::upsert(
                    $this->videoGameTitleSourceBatch,
                    ['video_game_title_id', 'video_game_source_id', 'provider_item_id'],
                    ['provider', 'raw_payload', 'updated_at']
                );
                $this->videoGameTitleSourceBatch = [];
            }
        });

        if ($this->uploadProgressBar) {
            $this->uploadProgressBar->advance();
        }

        $this->videoGameBatch = [];

        // Flush media for these games
        if (! empty($externalIds)) {
            $this->flushMediaForExternalIds($externalIds);
        }
    }

    private function flushMediaForExternalIds(array $externalIds): void
    {
        $externalIds = array_values(array_filter(array_unique($externalIds), fn (string $id) => $id !== ''));
        if (empty($externalIds)) {
            return;
        }

        $idMap = VideoGame::query()
            ->where('provider', $this->providerKey)
            ->whereIn('external_id', $externalIds)
            ->pluck('id', 'external_id')
            ->all();

        if (empty($idMap)) {
            return;
        }

        // Images
        $imageUpserts = [];
        foreach ($externalIds as $externalId) {
            $videoGameId = $idMap[$externalId] ?? null;
            if (! $videoGameId) {
                continue;
            }

            foreach (($this->imageBatch[$externalId] ?? []) as $imageData) {
                $url = is_array($imageData) ? $imageData['url'] : $imageData;
                $collection = is_array($imageData) ? ($imageData['collection'] ?? 'cover') : 'cover';
                $imageId = is_array($imageData) ? ($imageData['image_id'] ?? null) : null;

                $imageUpserts[] = [
                    'imageable_type' => VideoGame::class,
                    'imageable_id' => $videoGameId,
                    'video_game_id' => $videoGameId,
                    'uuid' => (string) \Illuminate\Support\Str::uuid(),
                    'url' => $url,
                    'primary_collection' => $collection,
                    'collection_names' => json_encode([$collection]),
                    'external_id' => $imageId,
                    'provider' => 'igdb',
                    'created_at' => $this->batchTimestamp,
                    'updated_at' => $this->batchTimestamp,
                ];
            }

            unset($this->imageBatch[$externalId]);
        }

        // Videos
        $videoUpserts = [];
        foreach ($externalIds as $externalId) {
            $videoGameId = $idMap[$externalId] ?? null;
            if (! $videoGameId) {
                continue;
            }

            foreach (($this->videoBatch[$externalId] ?? []) as $videoData) {
                if (is_string($videoData)) {
                    $url = $videoData;
                    $name = null;
                    $videoId = null;
                } else {
                    $url = $videoData['url'];
                    $name = $videoData['name'] ?? null;
                    $videoId = $videoData['video_id'] ?? null;
                }

                $type = $this->categorizeVideoType((string) $name);

                $videoUpserts[] = [
                    'videoable_type' => VideoGame::class,
                    'videoable_id' => $videoGameId,
                    'video_game_id' => $videoGameId,
                    'uuid' => (string) \Illuminate\Support\Str::uuid(),
                    'primary_collection' => $type,
                    'collection_names' => json_encode([$type]),
                    'url' => $url,
                    'video_id' => $videoId,
                    'title' => $name,
                    'provider' => 'youtube',
                    'created_at' => $this->batchTimestamp,
                    'updated_at' => $this->batchTimestamp,
                ];
            }

            unset($this->videoBatch[$externalId]);
        }

        // Perform both media upserts in a single transaction
        DB::transaction(function () use ($imageUpserts, $videoUpserts) {
            if (! empty($imageUpserts)) {
                Image::upsert(
                    $imageUpserts,
                    ['imageable_type', 'imageable_id', 'url'],
                    ['primary_collection', 'collection_names', 'external_id', 'video_game_id', 'updated_at']
                );
            }

            if (! empty($videoUpserts)) {
                Video::upsert(
                    $videoUpserts,
                    ['videoable_type', 'videoable_id', 'url'],
                    ['primary_collection', 'collection_names', 'video_id', 'title', 'provider', 'updated_at']
                );
            }
        });
    }

    private function flushImageBatch(): void
    {
        if (empty($this->imageBatch)) {
            return;
        }

        $this->flushMediaForExternalIds(array_keys($this->imageBatch));
        $this->imageUrlCount = 0;
    }

    private function flushVideoBatch(): void
    {
        if (empty($this->videoBatch)) {
            return;
        }

        $this->flushMediaForExternalIds(array_keys($this->videoBatch));
        $this->videoUrlCount = 0;
    }

    private function flushAllBatches(): void
    {
        $this->flushVideoGameBatch();
        $this->flushImageBatch();
        $this->flushVideoBatch();

        if ($this->uploadProgressBar) {
            $this->uploadProgressBar->finish();
            $this->newLine();
        }
    }

    private function rateLimit(): void
    {
        $this->requestCount++;

        if ($this->requestCount % $this->maxRequestsPerSecond === 0) {
            usleep(250000); // 250ms delay every 4 requests = 4 req/sec
        }
    }
}
