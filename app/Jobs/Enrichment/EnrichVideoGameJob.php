<?php

declare(strict_types=1);

namespace App\Jobs\Enrichment;

use App\Models\VideoGame;
use App\Services\Provider\ProviderDiscoveryService;
use Illuminate\Bus\Queueable;
use Illuminate\Contracts\Queue\ShouldBeUnique;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Bus\Dispatchable;
use Illuminate\Queue\InteractsWithQueue;
use Illuminate\Queue\SerializesModels;
use Illuminate\Support\Facades\Cache;
use Illuminate\Support\Facades\Log;

/**
 * UNIFIED Enrichment Dispatcher.
 *
 * This job is the single entry point for all game enrichment.
 * It orchestrates a consolidated pipeline where:
 *
 * 1. STORE PROVIDERS (Steam, PSN, Xbox) - fetch prices + media in single API calls
 * 2. MEDIA PROVIDERS (IGDB, TGDB, RAWG) - ALWAYS fetch for comprehensive coverage
 *
 * The key insight: store APIs return both prices AND media, so we dispatch
 * unified jobs per store rather than separating concerns.
 *
 * IGDB is special: it also discovers store URLs (Steam, GOG, Epic) for games
 * that don't have provider mappings yet, enabling cascading price discovery.
 *
 * Performance Characteristics:
 * - ShouldBeUnique prevents duplicate processing
 * - Atomic locks prevent race conditions
 * - All sub-jobs run in parallel on dedicated queues
 * - Rate-limited per provider (steam: 200/min, igdb: 4/sec, psstore: 2/sec)
 */
class EnrichVideoGameJob implements ShouldBeUnique, ShouldQueue
{
    use Dispatchable, InteractsWithQueue, Queueable, SerializesModels;

    public int $tries = 3;

    /** @var array<int, int> */
    public array $backoff = [30, 60, 120];

    public function __construct(
        public int $videoGameId,
        public bool $forceRefresh = false
    ) {}

    /**
     * The unique ID of the job.
     */
    public function uniqueId(): string
    {
        return (string) $this->videoGameId;
    }

    public function handle(ProviderDiscoveryService $discovery): void
    {
        // Atomic Lock to prevent race conditions
        $lockKey = "enrich_game:{$this->videoGameId}";
        $lock = Cache::lock($lockKey, 120); // 2 minute lock for comprehensive enrichment

        if (! $lock->get()) {
            Log::debug('EnrichVideoGameJob: Skipped - locked', ['game_id' => $this->videoGameId]);

            return;
        }

        try {
            $game = VideoGame::with('title.sources')->find($this->videoGameId);

            if (! $game) {
                Log::warning('EnrichVideoGameJob: Game not found', ['game_id' => $this->videoGameId]);

                return;
            }

            $dispatched = [
                'stores' => [],
                'media' => [],
            ];

            // ═══════════════════════════════════════════════════════════════════
            // PHASE 1: STORE PROVIDERS - Fetch prices + media together
            // Each store job handles BOTH prices AND media in a single API call
            // ═══════════════════════════════════════════════════════════════════

            // Steam: Combined prices (multi-region) + media
            $steamId = $discovery->getExternalId($game, 'steam')
                ?? $discovery->getExternalId($game, 'steam_store');

            if ($steamId) {
                ConcurrentFetchSteamDataJob::dispatch($game->id, (int) $steamId)
                    ->onQueue('prices-steam');
                $dispatched['stores'][] = 'steam';
            }

            // PlayStation Store: Combined prices + media
            $psnId = $discovery->getExternalId($game, 'playstation_store');

            if ($psnId) {
                // FetchPsnDataJob::dispatch($game->id, $psnId)->onQueue('prices-psn');
                $dispatched['stores'][] = 'playstation';
            }

            // Xbox: Combined prices + media
            $xboxId = $discovery->getExternalId($game, 'xbox');

            if ($xboxId) {
                // FetchXboxDataJob::dispatch($game->id, $xboxId)->onQueue('prices-xbox');
                $dispatched['stores'][] = 'xbox';
            }

            // ═══════════════════════════════════════════════════════════════════
            // PHASE 2: MEDIA PROVIDERS - ALWAYS fetch for comprehensive coverage
            // These run regardless of store media to ensure maximum media quality
            // ═══════════════════════════════════════════════════════════════════

            // IGDB: Primary media source + store URL discovery
            // IGDB websites endpoint reveals Steam/GOG/Epic URLs we might not have
            $igdbId = $discovery->getExternalId($game, 'igdb');

            if ($igdbId) {
                FetchIgdbDataJob::dispatch($game->id, (int) $igdbId)
                    ->onQueue('media-igdb');
                $dispatched['media'][] = 'igdb';
            } elseif (! $this->hasAnyMediaSource($game)) {
                // Search IGDB by name if we don't have an ID and need media
                $this->queueIgdbSearch($game);
                $dispatched['media'][] = 'igdb_search';
            }

            // TGDB: Secondary media source - always fetch if we have ID
            $tgdbId = $discovery->getExternalId($game, 'tgdb');

            if ($tgdbId) {
                FetchTgdbDataJob::dispatch($game->id, (int) $tgdbId)
                    ->onQueue('media-tgdb');
                $dispatched['media'][] = 'tgdb';
            }

            // RAWG: Tertiary media source - always fetch if we have ID
            $rawgId = $discovery->getExternalId($game, 'rawg');

            if ($rawgId) {
                FetchRawgDataJob::dispatch($game->id, (int) $rawgId)
                    ->onQueue('media-rawg');
                $dispatched['media'][] = 'rawg';
            }

            // ═══════════════════════════════════════════════════════════════════
            // PHASE 3: SEARCH FALLBACKS
            // If no store mappings exist, try to find the game via search
            // ═══════════════════════════════════════════════════════════════════

            if (empty($dispatched['stores']) && $game->name) {
                $this->queueSearchFallbacks($game);
                $dispatched['stores'][] = 'search_fallback';
            }

            Log::info('EnrichVideoGameJob: Dispatched', [
                'game_id' => $game->id,
                'game_name' => $game->name,
                'stores' => $dispatched['stores'],
                'media' => $dispatched['media'],
            ]);

        } finally {
            $lock->release();
        }
    }

    /**
     * Check if game has any media source already.
     */
    private function hasAnyMediaSource(VideoGame $game): bool
    {
        return $game->images()->exists() || $game->videos()->exists();
    }

    /**
     * Queue IGDB search for games without IGDB ID.
     */
    private function queueIgdbSearch(VideoGame $game): void
    {
        if (! $game->name) {
            return;
        }

        // The IGDB search job will:
        // 1. Search IGDB by name
        // 2. If found, create VideoGameTitleSource mapping
        // 3. Dispatch FetchIgdbDataJob with the discovered ID
        SearchIgdbAndEnrichJob::dispatch($game->id, $game->name)
            ->onQueue('media-igdb');
    }

    /**
     * Queue search fallbacks for games without store mappings.
     */
    private function queueSearchFallbacks(VideoGame $game): void
    {
        // Search Steam by name
        SearchSteamAndEnrichJob::dispatch($game->id, $game->name)
            ->onQueue('prices-steam');

        // Future: Search PSN, Xbox by name
    }

    /**
     * Dispatch enrichment for a batch of games (high-performance bulk mode).
     *
     * Uses batch jobs where available for maximum throughput.
     *
     * @param  array<int>  $videoGameIds
     */
    public static function dispatchBatch(array $videoGameIds): int
    {
        if (empty($videoGameIds)) {
            return 0;
        }

        // For bulk operations, group by provider for efficient batch API calls
        $games = VideoGame::with('title.sources')
            ->whereIn('id', $videoGameIds)
            ->get();

        // Group IGDB games for batch processing
        $igdbMappings = [];
        $steamMappings = [];
        $individualGames = [];

        foreach ($games as $game) {
            $igdbId = $game->title?->sources
                ->firstWhere('provider', 'igdb')
                ?->external_id;

            $steamId = $game->title?->sources
                ->filter(fn ($s) => in_array($s->provider, ['steam', 'steam_store'], true))
                ->first()
                ?->external_id;

            if ($igdbId) {
                $igdbMappings[] = [
                    'video_game_id' => $game->id,
                    'igdb_id' => (int) $igdbId,
                ];
            }

            if ($steamId) {
                $steamMappings[] = [
                    'video_game_id' => $game->id,
                    'steam_app_id' => (int) $steamId,
                ];
            }

            // Games without batch-able providers get individual jobs
            if (! $igdbId && ! $steamId) {
                $individualGames[] = $game->id;
            }
        }

        $jobCount = 0;

        // Dispatch batch IGDB job (up to 500 per request)
        if (! empty($igdbMappings)) {
            foreach (array_chunk($igdbMappings, 500) as $chunk) {
                BatchFetchIgdbDataJob::dispatch($chunk)->onQueue('media-igdb');
                $jobCount++;
            }
        }

        // Dispatch batch Steam job (up to 50 per request)
        if (! empty($steamMappings)) {
            foreach (array_chunk($steamMappings, 50) as $chunk) {
                BatchFetchSteamDataJob::dispatch($chunk)->onQueue('prices-steam');
                $jobCount++;
            }
        }

        // Dispatch individual jobs for remaining games
        foreach ($individualGames as $gameId) {
            self::dispatch($gameId)->onQueue('enrichment');
            $jobCount++;
        }

        return $jobCount;
    }
}
