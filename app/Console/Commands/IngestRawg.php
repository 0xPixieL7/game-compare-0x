<?php

declare(strict_types=1);

namespace App\Console\Commands;

use App\Jobs\Enrichment\ConcurrentFetchSteamDataJob;
use App\Jobs\Enrichment\EnrichGamePricesJob;
use App\Jobs\Enrichment\FetchPlayStationStorePricesJob;
use App\Jobs\Enrichment\FetchRawgDataJob;
use App\Jobs\Enrichment\FetchXboxStorePricesJob;
use App\Models\VideoGame;
use App\Models\VideoGameTitleSource;
use App\Services\Price\Xbox\XboxStoreService;
use App\Services\Providers\RawgProvider;
use Illuminate\Console\Command;
use Illuminate\Support\Facades\File;
use Illuminate\Support\Facades\Log;
use Symfony\Component\Process\Process;

class IngestRawg extends Command
{
    /** @var array<int, string> */
    private const COMMAND_FILES = [
        'app/Console/Commands/IngestRawg.php',
        'app/Services/Providers/RawgProvider.php',
        'app/Services/Providers/Rawg/RawgCommerceLinkResolver.php',
        'app/Jobs/Enrichment/Traits/ExtractsStoreUrls.php',
        'app/Jobs/Enrichment/FetchRawgDataJob.php',
        'app/Jobs/Enrichment/EnrichGamePricesJob.php',
        'app/Jobs/Enrichment/ConcurrentFetchSteamDataJob.php',
        'app/Jobs/Enrichment/FetchPlayStationStorePricesJob.php',
        'app/Services/Providers/PlayStationStoreProvider.php',
        'app/Jobs/Enrichment/FetchXboxStorePricesJob.php',
        'app/Services/Price/Xbox/XboxStoreService.php',
        'app/Services/Provider/ProviderDiscoveryService.php',
        'app/Services/Provider/ProviderRegistry.php',
        'config/services.php',
        'app/Providers/AppServiceProvider.php',
    ];

    protected $signature = 'ingest:rawg
        {--mode=setup : setup|discover|ingest|prices}
        {--genres=all : Discover: comma-separated RAWG genre slugs (or "all")}
        {--per-genre=50 : Discover: number of games per genre}
        {--ordering=-rating : Discover: RAWG ordering (e.g. -rating, -metacritic, -added)}
        {--max-genres=12 : Discover: safety cap when --genres=all}
        {--output-dir=rawg-discovery : Discover: storage/app/<dir>}
        {--file= : Path to JSON file containing RAWG IDs (required for ingest mode)}
        {--workers=1 : Number of parallel workers for ingestion}
        {--chunk= : Internal: process specific chunk (format: N/TOTAL)}
        {--limit=0 : Optional limit of IDs to ingest (0 = all)}
        {--dispatch-media=0 : Dispatch FetchRawgDataJob after ingest (1/0)}
        {--dispatch-prices=1 : Dispatch EnrichGamePricesJob after each ingest (1/0)}
        {--inline=0 : Run media+prices inline (no queue). Intended for IGDB-style worker runs (1/0)}
        {--resolve-stores=1 : Ingest: parse RAWG store links into provider mappings (1/0)}
        {--allow-network=0 : Allow network requests (1/0). Required for discover/ingest}
    ';

    protected $description = 'RAWG ingest pipeline (sync + parallel). Does not run network requests unless --allow-network=1.';

    public function handle(): int
    {
        $startedAt = microtime(true);

        $mode = (string) $this->option('mode');
        $workers = max(1, (int) $this->option('workers'));
        $chunk = (string) ($this->option('chunk') ?? '');
        $limit = max(0, (int) $this->option('limit'));
        $dispatchMedia = (int) $this->option('dispatch-media') === 1;
        $dispatchPrices = (int) $this->option('dispatch-prices') === 1;
        $inline = (int) $this->option('inline') === 1;
        $resolveStores = (int) $this->option('resolve-stores') === 1;
        $allowNetwork = (int) $this->option('allow-network') === 1;

        if ($chunk !== '') {
            Log::info('RAWG ingest: child worker start', [
                'mode' => $mode,
                'chunk' => $chunk,
                'dispatch_media' => $dispatchMedia,
                'dispatch_prices' => $dispatchPrices,
                'inline' => $inline,
                'resolve_stores' => $resolveStores,
                'limit' => $limit,
            ]);

            return $this->runAsChildWorker($chunk, $dispatchMedia, $dispatchPrices, $inline, $resolveStores, $allowNetwork, $limit);
        }

        if ($mode === 'setup') {
            $this->printFileStorageSection($mode);

            $provider = new RawgProvider;
            $source = $provider->ensureSourceExists();
            $this->info('RAWG provider source ensured.');
            $this->line('provider='.$source->provider);

            Log::info('RAWG ingest: setup complete', [
                'provider' => $source->provider,
                'elapsed_ms' => (int) ((microtime(true) - $startedAt) * 1000),
            ]);

            return self::SUCCESS;
        }

        if ($mode === 'prices') {
            // Backfill pricing for already-ingested RAWG games (NO network to RAWG).
            // Network is still required for the store providers (Steam/PS/Xbox) via the jobs.
            $this->printFileStorageSection($mode);

            $processed = $this->backfillPrices(
                file: (string) ($this->option('file') ?? ''),
                dispatchPrices: $dispatchPrices,
                inline: $inline,
                limit: $limit,
            );

            Log::info('RAWG ingest: prices backfill complete', [
                'processed' => $processed,
                'elapsed_ms' => (int) ((microtime(true) - $startedAt) * 1000),
            ]);

            return self::SUCCESS;
        }

        if (! $allowNetwork) {
            $this->error('Refusing to run RAWG network requests without --allow-network=1');

            return self::FAILURE;
        }

        if ($mode === 'discover') {
            $this->printFileStorageSection($mode);

            $result = $this->discoverTopByGenre();

            Log::info('RAWG ingest: discover complete', [
                'result' => $result,
                'elapsed_ms' => (int) ((microtime(true) - $startedAt) * 1000),
            ]);

            return $result;
        }

        if ($mode !== 'ingest') {
            $this->error("Unknown --mode={$mode}. Use setup|discover|ingest.");

            return self::FAILURE;
        }

        $file = (string) ($this->option('file') ?? '');
        if ($file === '' || ! File::exists($file)) {
            $this->error('Missing --file or file not found (JSON array of RAWG IDs).');

            return self::FAILURE;
        }

        $this->printFileStorageSection($mode, $file);

        if ($workers > 1) {
            Log::info('RAWG ingest: parent start (parallel)', [
                'file' => $file,
                'workers' => $workers,
                'dispatch_media' => $dispatchMedia,
                'dispatch_prices' => $dispatchPrices,
                'inline' => $inline,
                'resolve_stores' => $resolveStores,
                'limit' => $limit,
            ]);

            $result = $this->runParallelImport($file, $workers, $dispatchMedia, $dispatchPrices, $inline, $resolveStores, $allowNetwork, $limit);

            Log::info('RAWG ingest: parent complete (parallel)', [
                'file' => $file,
                'workers' => $workers,
                'result' => $result,
                'elapsed_ms' => (int) ((microtime(true) - $startedAt) * 1000),
            ]);

            return $result;
        }

        Log::info('RAWG ingest: start (single)', [
            'file' => $file,
            'dispatch_media' => $dispatchMedia,
            'dispatch_prices' => $dispatchPrices,
            'inline' => $inline,
            'resolve_stores' => $resolveStores,
            'limit' => $limit,
        ]);

        $result = $this->runSingleProcess($file, $dispatchMedia, $dispatchPrices, $inline, $resolveStores, $allowNetwork, $limit);

        Log::info('RAWG ingest: complete (single)', [
            'file' => $file,
            'result' => $result,
            'elapsed_ms' => (int) ((microtime(true) - $startedAt) * 1000),
        ]);

        return $result;
    }

    private function printFileStorageSection(string $mode, ?string $inputFile = null): void
    {
        $this->newLine();
        $this->components->twoColumnDetail('Files', '');

        if ($inputFile) {
            $this->components->twoColumnDetail('input', $inputFile);
        }

        if ($mode === 'discover') {
            $outputDir = (string) ($this->option('output-dir') ?? 'rawg-discovery');
            $this->components->twoColumnDetail('output_dir', storage_path('app/'.trim($outputDir, '/')));
            $this->components->twoColumnDetail('outputs', 'rawg_top_<key>_<N>.json + rawg_ids_<key>_<N>.json');
        }

        $this->components->twoColumnDetail('logs', storage_path('logs/laravel.log'));
        $this->components->twoColumnDetail('code_paths', implode(', ', self::COMMAND_FILES));
        $this->newLine();
    }

    private function discoverTopByGenre(): int
    {
        $provider = new RawgProvider;

        $genresOption = (string) ($this->option('genres') ?? 'all');
        $perGenre = max(1, min(50, (int) $this->option('per-genre')));
        $ordering = (string) ($this->option('ordering') ?? '-rating');
        $maxGenres = max(1, (int) $this->option('max-genres'));
        $outputDir = (string) ($this->option('output-dir') ?? 'rawg-discovery');

        $genres = [];
        if (strtolower(trim($genresOption)) === 'all') {
            $genres = array_slice($provider->discoverGenreSlugs(), 0, $maxGenres);
        } else {
            $genres = array_values(array_unique(array_filter(array_map('trim', explode(',', $genresOption)))));
        }

        if ($genres === []) {
            $this->error('No genres resolved. Check RAWG_API_KEY and --genres/--max-genres.');

            return self::FAILURE;
        }

        $basePath = storage_path('app/'.trim($outputDir, '/'));
        File::ensureDirectoryExists($basePath);

        $this->info('Downloading RAWG top games by genre...');
        $this->line('genres='.implode(',', $genres));
        $this->line('per_genre='.$perGenre.' ordering='.$ordering);

        foreach ($genres as $genreSlug) {
            $key = strtolower(trim($genreSlug));

            if ($key === 'trending') {
                $games = $provider->discoverTopGames($perGenre, '-added');
            } elseif ($key === 'upcoming') {
                $games = $provider->discoverUpcomingGames($perGenre);
            } else {
                $games = $provider->discoverTopGamesByGenre($genreSlug, $perGenre, $ordering);
            }

            $compact = array_map(static function ($g): array {
                if (! is_array($g)) {
                    return [];
                }

                return [
                    'id' => $g['id'] ?? null,
                    'slug' => $g['slug'] ?? null,
                    'name' => $g['name'] ?? null,
                    'released' => $g['released'] ?? null,
                    'rating' => $g['rating'] ?? null,
                    'ratings_count' => $g['ratings_count'] ?? null,
                    'metacritic' => $g['metacritic'] ?? null,
                    'background_image' => $g['background_image'] ?? null,
                    'genres' => array_map(
                        static fn ($x) => is_array($x) ? ($x['slug'] ?? $x['name'] ?? null) : null,
                        $g['genres'] ?? []
                    ),
                ];
            }, $games);

            $ids = array_values(array_filter(array_map(static fn ($g) => is_array($g) ? ($g['id'] ?? null) : null, $games)));
            $ids = array_values(array_unique(array_map('intval', $ids)));

            File::put(
                $basePath."/rawg_top_{$genreSlug}_{$perGenre}.json",
                json_encode($compact, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES)
            );
            File::put(
                $basePath."/rawg_ids_{$genreSlug}_{$perGenre}.json",
                json_encode($ids, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES)
            );

            $this->info("{$genreSlug}: saved ".count($ids).' ids');
        }

        $this->info('Done. Output dir: '.$basePath);

        return self::SUCCESS;
    }

    private function runSingleProcess(string $file, bool $dispatchMedia, bool $dispatchPrices, bool $inline, bool $resolveStores, bool $allowNetwork, int $limit): int
    {
        $startedAt = microtime(true);
        $ids = json_decode((string) File::get($file), true);
        if (! is_array($ids)) {
            $this->error('Invalid JSON file. Expected an array of integers.');

            return self::FAILURE;
        }

        $ids = array_values(array_filter(array_map('intval', $ids)));
        if ($limit > 0) {
            $ids = array_slice($ids, 0, $limit);
        }

        $provider = new RawgProvider;
        Log::info('RAWG ingest: loaded ids (single)', [
            'file' => $file,
            'count' => count($ids),
            'dispatch_media' => $dispatchMedia,
            'dispatch_prices' => $dispatchPrices,
            'inline' => $inline,
            'resolve_stores' => $resolveStores,
        ]);

        $bar = $this->output->createProgressBar(count($ids));
        $bar->start();

        foreach ($ids as $id) {
            // resolve-stores is enabled by default inside RawgProvider ingest.
            // Keeping the flag here for future toggles.
            $result = $provider->ingestRawgId($id, $dispatchMedia);

            if ($result['video_game_id']) {
                if ($dispatchMedia && $inline) {
                    $this->runInlineMedia($result['video_game_id'], $id);
                }

                if ($dispatchPrices && $inline) {
                    $this->runInlinePrices($result['video_game_id']);
                }

                if ($dispatchPrices && ! $inline) {
                    EnrichGamePricesJob::dispatch($result['video_game_id'])
                        ->onQueue('prices-dispatch');
                }
            }
            $bar->advance();
        }

        $bar->finish();
        $this->newLine();

        Log::info('RAWG ingest: processed ids (single)', [
            'file' => $file,
            'count' => count($ids),
            'elapsed_ms' => (int) ((microtime(true) - $startedAt) * 1000),
        ]);

        return self::SUCCESS;
    }

    private function runParallelImport(string $file, int $workers, bool $dispatchMedia, bool $dispatchPrices, bool $inline, bool $resolveStores, bool $allowNetwork, int $limit): int
    {
        $startedAt = microtime(true);
        $phpBinary = PHP_BINARY;
        $artisanPath = base_path('artisan');

        $processes = [];

        for ($i = 1; $i <= $workers; $i++) {
            $cmd = [
                $phpBinary,
                $artisanPath,
                'ingest:rawg',
                '--mode=ingest',
                '--workers=1',
                "--chunk={$i}/{$workers}",
                "--file={$file}",
                '--allow-network=1',
                '--dispatch-media='.(int) $dispatchMedia,
                '--dispatch-prices='.(int) $dispatchPrices,
                '--inline='.(int) $inline,
                '--resolve-stores='.(int) $resolveStores,
                '--limit='.$limit,
            ];

            $process = new Process($cmd);
            $process->setTimeout(null);
            $process->start();
            $processes[$i] = $process;
        }

        while (count($processes) > 0) {
            foreach ($processes as $idx => $proc) {
                if (! $proc->isRunning()) {
                    if ($proc->getExitCode() !== 0) {
                        $this->error("RAWG worker {$idx} failed: ".$proc->getErrorOutput());

                        Log::error('RAWG ingest: worker failed', [
                            'worker' => $idx,
                            'exit_code' => $proc->getExitCode(),
                            'stderr' => $proc->getErrorOutput(),
                        ]);
                    }

                    unset($processes[$idx]);
                }
            }

            sleep(1);
        }

        Log::info('RAWG ingest: parallel workers complete', [
            'file' => $file,
            'workers' => $workers,
            'elapsed_ms' => (int) ((microtime(true) - $startedAt) * 1000),
        ]);

        return self::SUCCESS;
    }

    private function runAsChildWorker(string $chunkSpec, bool $dispatchMedia, bool $dispatchPrices, bool $inline, bool $resolveStores, bool $allowNetwork, int $limit): int
    {
        $startedAt = microtime(true);
        $file = (string) ($this->option('file') ?? '');
        if ($file === '' || ! File::exists($file)) {
            return self::FAILURE;
        }

        if (! preg_match('/^(\d+)\/(\d+)$/', $chunkSpec, $m)) {
            return self::FAILURE;
        }

        $index = (int) $m[1];
        $total = (int) $m[2];

        $ids = json_decode((string) File::get($file), true);
        if (! is_array($ids)) {
            return self::FAILURE;
        }

        $ids = array_values(array_filter(array_map('intval', $ids)));
        if ($limit > 0) {
            $ids = array_slice($ids, 0, $limit);
        }

        $chunkSize = (int) ceil(count($ids) / max(1, $total));
        $offset = max(0, ($index - 1) * $chunkSize);
        $myIds = array_slice($ids, $offset, $chunkSize);

        Log::info('RAWG ingest: child worker slice', [
            'chunk' => $chunkSpec,
            'file' => $file,
            'total_ids' => count($ids),
            'slice_count' => count($myIds),
            'offset' => $offset,
            'dispatch_media' => $dispatchMedia,
            'dispatch_prices' => $dispatchPrices,
            'inline' => $inline,
            'resolve_stores' => $resolveStores,
        ]);

        if (! $allowNetwork) {
            return self::FAILURE;
        }

        $provider = new RawgProvider;
        foreach ($myIds as $id) {
            $result = $provider->ingestRawgId($id, $dispatchMedia);

            if ($result['video_game_id']) {
                if ($dispatchMedia && $inline) {
                    $this->runInlineMedia($result['video_game_id'], $id);
                }

                if ($dispatchPrices && $inline) {
                    $this->runInlinePrices($result['video_game_id']);
                }

                if ($dispatchPrices && ! $inline) {
                    EnrichGamePricesJob::dispatch($result['video_game_id'])
                        ->onQueue('prices-dispatch');
                }
            }
        }

        Log::info('RAWG ingest: child worker complete', [
            'chunk' => $chunkSpec,
            'file' => $file,
            'slice_count' => count($myIds),
            'elapsed_ms' => (int) ((microtime(true) - $startedAt) * 1000),
        ]);

        return self::SUCCESS;
    }

    private function backfillPrices(string $file, bool $dispatchPrices, bool $inline, int $limit): int
    {
        if (! $dispatchPrices) {
            $this->warn('dispatch-prices=0; nothing to do.');

            return 0;
        }

        $videoGameIds = [];

        if ($file !== '' && File::exists($file)) {
            $rawgIds = json_decode((string) File::get($file), true);
            if (! is_array($rawgIds)) {
                $this->error('Invalid JSON file. Expected an array of RAWG IDs.');

                return 0;
            }

            $rawgIds = array_values(array_filter(array_map('intval', $rawgIds)));
            if ($limit > 0) {
                $rawgIds = array_slice($rawgIds, 0, $limit);
            }

            foreach (array_chunk($rawgIds, 500) as $chunk) {
                $ids = VideoGame::query()
                    ->where('provider', 'rawg')
                    ->whereIn('external_id', array_map('strval', $chunk))
                    ->pluck('id')
                    ->all();

                $videoGameIds = array_merge($videoGameIds, $ids);
            }
        } else {
            $query = VideoGame::query()
                ->where('provider', 'rawg')
                ->orderBy('id')
                ->select(['id']);

            if ($limit > 0) {
                $query->limit($limit);
            }

            $videoGameIds = $query->pluck('id')->all();
        }

        $videoGameIds = array_values(array_unique(array_map('intval', $videoGameIds)));

        $bar = $this->output->createProgressBar(count($videoGameIds));
        $bar->start();

        foreach ($videoGameIds as $videoGameId) {
            if ($inline) {
                $this->runInlinePrices($videoGameId);
            } else {
                EnrichGamePricesJob::dispatch($videoGameId)
                    ->onQueue('prices-dispatch');
            }

            $bar->advance();
        }

        $bar->finish();
        $this->newLine();

        return count($videoGameIds);
    }

    private function runInlineMedia(int $videoGameId, int $rawgId): void
    {
        (new FetchRawgDataJob($videoGameId, $rawgId))->handle();
    }

    private function runInlinePrices(int $videoGameId): void
    {
        $game = VideoGame::with('title.sources')->find($videoGameId);
        if (! $game || ! $game->title) {
            return;
        }

        $sources = $game->title->sources;
        if (! $sources) {
            return;
        }

        /** @var VideoGameTitleSource|null $steam */
        $steam = $sources->firstWhere('provider', 'steam_store');
        if ($steam && $steam->external_id) {
            (new ConcurrentFetchSteamDataJob($videoGameId, (int) $steam->external_id))
                ->handle();
        }

        /** @var VideoGameTitleSource|null $ps */
        $ps = $sources->firstWhere('provider', 'playstation_store');
        if ($ps) {
            (new FetchPlayStationStorePricesJob($videoGameId, (int) $ps->id))->handle();
        }

        /** @var VideoGameTitleSource|null $xbox */
        $xbox = $sources->firstWhere('provider', 'xbox');
        if ($xbox) {
            (new FetchXboxStorePricesJob($videoGameId, (int) $xbox->id))
                ->handle(app(XboxStoreService::class));
        }
    }
}
