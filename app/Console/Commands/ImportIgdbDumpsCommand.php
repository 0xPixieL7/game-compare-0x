<?php

declare(strict_types=1);

namespace App\Console\Commands;

use App\Models\Image;
use App\Models\Product;
use App\Models\Video;
use App\Models\VideoGame;
use App\Models\VideoGameSource;
use App\Models\VideoGameTitle;
use App\Models\VideoGameTitleSource;
use App\Services\Graph\GameGraphService;
use App\Services\Import\Concerns\CanOptimizeImport;
use App\Services\Import\Concerns\HasProgressBar;
use App\Services\Normalization\IgdbRatingHelper;
use App\Services\Normalization\PlatformNormalizer;
use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\File;
use Illuminate\Support\Facades\Log;
use Illuminate\Support\Str;
use Symfony\Component\Console\Helper\ProgressBar;

class ImportIgdbDumpsCommand extends Command
{
    use CanOptimizeImport;
    use HasProgressBar;

    protected $signature = 'gc:import-igdb {--path= : Directory containing IGDB dump files, or a specific dump file path (e.g. *_games.csv)} {--games-file= : Internal: specific games CSV file to process (used by parallel workers)} {--provider=igdb : Provider key for video_game_sources.provider} {--limit=0 : Optional record limit for games} {--resume=1 : Resume from the last saved checkpoint (1/0)} {--reset-checkpoint : Ignore and delete any existing checkpoint for this import target} {--merge-media=0 : Merge with existing media (1) or skip lookups for speed (0)} {--progress-chunk=0 : Only refresh progress bar every N records (0 = auto)} {--fixed-offsets : Use original fixed ID offsets to avoid clashes across reruns} {--fast=0 : Skip per-record fallback and expensive lookups for speed} {--workers=1 : Number of parallel workers for CSV processing} {--chunk= : Internal: process specific chunk (format: N/TOTAL, e.g., 1/4)}';

    protected $description = 'Import IGDB dump CSV/JSON files into products, sources, titles, and video games (streamed to avoid high memory).';

    /**
     * In-memory caches to reduce database lookups.
     * Storing IDs only to save memory.
     */
    private array $productCache = [];

    private array $sourceCache = [];

    private array $titleCache = [];

    /**
     * Batch queue for bulk inserts.
     */
    private array $videoGameBatch = [];

    /**
     * Batch queue for provider-item mappings (video_game_title_sources).
     *
     * IMPORTANT: This table stores per-provider per-item IDs + payloads.
     * `video_game_sources` remains provider-level only (one row per provider).
     */
    private array $videoGameTitleSourceBatch = [];

    /**
     * Batch queue for aggregating image media per video game.
     * Structure: ['video_game_id' => ['urls' => [...], 'metadata' => [...]]]
     *
     * @var array<int, array{urls: array<int, string>, metadata: array<string, mixed>}>
     */
    private array $imageBatch = [];

    /**
     * Batch queue for aggregating video media per video game.
     * Structure: ['video_game_id' => ['urls' => [...], 'provider' => string, 'metadata' => [...]]]
     *
     * @var array<int, array{urls: array<int, string>, provider: string, metadata: array<int, mixed>}>
     */
    private array $videoBatch = [];

    private const BATCH_SIZE = 4000;

    private const MEDIA_BATCH_SIZE = 2500;

    /**
     * Maximum safe parameter count for PostgreSQL bulk operations.
     * PostgreSQL can handle ~65k parameters. We use a high but safe value.
     *
     * This limit applies to the number of columns × rows in a single batch.
     */
    private const MAX_SAFE_PARAMS = 65000;

    /**
     * Number of parsed game records to buffer before running a set-based write.
     */
    private const RECORD_BUFFER_SIZE = 10000;

    private const CHECKPOINT_ROWS_INTERVAL = 10000;

    private const CHECKPOINT_SECONDS_INTERVAL = 60.0;

    private bool $mergeMedia;

    private bool $fastMode = false;

    private int $progressChunk;

    private array $tableIdOffsets = [];

    private array $tableIdCounters = [];

    private array $fileTotalRowsCache = [];

    private ?PlatformNormalizer $platformNormalizer = null;

    private array $platformNormalizationCache = [];

    private array $genreCache = [];

    private array $involvedCompanyCache = [];

    private ?IgdbRatingHelper $igdbRatingHelper = null;

    private ?GameGraphService $graphService = null;

    /**
     * Map IGDB platform family IDs to names.
     *
     * @var array<int, string>
     */
    private array $platformFamilyIdToName = [];

    /**
     * Map IGDB platform logo IDs to URLs.
     *
     * @var array<int, string>
     */
    private array $platformLogoIdToUrl = [];

    public function getName(): ?string
    {
        return 'gc:import-igdb';
    }

    public function handle(): int
    {
        // Disable Telescope for performance
        try {
            if (class_exists(\Laravel\Telescope\Telescope::class)) {
                \Laravel\Telescope\Telescope::stopRecording();
            }
        } catch (\Throwable $e) {
            // Ignore if Telescope is not installed or enabled
        }

        config(['telescope.enabled' => false]);

        // Detect if running as child worker (parallel import)
        $isChildWorker = $this->option('chunk') !== null;

        // Start optimized import session (UNLOGGED tables, deferred constraints)
        // Only parent process should toggle table state to avoid race conditions
        if (! $isChildWorker) {
            $this->startOptimizedImport();

            // CRITICAL: Guarantee cleanup runs even if command crashes
            // This prevents tables from staying UNLOGGED permanently
            $cleanup = fn () => $this->endOptimizedImport();
            defer($cleanup)->always();

            // Handle Ctrl+C and termination signals for graceful cleanup
            // Ensures tables are restored to LOGGED even when interrupted
            $this->trap([SIGTERM, SIGINT], function () use ($cleanup) {
                $this->warn("\n⚠️  Import interrupted - restoring table logging...");
                $cleanup();
                exit(1);
            });
        }

        // Parse flags
        $this->mergeMedia = (int) $this->option('merge-media') !== 0;
        $this->fastMode = (int) $this->option('fast') !== 0;
        $this->progressChunk = max(0, (int) $this->option('progress-chunk'));
        $fixedOffsets = (bool) $this->option('fixed-offsets');

        $this->graphService = new GameGraphService;
        // Ensure database is initialized before workers start
        $this->graphService->beginTransaction();
        $this->graphService->commit();

        // Download new dumps if requested or if directory is empty
        $inputPath = (string) ($this->option('path') ?: base_path('storage/igdb-dumps'));
        if (! File::exists($inputPath) || count(File::files($inputPath)) < 5) {
            $this->info('📥 Downloading latest IGDB dumps...');
            $endpoints = ['games', 'platforms', 'genres', 'companies', 'involved_companies', 'covers', 'screenshots', 'artworks', 'game_videos'];
            foreach ($endpoints as $endpoint) {
                $this->info("   Downloading {$endpoint}...");
                $this->call('igdb:dump:download', ['endpoint' => $endpoint, '--output-dir' => 'igdb-dumps']);
            }
        }

        // Validate batch size configuration
        $this->validateBatchConfiguration();

        $startTime = microtime(true);
        $provider = (string) ($this->option('provider') ?: 'igdb');
        $limit = (int) $this->option('limit');

        $this->info('=== IGDB Import Started ===');
        $this->info("Path: {$inputPath}");
        $this->info("Provider: {$provider}");
        $this->info('Limit: '.($limit > 0 ? $limit : 'unlimited'));
        $this->info('Merge media: '.($this->mergeMedia ? 'on (will merge existing)' : 'off (skip existing lookups for speed)'));
        $this->info('Progress chunk: '.($this->progressChunk > 0 ? $this->progressChunk : 'auto'));
        $this->info('Fixed offsets: '.($fixedOffsets ? 'on (using original IDs)' : 'off (compute from DB)'));
        $this->newLine();

        if (! File::exists($inputPath)) {
            $this->error("Path does not exist: {$inputPath}");

            return self::FAILURE;
        }

        $directory = $inputPath;
        $explicitGamesFile = null;

        if (File::isFile($inputPath)) {
            $directory = dirname($inputPath);

            $basename = strtolower(basename($inputPath));
            $ext = strtolower(pathinfo($inputPath, PATHINFO_EXTENSION));
            $isSchemaArtifact = str_ends_with($basename, '_schema.json') || str_ends_with($basename, 'schema.json');

            if (str_contains($basename, 'games') && in_array($ext, ['csv', 'json', 'ndjson', 'jsonl'], true) && ! $isSchemaArtifact) {
                $explicitGamesFile = $inputPath;
            }
        }

        if (! File::isDirectory($directory)) {
            $this->error("Directory does not exist: {$directory}");

            return self::FAILURE;
        }

        if ($directory !== $inputPath) {
            $this->info("Resolved dump directory: {$directory}");
            $this->newLine();
        }

        // Calculate ID offsets from existing data to prevent clashes
        $this->calculateTableIdOffsets($fixedOffsets);
        $this->alignPostgresSequences();
        $this->info('📊 ID Offsets calculated:');
        foreach ($this->tableIdOffsets as $table => $offset) {
            $this->line("   {$table}: starting from ID ".($offset + 1));
        }
        $this->newLine();

        // Check for --games-file option (used by parallel workers)
        $gamesFileOption = $this->option('games-file');
        $gamesFile = $gamesFileOption ?: ($explicitGamesFile ?: $this->findFile($directory, 'games'));
        if (! $gamesFile) {
            $this->error('No games CSV/JSON file found.');

            return self::FAILURE;
        }

        // Load reference dumps first so games can be cross-referenced on insert.
        // Media runs last because it depends on the `video_games` rows being present.
        $this->loadPlatformFamilyIdToNameMap($directory);
        $this->loadPlatformLogoIdToUrlMap($directory);
        $this->loadPlatformIdToNameMap($directory);
        $this->loadGenreIdToNameMap($directory);
        $this->loadCompanyAndInvolvedCompanyMaps($directory);

        // Pre-create/warm the source provider to avoid race conditions in parallel workers
        VideoGameSource::query()->firstOrCreate(['provider' => $provider]);

        // Check for parallel worker mode
        $workers = max(1, (int) $this->option('workers'));
        $chunkSpec = $this->option('chunk');

        // If this is a child worker process (has --chunk), process only our assigned chunk
        if ($chunkSpec !== null && $chunkSpec !== '') {
            return $this->runAsChildWorker($gamesFile, $provider, $limit, $chunkSpec, $startTime);
        }

        // If multiple workers requested, spawn child processes and return early
        if ($workers > 1) {
            $this->runParallelImport($gamesFile, $provider, $limit, $workers, $directory, $startTime);

            $this->newLine();
            $this->info('=== Post-Import Steps ===');
            $this->info('🚀 Running Retailer Extraction...');
            $this->call('app:extract-retailers');

            return self::SUCCESS;
        }

        // Single worker mode
        $this->info('📥 Importing games (streaming)...');
        $this->info("File: {$gamesFile}");
        $this->newLine();

        $processed = $this->processGamesStreaming($gamesFile, $provider, $limit);

        $this->newLine();
        $this->info("✅ Processed {$processed} game rows.");

        if ($limit > 0) {
            $this->warn('NOTE: This was a limited run (--limit set). Only that many rows were imported.');
            $this->line('Tip: omit --limit (or set --limit=0) for a full import. Use --reset-checkpoint to restart from the beginning.');
        }

        // Flush remaining batches.
        $this->flushBatches();

        $this->updateProviderItemsCount($provider);

        $this->newLine();
        $this->info('📸 Processing media files...');

        $mediaStats = [
            'covers' => $this->processMediaIfPresent($directory, 'covers', function ($g, array $row): void {
                $this->addImageMedia($g, $row, 'cover_images', true);
            }, $provider),
            'screenshots' => $this->processMediaIfPresent($directory, 'screenshots', function ($g, array $row): void {
                $this->addImageMedia($g, $row, 'screenshots', false);
            }, $provider),
            'artworks' => $this->processMediaIfPresent($directory, 'artworks', function ($g, array $row): void {
                $this->addImageMedia($g, $row, 'artworks', false);
            }, $provider),
            'videos' => $this->processMediaIfPresent($directory, 'videos', function ($g, array $row): void {
                $this->addVideoMedia($g, $row);
            }, $provider),
        ];

        $this->newLine();
        $this->info('🔗 Processing auxiliary data (websites, external links, alt names)...');

        $auxStats = [
            'websites' => $this->processAuxiliaryIfPresent($directory, 'websites', 'video_game_websites', function (array $row): ?array {
                return [
                    'video_game_id' => (int) ($row['game'] ?? 0),
                    'category' => (int) ($row['category'] ?? 0),
                    'url' => $row['url'] ?? '',
                    'trusted' => (bool) ($row['trusted'] ?? false),
                    'created_at' => now(),
                    'updated_at' => now(),
                ];
            }, $provider),
            'external_links' => $this->processAuxiliaryIfPresent($directory, 'external_games', 'video_game_external_links', function (array $row): ?array {
                return [
                    'video_game_id' => (int) ($row['game'] ?? 0),
                    'category' => (int) ($row['category'] ?? 0),
                    'external_id' => (string) ($row['uid'] ?? $row['external_id'] ?? ''),
                    'url' => $row['url'] ?? null,
                    'created_at' => now(),
                    'updated_at' => now(),
                ];
            }, $provider),
            'alternative_names' => $this->processAuxiliaryIfPresent($directory, 'alternative_names', 'video_game_alternative_names', function (array $row): ?array {
                return [
                    'video_game_id' => (int) ($row['game'] ?? 0),
                    'name' => $row['name'] ?? '',
                    'comment' => $row['comment'] ?? null,
                    'created_at' => now(),
                    'updated_at' => now(),
                ];
            }, $provider),
        ];

        // CRITICAL: Backfill steam_id and official_url on products table from auxiliary data
        $this->newLine();
        $this->info('🔄 Backfilling Steam IDs and Official URLs to products...');

        // Steam is category 1 in external_games
        DB::statement("
            UPDATE products p
            SET external_ids = p.external_ids || jsonb_build_object('steam', el.external_id)
            FROM video_games vg
            JOIN video_game_external_links el ON el.video_game_id = vg.id
            WHERE vg.video_game_title_id = (SELECT id FROM video_game_titles WHERE product_id = p.id LIMIT 1)
            AND el.category = 1
            AND NOT (p.external_ids ?? 'steam')
        ");

        // Official site is category 1 in websites
        DB::statement("
            UPDATE products p
            SET metadata = p.metadata || jsonb_build_object('official_url', w.url)
            FROM video_games vg
            JOIN video_game_websites w ON w.video_game_id = vg.id
            WHERE vg.video_game_title_id = (SELECT id FROM video_game_titles WHERE product_id = p.id LIMIT 1)
            AND w.category = 1
            AND NOT (p.metadata ?? 'official_url')
        ");

        $this->info('🚀 Triggering Synchronous CSV Import...');
        $this->call('import:csvs');

        // CRITICAL: Flush remaining batches after all CSV processing
        $this->flushBatches();

        $duration = round(microtime(true) - $startTime, 2);

        $this->newLine(2);
        $this->info('=== Import Complete ===');
        $this->table(
            ['Metric', 'Count'],
            [
                ['Games Processed', $processed],
                ['Covers', $mediaStats['covers']],
                ['Screenshots', $mediaStats['screenshots']],
                ['Artworks', $mediaStats['artworks']],
                ['Videos', $mediaStats['videos']],
                ['Websites', $auxStats['websites']],
                ['External Links', $auxStats['external_links']],
                ['Alt Names', $auxStats['alternative_names']],
                ['Duration', "{$duration}s"],
            ]
        );

        $this->newLine();
        $this->info('=== Post-Import Steps ===');

        // Collect all imported game IDs for propagation
        $importedGameIds = array_map(
            fn ($row) => $row['id'] ?? null,
            $this->videoGameBatch
        );
        $importedGameIds = array_filter($importedGameIds, fn ($id) => ! is_null($id));

        // Dispatch job to propagate media and enrichment (images/videos, fill missing data)
        if (! empty($importedGameIds)) {
            \App\Jobs\PropagateMediaAndEnrichmentJob::dispatch($importedGameIds, true);
            $this->info('🖼️ PropagateMediaAndEnrichmentJob dispatched for imported games.');
        } else {
            $this->warn('No imported game IDs found for propagation.');
        }

        $this->info('🚀 Running Retailer Extraction...');
        $this->call('app:extract-retailers');

        // Show performance report
        // Note: endOptimizedImport() is handled by defer()->always() at line 169
        $this->outputPerformanceReport();

        $this->newLine();
        $this->info('🧠 Building Game Graph (Rust)...');
        $this->buildGraphWithRust();

        $this->info('📥 Importing graph relationships into local SQLite...');
        $this->importGraphExport();

        $this->info('✓ Graph updated successfully');

        return self::SUCCESS;
    }

    private function buildGraphWithRust(): void
    {
        $this->info('   Executing Rust graph builder...');
        $process = new \Symfony\Component\Process\Process(['./rust/target/release/build_game_graph']);
        $process->setWorkingDirectory(base_path());
        $process->setTimeout(300);
        $process->run();

        if (! $process->isSuccessful()) {
            $this->error('   Rust graph builder failed: '.$process->getErrorOutput());
        } else {
            $this->info('   Rust graph builder finished.');
        }
    }

    private function importGraphExport(): void
    {
        $path = storage_path('app/game_graph_export.jsonl');
        if (! File::exists($path)) {
            $this->warn('   Graph export file not found, skipping.');

            return;
        }

        $handle = fopen($path, 'r');
        $this->graphService->beginTransaction();

        $count = 0;
        while (($line = fgets($handle)) !== false) {
            $item = json_decode($line, true);
            if (! $item) {
                continue;
            }

            if ($item['type'] === 'node') {
                $data = $item['data'];
                $this->graphService->addNode($data['type_'], $data['id'], $data['label'], $data['prices'] ?? null);
            } elseif ($item['type'] === 'edge') {
                $data = $item['data'];
                // We need to resolve node IDs to internal SQLite IDs
                // For simplicity, we'll let addNode handle the resolution/insertion
                $fromId = $this->graphService->addNode('GAME', $data['from']);
                $toId = $this->graphService->addNode('GAME', $data['to']);
                $this->graphService->addEdge($fromId, $toId, $data['type_']);
            }

            $count++;
            if ($count % 1000 === 0) {
                $this->graphService->commit();
                $this->graphService->beginTransaction();
            }
        }

        $this->graphService->commit();
        fclose($handle);
        $this->info("   Imported {$count} graph elements.");
    }

    /**
     * Run import in parallel using multiple child processes.
     *
     * Spawns N worker processes, each processing a distinct chunk of the CSV file.
     * Uses byte-range based chunking to avoid reading the entire file.
     */
    private function runParallelImport(string $gamesFile, string $provider, int $limit, int $workers, string $directory, float $startTime): int
    {
        $this->info("🚀 Starting parallel import with {$workers} workers...");
        $this->newLine();

        // Calculate file size and chunk byte ranges
        $fileSize = filesize($gamesFile);
        if ($fileSize === false || $fileSize < 1000) {
            $this->warn('File too small for parallel processing, falling back to single worker.');

            return $this->processGamesStreaming($gamesFile, $provider, $limit);
        }

        // Count total rows for progress reporting
        $totalRows = $limit > 0 ? $limit : $this->countFileRows($gamesFile, false);
        $this->info("Total rows to process: ~{$totalRows}");

        // Build the base command for child workers
        $phpBinary = PHP_BINARY;
        $artisanPath = base_path('artisan');

        $baseArgs = [
            $phpBinary,
            $artisanPath,
            'gc:import-igdb',
            '--path='.$directory,  // FIXED: Pass directory, not games file
            '--games-file='.$gamesFile,  // Pass specific games file separately
            '--provider='.$provider,
            '--fast='.($this->fastMode ? '1' : '0'),
            '--merge-media='.($this->mergeMedia ? '1' : '0'),
            '--resume=0', // Child workers don't use checkpoints
            '--workers=1', // Child runs as single worker
        ];

        if ($limit > 0) {
            // Distribute limit across workers
            $perWorker = (int) ceil($limit / $workers);
            $baseArgs[] = '--limit='.$perWorker;
        }

        // Spawn child processes
        $processes = [];
        $pipes = [];

        for ($i = 1; $i <= $workers; $i++) {
            $args = $baseArgs;
            $args[] = "--chunk={$i}/{$workers}";

            $cmd = implode(' ', array_map('escapeshellarg', $args));

            $descriptorSpec = [
                0 => ['pipe', 'r'], // stdin
                1 => ['pipe', 'w'], // stdout
                2 => ['pipe', 'w'], // stderr
            ];

            $process = proc_open($cmd, $descriptorSpec, $processPipes, base_path());

            if (is_resource($process)) {
                $processes[$i] = $process;
                $pipes[$i] = $processPipes;

                // Close stdin, we don't need it
                fclose($processPipes[0]);

                // Make stdout/stderr non-blocking
                stream_set_blocking($processPipes[1], false);
                stream_set_blocking($processPipes[2], false);

                $this->line("  Started worker {$i}/{$workers}");
            } else {
                $this->error("Failed to start worker {$i}");
            }
        }

        $this->newLine();
        $this->info('⏳ Waiting for workers to complete...');

        // Monitor workers and collect output
        $results = [];
        $running = count($processes);

        while ($running > 0) {
            foreach ($processes as $i => $process) {
                if (! is_resource($process)) {
                    continue;
                }

                $status = proc_get_status($process);

                if (! $status['running']) {
                    // Collect final output
                    $stdout = stream_get_contents($pipes[$i][1]);
                    $stderr = stream_get_contents($pipes[$i][2]);

                    fclose($pipes[$i][1]);
                    fclose($pipes[$i][2]);

                    $exitCode = proc_close($process);
                    $processes[$i] = null;
                    $running--;

                    // Parse result from stdout (look for processed count)
                    $processed = 0;
                    if (preg_match('/WORKER_RESULT:(\d+)/', $stdout, $matches)) {
                        $processed = (int) $matches[1];
                    }

                    $results[$i] = [
                        'processed' => $processed,
                        'exit_code' => $exitCode,
                        'stderr' => $stderr,
                    ];

                    $status_icon = $exitCode === 0 ? '✓' : '✗';
                    $this->line("  {$status_icon} Worker {$i} finished: {$processed} records".($exitCode !== 0 ? " (exit: {$exitCode})" : ''));
                }
            }

            // Small sleep to avoid busy-waiting
            usleep(100000); // 100ms
        }

        // Aggregate results
        $totalProcessed = array_sum(array_column($results, 'processed'));

        $this->newLine();
        $this->info("📊 Parallel import complete: {$totalProcessed} total records processed");

        // Now process media files (single-threaded, depends on video_games being populated)
        $this->newLine();
        $this->info('📸 Processing media files...');

        $mediaStats = [
            'covers' => $this->processMediaIfPresent($directory, 'covers', function ($g, array $row): void {
                $this->addImageMedia($g, $row, 'cover_images', true);
            }, $provider),
            'screenshots' => $this->processMediaIfPresent($directory, 'screenshots', function ($g, array $row): void {
                $this->addImageMedia($g, $row, 'screenshots', false);
            }, $provider),
            'artworks' => $this->processMediaIfPresent($directory, 'artworks', function ($g, array $row): void {
                $this->addImageMedia($g, $row, 'artworks', false);
            }, $provider),
            'videos' => $this->processMediaIfPresent($directory, 'videos', function ($g, array $row): void {
                $this->addVideoMedia($g, $row);
            }, $provider),
        ];

        // CRITICAL: Flush remaining media batches after all CSV processing
        $this->flushBatches();

        $duration = round(microtime(true) - $startTime, 2);

        $this->newLine(2);
        $this->info('=== Parallel Import Complete ===');
        $this->table(
            ['Metric', 'Count'],
            [
                ['Workers', $workers],
                ['Games Processed', $totalProcessed],
                ['Covers', $mediaStats['covers']],
                ['Screenshots', $mediaStats['screenshots']],
                ['Artworks', $mediaStats['artworks']],
                ['Videos', $mediaStats['videos']],
                ['Duration', "{$duration}s"],
            ]
        );

        return $totalProcessed;
    }

    /**
     * Run as a child worker processing a specific chunk of the CSV.
     */
    private function runAsChildWorker(string $gamesFile, string $provider, int $limit, string $chunkSpec, float $startTime): int
    {
        // Parse chunk spec: "N/TOTAL" e.g., "2/4" means worker 2 of 4
        if (! preg_match('/^(\d+)\/(\d+)$/', $chunkSpec, $matches)) {
            fwrite(STDERR, "Invalid chunk spec: {$chunkSpec}\n");

            return self::FAILURE;
        }

        $workerNum = (int) $matches[1];
        $totalWorkers = (int) $matches[2];

        if ($workerNum < 1 || $workerNum > $totalWorkers) {
            fwrite(STDERR, "Invalid worker number: {$workerNum}/{$totalWorkers}\n");

            return self::FAILURE;
        }

        // CRITICAL: Load reference data (genres, companies, platforms)
        // Workers receive --path=directory (not the games file)
        $directory = $this->option('path') ?? dirname($gamesFile);
        try {
            $this->loadGenreIdToNameMap($directory);
            $this->loadPlatformIdToNameMap($directory);
            $this->loadCompanyAndInvolvedCompanyMaps($directory);
            fwrite(STDOUT, "Worker {$workerNum}: Loaded reference data\n");
        } catch (\Throwable $e) {
            fwrite(STDERR, "Worker {$workerNum}: Failed to load reference data: {$e->getMessage()}\n");

            return self::FAILURE;
        }

        // Process only our chunk of the file
        $processed = $this->processGamesStreamingChunk($gamesFile, $provider, $limit, $workerNum, $totalWorkers);

        // Flush remaining batches
        $this->flushBatches();

        // Output result marker for parent to parse
        fwrite(STDOUT, "WORKER_RESULT:{$processed}\n");

        return self::SUCCESS;
    }

    /**
     * Process a specific chunk of the CSV file (for parallel workers).
     */
    private function processGamesStreamingChunk(string $file, string $provider, int $limit, int $workerNum, int $totalWorkers): int
    {
        $handle = fopen($file, 'rb');
        if (! $handle) {
            return 0;
        }

        // Read header
        $headers = fgetcsv($handle) ?: [];
        $headerEndPos = ftell($handle);

        // Calculate file size and chunk boundaries
        $fileSize = filesize($file);
        if ($fileSize === false) {
            fclose($handle);

            return 0;
        }

        $dataSize = $fileSize - $headerEndPos;
        $chunkSize = (int) ceil($dataSize / $totalWorkers);

        $startByte = $headerEndPos + ($chunkSize * ($workerNum - 1));
        $endByte = ($workerNum === $totalWorkers) ? $fileSize : $headerEndPos + ($chunkSize * $workerNum);

        // Seek to start position
        if ($workerNum > 1) {
            fseek($handle, $startByte);
            // Skip to next line boundary (we may be mid-line)
            fgets($handle);
        }

        $processed = 0;
        $errors = 0;
        $recordBuffer = [];

        $now = now();
        $source = VideoGameSource::query()->firstOrCreate(['provider' => $provider]);
        $this->sourceCache[$provider] = $source;

        $flushBuffer = function () use (&$recordBuffer, $provider, &$errors): void {
            if ($recordBuffer === []) {
                return;
            }
            try {
                $errors += $this->processGameRecordsBatch($recordBuffer, $provider);
            } catch (\Throwable $e) {
                $errors += count($recordBuffer);
            }
            $recordBuffer = [];
            $this->flushBatches();
        };

        while (($row = fgetcsv($handle)) !== false) {
            $currentPos = ftell($handle);

            // Stop if we've passed our end boundary
            if ($currentPos !== false && $currentPos > $endByte) {
                break;
            }

            // Skip malformed rows
            if ($row === null || count($row) !== count($headers)) {
                continue;
            }

            $record = array_combine($headers, $row);
            if ($record !== false) {
                $recordBuffer[] = $record;
                $processed++;

                if (count($recordBuffer) >= self::RECORD_BUFFER_SIZE) {
                    $flushBuffer();
                }

                if ($limit > 0 && $processed >= $limit) {
                    break;
                }
            }
        }

        $flushBuffer();
        fclose($handle);

        return $processed;
    }

    /**
     * Chunk an array of associative rows to stay under conservative bind/parameter limits.
     *
     * SQLite has a hard parameter limit (commonly 999). Even on MySQL/Postgres,
     * smaller chunks reduce risk during large imports.
     *
     * @param  array<int, array<string, mixed>>  $rows
     * @return array<int, array<int, array<string, mixed>>>
     */
    private function chunkRowsForSafeParams(array $rows, int $preferredChunkSize): array
    {
        if ($rows === []) {
            return [];
        }

        $first = reset($rows);
        if (! is_array($first) || $first === []) {
            return array_chunk($rows, max(1, $preferredChunkSize));
        }

        $columnCount = count($first);
        $safeMax = (int) floor(self::MAX_SAFE_PARAMS / max(1, $columnCount));

        $chunkSize = max(1, min($preferredChunkSize, $safeMax));

        return array_chunk($rows, $chunkSize);
    }

    /**
     * Validates that BATCH_SIZE is safe for database parameter limits.
     *
     * Warns if the batch size might exceed SQLite's 999 parameter limit.
     */
    private function validateBatchConfiguration(): void
    {
        // Estimate: video_games table has ~14 columns
        // products table has ~6 columns
        // video_game_title_sources has ~5 columns
        $estimatedMaxColumns = 14;

        $paramsPerBatch = self::BATCH_SIZE * $estimatedMaxColumns;

        if ($paramsPerBatch > self::MAX_SAFE_PARAMS) {
            $this->warn(
                'BATCH_SIZE ('.self::BATCH_SIZE.") × max columns ({$estimatedMaxColumns}) = {$paramsPerBatch} params, ".
                'which exceeds the safe limit of '.self::MAX_SAFE_PARAMS.'.'
            );
            $this->warn('Consider reducing BATCH_SIZE to avoid SQLite parameter errors.');
            $this->newLine();
        }
    }

    private function calculateTableIdOffsets(bool $fixedOffsets = false): void
    {
        $tables = [
            'products',
            'video_game_titles',
            'video_games',
            'video_game_title_sources',
            'images',
            'videos',
        ];

        if ($fixedOffsets) {
            $this->tableIdOffsets = [
                'products' => 5724,
                'video_game_titles' => 5724,
                'video_games' => 5724,
                'video_game_title_sources' => 5724,
                'images' => 16981,
                'videos' => 2717,
            ];
            foreach ($tables as $table) {
                $this->tableIdCounters[$table] = 0;
            }

            return;
        }

        foreach ($tables as $table) {
            $maxId = (int) DB::table($table)->max('id');
            $this->tableIdOffsets[$table] = $maxId;
            $this->tableIdCounters[$table] = 0;
        }
    }

    private function alignPostgresSequences(): void
    {
        if (DB::getDriverName() !== 'pgsql') {
            return;
        }

        $tables = [
            'products',
            'video_game_titles',
            'video_games',
            'video_game_title_sources',
            'images',
            'videos',
        ];

        foreach ($tables as $table) {
            $sequence = DB::selectOne("SELECT pg_get_serial_sequence(?, 'id') AS sequence_name", [$table]);
            $sequenceName = $sequence?->sequence_name ?? null;
            if (! is_string($sequenceName) || $sequenceName === '') {
                continue;
            }

            $maxId = (int) (DB::table($table)->max('id') ?? 0);
            if ($maxId <= 0) {
                continue;
            }

            DB::statement('SELECT setval(?, ?, true)', [$sequenceName, $maxId]);
        }
    }

    private function updateProviderItemsCount(string $provider): void
    {
        $source = VideoGameSource::query()->where('provider', $provider)->first();
        if (! $source) {
            return;
        }

        $count = VideoGameTitleSource::query()
            ->where('video_game_source_id', $source->id)
            ->distinct()
            ->count('provider_item_id');

        $source->forceFill([
            'items_count' => $count,
        ])->save();
    }

    private function findFile(string $directory, string $basename): ?string
    {
        $candidates = [];
        foreach (File::files($directory) as $file) {
            $filename = $file->getFilename();
            $name = strtolower($filename);
            $ext = strtolower($file->getExtension());

            // Strict matching: pattern must be at the end of the name before extension
            // e.g. "12345_genres.csv" matches "genres", but "12345_involved_companies.csv" won't match "companies"
            $basenamePattern = '_'.$basename.'.'.$ext;
            if (! str_ends_with($name, $basenamePattern)) {
                continue;
            }

            if (! in_array($ext, ['csv', 'json', 'ndjson', 'jsonl'], true)) {
                continue;
            }

            // Never treat schema artifacts as import payloads.
            if (str_ends_with($name, '_schema.json') || str_ends_with($name, 'schema.json')) {
                continue;
            }

            // EXCLUSION: "companies" should not match "involved_companies"
            if ($basename === 'companies' && str_contains($name, 'involved_companies')) {
                continue;
            }

            // Skip empty files (less than 100 bytes - likely just headers or corrupted).
            if ($file->getSize() < 100) {
                continue;
            }

            $candidates[] = $file;
        }

        if ($candidates === []) {
            return null;
        }

        // Prefer CSV payloads when both CSV and JSON variants exist.
        usort($candidates, function ($a, $b): int {
            $aExt = strtolower($a->getExtension());
            $bExt = strtolower($b->getExtension());

            if ($aExt !== $bExt) {
                if ($aExt === 'csv') {
                    return -1;
                }
                if ($bExt === 'csv') {
                    return 1;
                }
            }

            // Prefer LATEST timestamped dumps (highest timestamp = most recent).
            $aName = strtolower($a->getFilename());
            $bName = strtolower($b->getFilename());

            $aTs = preg_match('/^(\d+)_/', $aName, $m1) === 1 ? (int) $m1[1] : 0;
            $bTs = preg_match('/^(\d+)_/', $bName, $m2) === 1 ? (int) $m2[1] : 0;
            if ($aTs !== $bTs) {
                // Sort DESC - LATEST timestamp first (1768197600 before 1767852000).
                return $bTs <=> $aTs;
            }

            // Finally, prefer larger files (likely the real payload).
            return $b->getSize() <=> $a->getSize();
        });

        return $candidates[0]->getPathname();
    }

    private function processGamesStreaming(string $file, string $provider, int $limit): int
    {
        $ext = strtolower(pathinfo($file, PATHINFO_EXTENSION));

        if ($ext === 'csv') {
            return $this->processGamesCsv($file, $provider, $limit);
        }

        return $this->processGamesJson($file, $provider, $limit);
    }

    private function processGamesCsv(string $file, string $provider, int $limit): int
    {
        $this->info('DEBUG: Opening CSV file...');
        $handle = fopen($file, 'r');
        if (! $handle) {
            $this->warn("Could not open games file: {$file}");

            return 0;
        }

        $this->info('DEBUG: Reading CSV headers...');
        $headers = fgetcsv($handle) ?: [];
        $this->info('DEBUG: Found '.count($headers).' columns');

        $resumeEnabled = (int) $this->option('resume') !== 0;
        if ($this->fastMode) {
            $resumeEnabled = false;
        }
        $resetCheckpoint = (bool) $this->option('reset-checkpoint');

        if ($resetCheckpoint) {
            $this->forgetCheckpoint($file, $provider);
        }

        $checkpoint = $resumeEnabled && ! $resetCheckpoint ? $this->loadCheckpoint($file, $provider) : null;

        $processed = 0;
        $errors = 0;

        try {
            $totalRows = $limit > 0 ? $limit : ($this->fileTotalRowsCache[$file] ?? $checkpoint['total_rows'] ?? null);
            if ($totalRows === null) {
                $this->info("DEBUG: Calculating total rows (limit={$limit})...");
                $totalRows = $this->countFileRows($file, true);
                $this->fileTotalRowsCache[$file] = $totalRows;
            }
            $this->info("DEBUG: Total rows = {$totalRows}");

            $startTime = microtime(true);
            $lastOutputTime = microtime(true);

            // Skip to checkpoint if applicable
            if ($checkpoint) {
                $this->maybeSeekToCheckpoint($handle, $file, $provider);
            }

            // Create progress bar with 3 second updates
            $progressBar = $this->output->createProgressBar($totalRows);
            $progressBar->setFormat(' %current%/%max% [%bar%] %percent:3s%% | %elapsed:6s% | ~%remaining:6s%');
            $progressBar->start();

            $read = $checkpoint['processed'] ?? 0;
            $processed = $checkpoint['processed'] ?? 0;
            $errors = $checkpoint['errors'] ?? 0;

            $lastCheckpointAt = microtime(true);
            $lastCheckpointRows = 0;

            /** @var array<int, array<string, mixed>> $recordBuffer */
            $recordBuffer = [];

            $flushRecordBuffer = function () use (&$recordBuffer, $provider, &$errors): void {
                if ($recordBuffer === []) {
                    return;
                }

                try {
                    $errors += $this->processGameRecordsBatch($recordBuffer, $provider);
                } catch (\Throwable $e) {
                    if (! $this->fastMode) {
                        Log::error('Failed to import game batch; falling back to per-record handling', [
                            'error' => $e->getMessage(),
                            'provider' => $provider,
                            'batch_size' => count($recordBuffer),
                        ]);

                        foreach ($recordBuffer as $record) {
                            try {
                                $this->processGameRecord($record, $provider);
                            } catch (\Throwable $inner) {
                                $errors++;
                                Log::error('Failed to import game (fallback path)', [
                                    'record' => $record,
                                    'error' => $inner->getMessage(),
                                ]);
                            }
                        }
                    } else {
                        $errors += count($recordBuffer);
                    }
                } finally {
                    $recordBuffer = [];
                    $this->flushBatches();
                }
            };

            $maybeCheckpoint = function (bool $force = false) use ($handle, $file, $provider, $limit, $resumeEnabled, $flushRecordBuffer, &$lastCheckpointAt, &$lastCheckpointRows, &$processed, &$errors, $totalRows): void {
                // ALWAYS flush buffer before checkpointing or finishing
                $flushRecordBuffer();

                if (! $resumeEnabled) {
                    return;
                }

                if (! $force && $limit > 0) {
                    return;
                }

                $rowsSince = $processed - $lastCheckpointRows;
                $secondsSince = microtime(true) - $lastCheckpointAt;

                if (! $force && $rowsSince < self::CHECKPOINT_ROWS_INTERVAL && $secondsSince < self::CHECKPOINT_SECONDS_INTERVAL) {
                    return;
                }

                $flushRecordBuffer();

                $pos = ftell($handle);
                if (! is_int($pos) || $pos < 0) {
                    return;
                }

                $this->storeCheckpoint($file, $provider, [
                    'pos' => $pos,
                    'total_rows' => $totalRows,
                    'processed' => $processed,
                    'errors' => $errors,
                ]);

                $lastCheckpointAt = microtime(true);
                $lastCheckpointRows = $processed;
            };

            $progressBar->setProgress($read);
            $progressBar->display();  // Force immediate redraw
            $lastOutputTime = microtime(true);

            while (($row = fgetcsv($handle)) !== false) {
                $read++;

                // Update progress bar every 2 seconds
                $now = microtime(true);
                if (($now - $lastOutputTime) >= 2.0) {
                    $progressBar->setProgress($read);
                    $progressBar->display();  // Force immediate redraw
                    $lastOutputTime = $now;
                }

                // Skip malformed rows (but don't continue - let limit check below handle exit)
                if ($row === null || count($row) !== count($headers)) {
                    if (count($recordBuffer) > 0) {
                        $maybeCheckpoint();
                    }
                    // Don't continue - fall through to limit check
                } else {
                    // Valid row - process it
                    $record = array_combine($headers, $row);
                    if ($record !== false) {
                        try {
                            $recordBuffer[] = $record;
                            $processed++;

                            // Flush buffer when full and checkpoint periodically
                            if (count($recordBuffer) >= self::RECORD_BUFFER_SIZE) {
                                $maybeCheckpoint();
                            }
                        } catch (\Throwable $e) {
                            $errors++;
                            if (! $this->fastMode) {
                                Log::error('Failed to buffer game record', [
                                    'record' => $record,
                                    'error' => $e->getMessage(),
                                ]);
                            }
                        }
                    } else {
                        $errors++;
                    }
                }

                // Common limit check for both valid and malformed rows
                if ($limit > 0 && $read >= $limit) {
                    $maybeCheckpoint(true);
                    break;
                }
            }

            $flushRecordBuffer();

            $progressBar->finish();
            $this->newLine();
            $this->info('✓ Finished reading CSV');

            // Indicate final batch processing
            $this->line('⏳ Flushing final batches to database...');
        } finally {
            fclose($handle);
        }

        if ($limit <= 0 && $resumeEnabled) {
            $this->forgetCheckpoint($file, $provider);
        }

        $this->flushBatches();
        $this->info('✓ Database writes completed');

        return $processed;
    }

    private function processGamesJson(string $file, string $provider, int $limit): int
    {
        $raw = File::get($file);
        $decoded = json_decode($raw, true);

        $list = [];
        if (is_array($decoded) && array_is_list($decoded)) {
            $list = $decoded;
        } elseif (is_array($decoded)) {
            foreach (['results', 'games', 'data'] as $key) {
                if (isset($decoded[$key]) && is_array($decoded[$key]) && array_is_list($decoded[$key])) {
                    $list = $decoded[$key];
                    break;
                }
            }
        }

        $total = $limit > 0 ? min($limit, count($list)) : count($list);
        $progressBar = $this->output->createProgressBar($total);
        $this->configureProgressBar($progressBar, true);
        $progressBar->setMessage('0', 'errors');
        $progressBar->setMessage('0', 'skipped');
        $processed = 0;
        $errors = 0;
        $skipped = 0;

        $progressEvery = $this->progressChunk > 0 ? $this->progressChunk : 5000;
        $progressCounter = 0;

        /** @var array<int, array<string, mixed>> $recordBuffer */
        $recordBuffer = [];

        foreach ($list as $row) {
            if (! is_array($row)) {
                $skipped++;
                $progressBar->setMessage((string) $skipped, 'skipped');
                $progressBar->advance();

                continue;
            }

            $recordBuffer[] = $row;
            if (count($recordBuffer) >= self::RECORD_BUFFER_SIZE) {
                try {
                    $errors += $this->processGameRecordsBatch($recordBuffer, $provider);
                } catch (\Throwable $e) {
                    if (! $this->fastMode) {
                        Log::error('Failed to import game batch; falling back to per-record handling', [
                            'error' => $e->getMessage(),
                            'provider' => $provider,
                            'batch_size' => count($recordBuffer),
                        ]);

                        foreach ($recordBuffer as $record) {
                            try {
                                $this->processGameRecord($record, $provider);
                            } catch (\Throwable $inner) {
                                $errors++;
                                Log::error('Failed to import game (fallback path)', [
                                    'record' => $record,
                                    'error' => $inner->getMessage(),
                                ]);
                            }
                        }
                    } else {
                        $errors += count($recordBuffer);
                    }
                } finally {
                    $recordBuffer = [];
                    $this->flushBatches();
                    $progressBar->setMessage((string) $errors, 'errors');
                }
            }

            $processed++;
            $progressCounter++;
            if ($progressCounter >= $progressEvery) {
                $progressBar->advance($progressCounter);
                $progressCounter = 0;
            }

            if ($limit > 0 && $processed >= $limit) {
                break;
            }
        }

        if ($progressCounter > 0) {
            $progressBar->advance($progressCounter);
        }

        if ($recordBuffer !== []) {
            try {
                $errors += $this->processGameRecordsBatch($recordBuffer, $provider);
            } catch (\Throwable $e) {
                Log::error('Failed to import game batch; falling back to per-record handling', [
                    'error' => $e->getMessage(),
                    'provider' => $provider,
                    'batch_size' => count($recordBuffer),
                ]);

                foreach ($recordBuffer as $record) {
                    try {
                        $this->processGameRecord($record, $provider);
                    } catch (\Throwable $inner) {
                        $errors++;
                        Log::error('Failed to import game (fallback path)', [
                            'record' => $record,
                            'error' => $inner->getMessage(),
                        ]);
                    }
                }
            } finally {
                $recordBuffer = [];
                $this->flushBatches();
                $progressBar->setMessage((string) $errors, 'errors');
            }
        }

        if ($progressCounter > 0) {
            $progressBar->advance($progressCounter);
        }

        $progressBar->finish();

        return $processed;
    }

    private function countFileRows(string $file, bool $hasHeader): int
    {
        $size = filesize($file);
        if ($size === false || $size < 10000) {
            // Fall back to line-by-line for small files
            return $this->countFileRowsExact($file, $hasHeader);
        }

        // Estimate based on sampling first 200 lines
        $handle = fopen($file, 'rb');
        if (! $handle) {
            return 0;
        }

        if ($hasHeader) {
            fgets($handle);
        }

        $sampleBytes = 0;
        $sampleLines = 0;
        for ($i = 0; $i < 200; $i++) {
            $line = fgets($handle);
            if ($line === false) {
                break;
            }
            $sampleBytes += strlen($line);
            $sampleLines++;
        }
        fclose($handle);

        if ($sampleLines === 0) {
            return 0;
        }

        $avgLineSize = $sampleBytes / $sampleLines;
        $estimate = (int) ($size / $avgLineSize);

        return $hasHeader ? max(0, $estimate - 1) : $estimate;
    }

    private function countFileRowsExact(string $file, bool $hasHeader): int
    {
        $handle = fopen($file, 'rb');
        if (! $handle) {
            return 0;
        }

        $count = 0;
        if ($hasHeader) {
            fgets($handle);
        }

        while (fgets($handle) !== false) {
            $count++;
        }

        fclose($handle);

        return $count;
    }

    /**
     * Persist a batch of game records using set-based writes.
     *
     * This is the main ingestion hot path. It avoids per-record Eloquent create/find
     * for Products and VideoGameTitles by using bulk insert-or-ignore + ID resolution.
     *
     * @param  array<int, array<string, mixed>>  $records
     * @return int Number of per-record errors encountered.
     */
    private function processGameRecordsBatch(array $records, string $provider): int
    {
        if ($records === []) {
            return 0;
        }

        $batchStart = microtime(true);
        $now = now();
        $errors = 0;

        // `video_game_sources` is provider-level aggregation: one row per provider.
        $source = $this->sourceCache[$provider] ?? null;
        if (! $source) {
            $source = VideoGameSource::query()->firstOrCreate([
                'provider' => $provider,
            ]);

            $this->sourceCache[$provider] = $source;
        }

        $t1 = microtime(true);
        /** @var array<string, array{name:string, normalized_title:string, synopsis:?string, platform:?string, release_date:?string, popularity_score:float, rating:?float, external_ids:array, metadata:array}> $productRowsBySlug */
        $productRowsBySlug = [];

        foreach ($records as $record) {
            $gameId = $record['id'] ?? null;
            if ($gameId === null || $gameId === '') {
                continue;
            }

            $gameName = $record['name'] ?? null;
            $gameName = is_string($gameName) && $gameName !== '' ? $gameName : 'Unknown Game';

            $slug = $record['slug'] ?? null;
            $normalizedTitle = Str::slug($gameName);
            $slug = is_string($slug) && $slug !== '' ? $slug : $normalizedTitle;
            if ($slug === '') {
                $slug = 'game-'.$gameId;
            }

            // Extract detailed fields for product enrichment
            $platforms = $this->extractPlatforms($record);
            $primaryPlatform = $platforms[0] ?? null;
            $releaseDate = $this->parseDate($record['first_release_date'] ?? null);
            $hypes = (int) ($record['hypes'] ?? 0);
            $follows = (int) ($record['follows'] ?? 0);
            $popularityScore = (float) ($hypes + $follows);
            $rating = $this->igdbRatingHelper()->extractPercentage($record);
            $genres = $this->extractGenres($record);
            $companyFields = $this->extractDeveloperAndPublisher($record);
            $summary = $record['summary'] ?? null;
            $storyline = $record['storyline'] ?? null;
            $description = $summary ?? $storyline;

            if (! isset($productRowsBySlug[$slug])) {
                $productRowsBySlug[$slug] = [
                    'name' => $gameName,
                    'normalized_title' => $normalizedTitle,
                    'synopsis' => $description,
                    'platform' => $primaryPlatform,
                    'category' => 'GAME',
                    'release_date' => $releaseDate,
                    'popularity_score' => $popularityScore,
                    'rating' => $rating,
                    'external_ids' => ['igdb' => (int) $gameId],
                    'metadata' => [
                        'genres' => $genres,
                        'developer' => $companyFields['developer'],
                        'publisher' => $companyFields['publisher'],
                    ],
                ];
            } else {
                // Merge logic for existing slug (e.g. accumulate platforms or pick best rating)
                $existing = &$productRowsBySlug[$slug];
                if ($rating > ($existing['rating'] ?? 0)) {
                    $existing['rating'] = $rating;
                }
                if ($popularityScore > $existing['popularity_score']) {
                    $existing['popularity_score'] = $popularityScore;
                }
                // Ensure IGDB ID is tracked if different (though slugs should be unique)
                $existing['external_ids']['igdb'] = (int) $gameId;
            }
        }

        $t2 = microtime(true);

        // Insert products in chunks (SQLite bind limits).
        $productRows = [];
        foreach ($productRowsBySlug as $slug => $row) {
            $productRows[] = [
                'slug' => $slug,
                'name' => $row['name'],
                'title' => $row['name'],
                'normalized_title' => $row['normalized_title'],
                'synopsis' => $row['synopsis'],
                'type' => 'video_game',
                'platform' => $row['platform'],
                'category' => $row['category'],
                'release_date' => $row['release_date'],
                'popularity_score' => $row['popularity_score'],
                'rating' => $row['rating'],
                'external_ids' => json_encode($row['external_ids'], JSON_THROW_ON_ERROR),
                'metadata' => json_encode($row['metadata'], JSON_THROW_ON_ERROR),
                'created_at' => $now,
                'updated_at' => $now,
            ];
        }

        // Use PostgreSQL COPY for 50-100x faster bulk insert
        $this->bulkInsertOptimized('products', $productRows, null, true, ['slug']);

        $t3 = microtime(true);

        $slugs = array_keys($productRowsBySlug);
        /** @var array<string, int> $productIdBySlug */
        $productIdBySlug = Product::query()->whereIn('slug', $slugs)->pluck('id', 'slug')->all();

        $t4 = microtime(true);

        // Insert titles (one per product slug) in chunks.
        $titleRows = [];
        foreach ($productRowsBySlug as $slug => $row) {
            $productId = $productIdBySlug[$slug] ?? null;
            if (! is_int($productId)) {
                continue;
            }

            $titleRows[] = [
                'product_id' => $productId,
                'name' => $row['name'],
                'normalized_title' => $row['normalized_title'],
                'slug' => $slug,
                'providers' => json_encode([$provider], JSON_THROW_ON_ERROR),
                'created_at' => $now,
                'updated_at' => $now,
            ];
        }

        // Use PostgreSQL COPY for 50-100x faster bulk insert
        $this->bulkInsertOptimized('video_game_titles', $titleRows, null, true, ['slug']);

        $t5 = microtime(true);

        /** @var array<string, VideoGameTitle> $titleBySlug */
        $titleBySlug = VideoGameTitle::query()
            ->whereIn('slug', $slugs)
            ->get(['id', 'slug', 'product_id', 'providers'])
            ->keyBy('slug')
            ->all();

        $t6 = microtime(true);

        // Ensure provider presence in the title's providers JSON array - BATCHED.
        $titlesToUpdate = [];
        foreach ($titleBySlug as $slug => $title) {
            $existingProviders = is_array($title->providers) ? $title->providers : [];
            if (! in_array($provider, $existingProviders, true)) {
                $merged = array_values(array_unique(array_merge($existingProviders, [$provider])));
                $titlesToUpdate[$title->id] = $merged;
                $title->providers = $merged;
            }

            $this->titleCache[(string) $title->product_id] = $title;
        }

        // Bulk update providers in single query (skip in fast mode)
        if (! $this->fastMode && $titlesToUpdate !== []) {
            $cases = [];
            $ids = [];
            foreach ($titlesToUpdate as $id => $providers) {
                $jsonProviders = json_encode($providers, JSON_THROW_ON_ERROR);
                $cases[] = "WHEN {$id} THEN '{$jsonProviders}'";
                $ids[] = $id;
            }

            DB::statement(
                'UPDATE video_game_titles SET providers = CASE id '.
                implode(' ', $cases).
                ' END, updated_at = ? WHERE id IN ('.implode(',', $ids).')',
                [$now]
            );
        }

        $t7 = microtime(true);

        // Finally, enqueue mappings + video games per record.
        foreach ($records as $record) {
            try {
                // Clear caches if they grow too large to prevent memory exhaustion
                if (count($this->productCache) > 10000) {
                    $this->productCache = [];
                }
                if (count($this->titleCache) > 10000) {
                    $this->titleCache = [];
                }
                $gameId = $record['id'] ?? null;
                if ($gameId === null || $gameId === '') {
                    $errors++;

                    continue;
                }

                $gameName = $record['name'] ?? null;
                $gameName = is_string($gameName) && $gameName !== '' ? $gameName : 'Unknown Game';

                $slug = $record['slug'] ?? null;
                $slug = is_string($slug) && $slug !== '' ? $slug : Str::slug($gameName);
                if ($slug === '') {
                    $slug = 'game-'.$gameId;
                }

                $title = $titleBySlug[$slug] ?? null;
                if (! $title) {
                    $errors++;

                    continue;
                }

                // Extract once, use multiple times
                $platforms = $this->extractPlatforms($record);
                $companyFields = $this->extractDeveloperAndPublisher($record);
                $rating = $this->igdbRatingHelper()->extractPercentage($record);
                $ratingCount = $this->igdbRatingHelper()->extractRatingCount($record);
                $hypes = (int) ($record['hypes'] ?? 0);
                $follows = (int) ($record['follows'] ?? 0);
                $popularityScore = (float) ($hypes + $follows);
                $releaseDate = $this->parseDate($record['first_release_date'] ?? null);
                $platformsJson = json_encode($platforms, JSON_THROW_ON_ERROR);
                $genres = $this->extractGenres($record); // Get array, not JSON
                $genreJson = json_encode($genres, JSON_THROW_ON_ERROR);
                $summary = $record['summary'] ?? null;
                $storyline = $record['storyline'] ?? null;
                $description = $summary ?? $storyline;

                $this->videoGameTitleSourceBatch[] = [
                    'video_game_title_id' => $title->id,
                    'video_game_source_id' => $source->id,
                    'external_id' => (int) $gameId,
                    'provider_item_id' => (int) $gameId,
                    'raw_payload' => $this->fastMode ? null : json_encode($record, JSON_THROW_ON_ERROR),
                    'provider' => $provider,
                    'slug' => $slug,
                    'name' => $gameName,
                    'description' => $description,
                    'release_date' => $releaseDate,
                    'platform' => $platformsJson,
                    'rating' => $rating,
                    'rating_count' => $ratingCount,
                    'hypes' => $hypes,
                    'follows' => $follows,
                    'developer' => $companyFields['developer'],
                    'publisher' => $companyFields['publisher'],
                    'genre' => $genreJson,
                    'created_at' => $now,
                    'updated_at' => $now,
                ];

                $this->videoGameBatch[] = [
                    'video_game_title_id' => $title->id,
                    'provider' => $provider,
                    'external_id' => (string) $gameId,
                    'slug' => $title->slug,
                    'name' => $gameName,
                    'description' => $description,
                    'summary' => $summary,
                    'storyline' => $storyline,
                    'release_date' => $releaseDate,
                    'platform' => $platformsJson,
                    'rating' => $rating,
                    'rating_count' => $ratingCount,
                    'hypes' => $hypes,
                    'follows' => $follows,
                    'developer' => $companyFields['developer'],
                    'publisher' => $companyFields['publisher'],
                    'genre' => $genreJson,
                    'created_at' => $now,
                    'updated_at' => $now,
                    'attributes' => $this->fastMode
                        ? json_encode(['platform' => $platforms], JSON_THROW_ON_ERROR)
                        : json_encode([
                            'platform' => $platforms,
                            'slug' => $title->slug,
                            'name' => $gameName,
                            'summary' => $summary,
                            'storyline' => $storyline,
                            'release_date' => $releaseDate,
                            'rating' => $rating,
                            'rating_count' => $ratingCount,
                            'hypes' => $hypes,
                            'follows' => $follows,
                            'developer' => $companyFields['developer'],
                            'publisher' => $companyFields['publisher'],
                            'genre' => $genres,
                            'media' => null,
                            'source_payload' => null,
                        ], JSON_THROW_ON_ERROR),
                ];

                // Record graph relationships
                $this->recordGraphRelationships($record, $gameName);
            } catch (\Throwable $e) {
                $errors++;
                if (! $this->fastMode) {
                    Log::error('Failed to import game', [
                        'record' => $record,
                        'error' => $e->getMessage(),
                    ]);
                }
            }
        }

        // CRITICAL: Flush batches immediately after processing the record buffer
        // This ensures video_games and video_game_title_sources are written as soon as products/titles are.
        $this->flushBatches();

        $this->displayProgressTable();

        $t8 = microtime(true);
        $batchEnd = microtime(true);

        // Log timing breakdown every batch for profiling
        $total = $batchEnd - $batchStart;
        $this->info(sprintf(
            'Batch timing (ms): prep=%.1f prod_ins=%.1f prod_pluck=%.1f title_ins=%.1f title_pluck=%.1f prov_upd=%.1f enqueue=%.1f total=%.1f',
            ($t2 - $t1) * 1000,
            ($t3 - $t2) * 1000,
            ($t4 - $t3) * 1000,
            ($t5 - $t4) * 1000,
            ($t6 - $t5) * 1000,
            ($t7 - $t6) * 1000,
            ($t8 - $t7) * 1000,
            $total * 1000
        ));

        return $errors;
    }

    private function checkpointPath(string $file, string $provider): string
    {
        $dir = rtrim(dirname($file), '/').'/'.'.checkpoints';
        File::ensureDirectoryExists($dir);

        $real = realpath($file) ?: $file;
        $key = sha1($provider.'|'.$real);

        return $dir.'/gc-import-igdb-'.$key.'.json';
    }

    /**
     * @return array{pos:int, total_rows?:int, file_size:int, file_mtime:int, processed?:int, skipped?:int, errors?:int}|null
     */
    private function loadCheckpoint(string $file, string $provider): ?array
    {
        $path = $this->checkpointPath($file, $provider);
        if (! File::exists($path)) {
            return null;
        }

        $raw = File::get($path);
        $decoded = json_decode($raw, true);
        if (! is_array($decoded)) {
            return null;
        }

        $pos = $decoded['pos'] ?? null;
        $fileSize = $decoded['file_size'] ?? null;
        $fileMtime = $decoded['file_mtime'] ?? null;

        if (! is_int($pos) || $pos < 0 || ! is_int($fileSize) || $fileSize < 1 || ! is_int($fileMtime) || $fileMtime < 0) {
            return null;
        }

        // Guard against seeking into the wrong file if the dump changed.
        $currentSize = (int) (filesize($file) ?: 0);
        $currentMtime = (int) (filemtime($file) ?: 0);
        if ($currentSize !== $fileSize || $currentMtime !== $fileMtime) {
            return null;
        }

        return [
            'pos' => $pos,
            'total_rows' => $decoded['total_rows'] ?? null,
            'file_size' => $fileSize,
            'file_mtime' => $fileMtime,
            'processed' => is_int($decoded['processed'] ?? null) ? $decoded['processed'] : null,
            'skipped' => is_int($decoded['skipped'] ?? null) ? $decoded['skipped'] : null,
            'errors' => is_int($decoded['errors'] ?? null) ? $decoded['errors'] : null,
        ];
    }

    /**
     * @param  array{pos:int, total_rows?:int, processed?:int, skipped?:int, errors?:int}  $data
     */
    private function storeCheckpoint(string $file, string $provider, array $data): void
    {
        $path = $this->checkpointPath($file, $provider);

        $payload = array_merge($data, [
            'file_size' => (int) (filesize($file) ?: 0),
            'file_mtime' => (int) (filemtime($file) ?: 0),
            'updated_at' => now()->toISOString(),
        ]);

        try {
            File::put($path, json_encode($payload, JSON_THROW_ON_ERROR));
        } catch (\Throwable $e) {
            Log::warning('Failed to store import checkpoint', [
                'file' => $file,
                'provider' => $provider,
                'error' => $e->getMessage(),
            ]);
        }
    }

    private function forgetCheckpoint(string $file, string $provider): void
    {
        $path = $this->checkpointPath($file, $provider);
        if (File::exists($path)) {
            File::delete($path);
        }
    }

    /**
     * @param  resource  $handle
     */
    private function maybeSeekToCheckpoint(mixed $handle, string $file, string $provider): void
    {
        $checkpoint = $this->loadCheckpoint($file, $provider);
        if ($checkpoint === null) {
            return;
        }

        $pos = $checkpoint['pos'];
        $current = ftell($handle);
        if (! is_int($current) || $current < 0) {
            $current = 0;
        }

        // Never seek backwards before the current pointer (e.g., before CSV header).
        if ($pos <= $current) {
            return;
        }

        $seekResult = fseek($handle, $pos);
        if ($seekResult === 0) {
            $this->warn("Resuming import from checkpoint at byte offset {$pos}...");
            $this->newLine();
        }
    }

    /**
     * Fallback path for processing a single game record.
     *
     * This method is INTENTIONALLY per-row (using firstOrCreate) and only executes
     * when batch processing fails. It trades performance for resilience, allowing
     * individual records to be saved even if the batch encounters errors.
     *
     * Do not convert this to batching - it serves as an error recovery mechanism.
     */
    /**
     * Fallback path for processing a single game record.
     *
     * This method is INTENTIONALLY per-row (using firstOrCreate) and only executes
     * when batch processing fails. It trades performance for resilience, allowing
     * individual records to be saved even if the batch encounters errors.
     *
     * Do not convert this to batching - it serves as an error recovery mechanism.
     */
    private function processGameRecord(array $record, string $provider): void
    {
        try {
            $gameId = $record['id'] ?? null;
            $gameName = $record['name'] ?? 'Unknown Game';
            $slug = $record['slug'] ?? Str::slug($gameName);
            if ($slug === '') {
                $slug = $gameId !== null && $gameId !== '' ? 'game-'.$gameId : 'unknown-game';
            }

            // Check cache first before hitting database
            $cacheKey = $slug;

            $product = $this->productCache[$cacheKey] ?? null;

            if (! $product) {
                $product = Product::query()->firstOrCreate(
                    ['slug' => $slug],
                    [
                        'name' => $gameName,
                        'title' => $gameName,
                        'normalized_title' => Str::slug($gameName),
                        'type' => 'video_game',
                        'synopsis' => $record['summary'] ?? $record['storyline'] ?? null,
                    ]
                );
                $this->productCache[$cacheKey] = $product;
            }

            // `video_game_sources` is provider-level aggregation: one row per provider.
            $source = $this->sourceCache[$provider] ?? null;
            if (! $source) {
                $source = VideoGameSource::query()->firstOrCreate([
                    'provider' => $provider,
                ]);

                $this->sourceCache[$provider] = $source;
            }

            // Titles are canonical per Product; providers attach via `video_game_title_sources`.
            $titleKey = (string) $product->id;
            $title = $this->titleCache[$titleKey] ?? null;

            if (! $title) {
                $title = VideoGameTitle::query()->firstOrCreate(
                    [
                        'product_id' => $product->id,
                    ],
                    [
                        'name' => $gameName,
                        'normalized_title' => Str::slug($gameName),
                        'slug' => $product->slug,
                        'providers' => [$provider],
                    ]
                );

                $this->titleCache[$titleKey] = $title;
            }

            $existingProviders = is_array($title->providers) ? $title->providers : [];
            if (! in_array($provider, $existingProviders, true)) {
                $title->forceFill([
                    'providers' => array_values(array_unique(array_merge($existingProviders, [$provider]))),
                ])->save();
            }

            // DIRECT INSERT (No Batching for Fallback)
            $platforms = $this->extractPlatforms($record);
            $companyFields = $this->extractDeveloperAndPublisher($record);
            $rating = $this->igdbRatingHelper()->extractPercentage($record);
            $ratingCount = $this->igdbRatingHelper()->extractRatingCount($record);
            $hypes = (int) ($record['hypes'] ?? 0);
            $follows = (int) ($record['follows'] ?? 0);

            DB::table('video_game_title_sources')->updateOrInsert(
                [
                    'video_game_title_id' => $title->id,
                    'video_game_source_id' => $source->id,
                    'provider_item_id' => (int) $gameId,
                ],
                [
                    'external_id' => (int) $gameId,
                    'raw_payload' => json_encode($record, JSON_THROW_ON_ERROR),
                    'provider' => $provider,
                    'slug' => $slug,
                    'name' => $gameName,
                    'description' => $record['summary'] ?? $record['storyline'] ?? null,
                    'release_date' => $this->parseDate($record['first_release_date'] ?? null),
                    'platform' => json_encode($platforms, JSON_THROW_ON_ERROR),
                    'rating' => $rating,
                    'rating_count' => $ratingCount,
                    'hypes' => $hypes,
                    'follows' => $follows,
                    'developer' => $companyFields['developer'],
                    'publisher' => $companyFields['publisher'],
                    'genre' => $this->extractGenresAsJson($record),
                    'updated_at' => now(),
                    // created_at handled by DB or ignored on update
                ]
            );

            DB::table('video_games')->updateOrInsert(
                [
                    'provider' => $provider,
                    'external_id' => (int) $gameId,
                ],
                [
                    'video_game_title_id' => $title->id,
                    'slug' => $title->slug,
                    'name' => $gameName,
                    'rating' => $rating,
                    'hypes' => $hypes,
                    'follows' => $follows,
                    'release_date' => $this->parseDate($record['first_release_date'] ?? null),
                    'attributes' => json_encode([
                        'platform' => $platforms,
                        'slug' => $title->slug,
                        'name' => $gameName,
                        'summary' => $record['summary'] ?? null,
                        'storyline' => $record['storyline'] ?? null,
                        'release_date' => $this->parseDate($record['first_release_date'] ?? null),
                        'rating' => $rating,
                        'rating_count' => $ratingCount,
                        'hypes' => $hypes,
                        'follows' => $follows,
                        'developer' => $companyFields['developer'],
                        'publisher' => $companyFields['publisher'],
                        'genre' => $this->extractGenresAsJson($record),
                        'media' => null,
                        'source_payload' => null,
                    ], JSON_THROW_ON_ERROR),
                    'updated_at' => now(),
                ]
            );

        } catch (\Throwable $e) {
            Log::error('Failed to import game (fallback)', [
                'record' => $record,
                'error' => $e->getMessage(),
            ]);
        }
    }

    private function flushBatches(): void
    {
        $flushStart = microtime(true);
        try {
            DB::transaction(function () {
                $this->flushVideoGameTitleSourceBatch();
                $this->flushVideoGameBatch();
                $this->flushImageBatch();
                $this->flushVideoBatch();
            });
        } catch (\Throwable $e) {
            Log::error('Flush batches failed', [
                'error' => $e->getMessage(),
                'trace' => $e->getTraceAsString(),
            ]);
            throw $e;
        }
        $flushEnd = microtime(true);

        // Log flush timing occasionally
        static $flushCounter = 0;
        $flushCounter++;
        if ($flushCounter % 20 === 0) {
            $this->info(sprintf('Flush took %.1f ms', ($flushEnd - $flushStart) * 1000));
        }
    }

    private function flushVideoGameTitleSourceBatch(): void
    {
        if ($this->videoGameTitleSourceBatch === []) {
            return;
        }

        foreach ($this->chunkRowsForSafeParams($this->videoGameTitleSourceBatch, self::BATCH_SIZE) as $chunk) {
            // Ensure provider_item_id is treated as integer for the upsert to match the schema (int8)
            $castedChunk = array_map(function ($row) {
                $row['provider_item_id'] = (int) $row['provider_item_id'];

                return $row;
            }, $chunk);

            DB::table('video_game_title_sources')->upsert(
                $castedChunk,
                ['video_game_title_id', 'video_game_source_id', 'provider_item_id'],
                [
                    'raw_payload', 'updated_at', 'provider', 'external_id',
                    'slug', 'name', 'description', 'release_date', 'platform',
                    'rating', 'rating_count', 'hypes', 'follows',
                    'developer', 'publisher', 'genre',
                ]
            );
        }

        // CRITICAL: Clear the batch after successful flush
        $this->videoGameTitleSourceBatch = [];
    }

    private function flushVideoGameBatch(): void
    {
        if (empty($this->videoGameBatch)) {
            return;
        }

        // Deduplicate batch internally
        $deduped = [];
        foreach ($this->videoGameBatch as $row) {
            $key = $row['provider'].'|'.$row['external_id'];
            $deduped[$key] = $row;
        }
        $this->videoGameBatch = array_values($deduped);

        // Use PostgreSQL COPY for 50-100x faster bulk insert
        // Use uniqueBy=['provider', 'external_id'] to handle potential duplicates across batches and update existing rows
        $this->bulkInsertOptimized('video_games', $this->videoGameBatch, null, true, ['provider', 'external_id']);

        // CRITICAL: Clear the batch after successful flush to prevent re-inserting in next flush
        $this->videoGameBatch = [];
    }

    private function flushImageBatch(): void
    {
        if (empty($this->imageBatch)) {
            return;
        }

        $videoGameIds = array_values(array_map('intval', array_keys($this->imageBatch)));

        $existingByGameId = [];
        // ALWAYS load existing rows for images to prevent data loss between dump files (covers -> screenshots -> artworks).
        // Since these different dumps populate the SAME table, skipping the load would cause the later dumps
        // to overwrite/erase the data from the earlier dumps (e.g. screenshots erasing covers).
        $existingRows = DB::table('images')
            ->where('imageable_type', VideoGame::class)
            ->whereIn('imageable_id', $videoGameIds)
            ->get([
                'imageable_id',
                'video_game_id',
                'url',
                'source_url',
                'width',
                'height',
                'is_thumbnail',
                'alt_text',
                'caption',
                'urls',
                'metadata',
                'external_id',
            ]);

        foreach ($existingRows as $row) {
            $existingByGameId[(int) ($row->imageable_id ?? $row->video_game_id)] = $row;
        }

        // Defensive: ensure we never pass duplicate `video_game_id` rows into a single bulk upsert.
        // Some databases can throw a unique constraint violation when the VALUES list contains duplicates,
        // even though we're using ON CONFLICT/UPSERT semantics.
        $upsertByGameId = [];
        foreach ($this->imageBatch as $videoGameId => $batch) {
            $videoGameId = (int) $videoGameId;
            if ($videoGameId <= 0) {
                continue;
            }

            $existing = $existingByGameId[(int) $videoGameId] ?? null;
            // Always merge for images to support multi-file accumulation
            // if (! $this->mergeMedia) { $existing = null; }

            $existingUrls = $existing && is_string($existing->urls) ? json_decode($existing->urls, true) : [];
            if (! is_array($existingUrls)) {
                $existingUrls = [];
            }

            $existingMetadata = $existing && is_string($existing->metadata) ? json_decode($existing->metadata, true) : [];
            if (! is_array($existingMetadata)) {
                $existingMetadata = [];
            }

            $newUrls = $batch['urls'] ?? [];
            if (! is_array($newUrls)) {
                $newUrls = [];
            }

            $mergedUrls = $this->mergeUniqueStrings($existingUrls, $newUrls);

            $newMetadata = [
                'collections' => $batch['metadata']['collections'] ?? [],
                'all_details' => $batch['metadata']['details'] ?? [],
            ];
            $mergedMetadata = $this->mergeImageMetadata($existingMetadata, $newMetadata);

            $newPrimaryDetail = $this->pickImagePrimaryDetail($newMetadata['all_details'] ?? []);

            $existingIsThumbnail = $existing ? (bool) $existing->is_thumbnail : false;
            $newHasThumbnail = in_array(true, (array) ($batch['metadata']['thumbnails'] ?? []), true);

            $sourceUrl = $existingIsThumbnail
            ? $existing->source_url
            : ($newPrimaryDetail['url'] ?? ($mergedUrls[0] ?? null));

            $primaryUrl = $sourceUrl ?? ($mergedUrls[0] ?? null);
            if ($primaryUrl === null) {
                $primaryUrl = sprintf('igdb://video-game/%d/primary-image', $videoGameId);
                $mergedUrls = $this->mergeUniqueStrings($mergedUrls, [$primaryUrl]);
            }

            // Ensure integers are strictly typed or null (fixes "invalid input syntax for type integer: ''")
            $rawWidth = $existingIsThumbnail ? $existing->width : ($newPrimaryDetail['width'] ?? null);
            $width = ($rawWidth === null || $rawWidth === '') ? null : (int) $rawWidth;

            $rawHeight = $existingIsThumbnail ? $existing->height : ($newPrimaryDetail['height'] ?? null);
            $height = ($rawHeight === null || $rawHeight === '') ? null : (int) $rawHeight;

            $isThumbnail = $existingIsThumbnail || $newHasThumbnail;

            $altText = $existingIsThumbnail
            ? $existing->alt_text
            : ($newPrimaryDetail['alt_text'] ?? ($newPrimaryDetail['image_id'] ?? null));

            $caption = $existingIsThumbnail
            ? $existing->caption
            : ($newPrimaryDetail['caption'] ?? null);

            $metadata = $mergedMetadata;

            // Extract Spatie-compatible fields
            $collections = $mergedMetadata['collections'] ?? [];
            $primaryCollection = $collections[0] ?? 'cover_images';
            $externalId = $newPrimaryDetail['image_id'] ?? null;

            $row = [
                'imageable_type' => VideoGame::class,
                'imageable_id' => $videoGameId,
                'video_game_id' => $videoGameId,
                'uuid' => (string) \Illuminate\Support\Str::uuid(),
                'collection_names' => json_encode($collections),
                'primary_collection' => $primaryCollection,
                'url' => $primaryUrl,
                'external_id' => $externalId,
                'provider' => 'igdb',
                'source_url' => $sourceUrl,
                'width' => $width,
                'height' => $height,
                'is_thumbnail' => $isThumbnail,
                'order_column' => 0,
                'alt_text' => $altText,
                'caption' => $caption,
                'urls' => json_encode($mergedUrls),
                'metadata' => json_encode($metadata),
                'created_at' => now(),
                'updated_at' => now(),
            ];

            if (! isset($upsertByGameId[$videoGameId])) {
                $upsertByGameId[$videoGameId] = $row;

                continue;
            }

            if (! $this->mergeMedia) {
                // Skip merging when merge-media=0; keep first row for this game.
                continue;
            }

            // Merge duplicates (prefer thumbnails for primary fields, preserve all urls/details).
            $existingRow = $upsertByGameId[$videoGameId];

            $existingRowUrls = is_string($existingRow['urls'] ?? null) ? json_decode((string) $existingRow['urls'], true) : [];
            if (! is_array($existingRowUrls)) {
                $existingRowUrls = [];
            }
            $rowUrls = is_string($row['urls'] ?? null) ? json_decode((string) $row['urls'], true) : [];
            if (! is_array($rowUrls)) {
                $rowUrls = [];
            }
            $mergedRowUrls = $this->mergeUniqueStrings($existingRowUrls, $rowUrls);

            $existingRowMeta = is_string($existingRow['metadata'] ?? null) ? json_decode((string) $existingRow['metadata'], true) : [];
            if (! is_array($existingRowMeta)) {
                $existingRowMeta = [];
            }
            $rowMeta = is_string($row['metadata'] ?? null) ? json_decode((string) $row['metadata'], true) : [];
            if (! is_array($rowMeta)) {
                $rowMeta = [];
            }
            $mergedRowMeta = $this->mergeImageMetadata($existingRowMeta, $rowMeta);

            $existingRowIsThumbnail = (bool) ($existingRow['is_thumbnail'] ?? false);
            $rowIsThumbnail = (bool) ($row['is_thumbnail'] ?? false);

            $preferRowScalars = $rowIsThumbnail && ! $existingRowIsThumbnail;

            // Merge collection_names
            $existingCollections = is_string($existingRow['collection_names'] ?? null)
            ? json_decode((string) $existingRow['collection_names'], true) : [];
            $rowCollections = is_string($row['collection_names'] ?? null)
            ? json_decode((string) $row['collection_names'], true) : [];
            $mergedCollections = array_values(array_unique(array_merge(
                is_array($existingCollections) ? $existingCollections : [],
                is_array($rowCollections) ? $rowCollections : []
            )));

            $upsertByGameId[$videoGameId] = [
                'imageable_type' => VideoGame::class,
                'imageable_id' => $videoGameId,
                'video_game_id' => $videoGameId,
                'uuid' => $existingRow['uuid'] ?? $row['uuid'],
                'collection_names' => json_encode($mergedCollections),
                'primary_collection' => $existingRow['primary_collection'] ?? $row['primary_collection'],
                'url' => $preferRowScalars ? $row['url'] : ($existingRow['url'] ?? $row['url']),
                'external_id' => $preferRowScalars ? $row['external_id'] : ($existingRow['external_id'] ?? $row['external_id']),
                'provider' => 'igdb',
                'source_url' => $preferRowScalars ? $row['source_url'] : ($existingRow['source_url'] ?? $row['source_url']),
                'width' => $preferRowScalars ? $row['width'] : ($existingRow['width'] ?? $row['width']),
                'height' => $preferRowScalars ? $row['height'] : ($existingRow['height'] ?? $row['height']),
                'is_thumbnail' => $existingRowIsThumbnail || $rowIsThumbnail,
                'order_column' => $existingRow['order_column'] ?? $row['order_column'] ?? 0,
                'alt_text' => $preferRowScalars ? $row['alt_text'] : ($existingRow['alt_text'] ?? $row['alt_text']),
                'caption' => $preferRowScalars ? $row['caption'] : ($existingRow['caption'] ?? $row['caption']),
                'urls' => json_encode($mergedRowUrls),
                'metadata' => json_encode($mergedRowMeta),
                'created_at' => $existingRow['created_at'] ?? $row['created_at'],
                'updated_at' => now(),
            ];
        }

        $upsertData = array_values($upsertByGameId);

        // Bulk upsert - let database handle merging via upsert
        if ($upsertData !== []) {
            foreach ($this->chunkRowsForSafeParams($upsertData, self::MEDIA_BATCH_SIZE) as $chunk) {
                DB::table('images')->upsert(
                    $chunk,
                    ['imageable_type', 'imageable_id', 'url'],
                    [
                        'video_game_id',
                        'uuid',
                        'collection_names',
                        'primary_collection',
                        'external_id',
                        'provider',
                        'source_url',
                        'width',
                        'height',
                        'is_thumbnail',
                        'order_column',
                        'alt_text',
                        'caption',
                        'urls',
                        'metadata',
                        'url',
                        'updated_at',
                    ]
                );
            }
        }

        // CRITICAL: Clear the batch after successful flush
        $this->imageBatch = [];
    }

    private function flushVideoBatch(): void
    {
        if (empty($this->videoBatch)) {
            return;
        }

        $videoGameIds = array_values(array_map('intval', array_keys($this->videoBatch)));

        $existingByGameId = [];
        // Always load existing videos to support additive updates (e.g. if we run import multiple times)
        // if ($this->mergeMedia) {
        $existingRows = DB::table('videos')
            ->whereIn('video_game_id', $videoGameIds)
            ->get(['video_game_id', 'urls', 'provider', 'metadata', 'external_id', 'video_id']);

        foreach ($existingRows as $row) {
            $existingByGameId[(int) $row->video_game_id] = $row;
        }
        // }

        $upsertData = [];
        foreach ($this->videoBatch as $videoGameId => $batch) {
            $existing = $existingByGameId[(int) $videoGameId] ?? null;
            // Always merge for videos
            // if (! $this->mergeMedia) { $existing = null; }

            $existingUrls = $existing && is_string($existing->urls) ? json_decode($existing->urls, true) : [];
            if (! is_array($existingUrls)) {
                $existingUrls = [];
            }

            $existingMetadata = $existing && is_string($existing->metadata) ? json_decode($existing->metadata, true) : [];
            if (! is_array($existingMetadata)) {
                $existingMetadata = [];
            }

            $newUrls = $batch['urls'] ?? [];
            if (! is_array($newUrls)) {
                $newUrls = [];
            }

            $mergedUrls = $this->mergeUniqueStrings($existingUrls, $newUrls);
            $mergedMetadata = $this->mergeVideoMetadata($existingMetadata, (array) ($batch['metadata'] ?? []));
            $provider = $existing && is_string($existing->provider) && $existing->provider !== ''
            ? $existing->provider
            : ($batch['provider'] ?? 'youtube');

            $primaryUrl = $mergedUrls[0] ?? sprintf('igdb://video-game/%d/primary-video', $videoGameId);

            // Extract first video_id as external_id
            // If primaryUrl is a YouTube URL, extract the ID. Otherwise use the value as is.
            $primaryVideoId = $mergedUrls[0] ?? null;
            if ($primaryVideoId && $provider === 'youtube' && str_contains($primaryVideoId, 'youtube.com')) {
                parse_str(parse_url($primaryVideoId, PHP_URL_QUERY), $params);
                $primaryVideoId = $params['v'] ?? $primaryVideoId;
            }

            // Find metadata for the primary video to get the correct external_id (IGDB ID)
            $primaryMeta = null;
            foreach ($mergedMetadata as $meta) {
                if (is_array($meta) && isset($meta['video_id']) && $meta['video_id'] === $primaryVideoId) {
                    $primaryMeta = $meta;
                    break;
                }
            }

            // external_id = IGDB ID (from CSV 'id')
            // video_id = YouTube ID (from CSV 'video_id')
            $externalId = $primaryMeta['id'] ?? $existing->external_id ?? null;
            $videoId = $primaryVideoId ?? $existing->video_id ?? null;

            $upsertData[] = [
                'videoable_type' => \App\Models\VideoGame::class,
                'videoable_id' => $videoGameId,
                'video_game_id' => $videoGameId,
                'uuid' => (string) \Illuminate\Support\Str::uuid(),
                'collection_names' => json_encode(['trailers']),
                'primary_collection' => 'trailers',
                'url' => $primaryUrl,
                'external_id' => $externalId,
                'video_id' => $videoId,
                'urls' => json_encode($mergedUrls),
                'provider' => $provider,
                'order_column' => 0,
                'metadata' => json_encode($mergedMetadata),
                'created_at' => now(),
                'updated_at' => now(),
            ];
        }

        // Bulk upsert - let database handle merging
        if ($upsertData !== []) {
            foreach ($this->chunkRowsForSafeParams($upsertData, self::MEDIA_BATCH_SIZE) as $chunk) {
                DB::table('videos')->upsert(
                    $chunk,
                    ['videoable_type', 'videoable_id', 'url'],
                    [
                        'video_game_id',
                        'uuid',
                        'collection_names',
                        'primary_collection',
                        'external_id',
                        'video_id',
                        'urls',
                        'provider',
                        'order_column',
                        'metadata',
                        'updated_at',
                    ]
                );
            }
        }

        // CRITICAL: Clear the batch after successful flush
        $this->videoBatch = [];
    }

    private function extractPlatforms(array $record): array
    {
        $rawPlatforms = $record['platforms'] ?? null;
        if ($rawPlatforms === null) {
            return ['PC'];
        }

        $cacheKey = is_string($rawPlatforms) ? $rawPlatforms : json_encode($rawPlatforms);
        if (isset($this->platformNormalizationCache[$cacheKey])) {
            return $this->platformNormalizationCache[$cacheKey];
        }

        if (is_array($rawPlatforms)) {
            $names = [];
            foreach ($rawPlatforms as $platform) {
                if (is_array($platform) && isset($platform['name'])) {
                    $names[] = (string) $platform['name'];
                }
            }

            $names = $this->platformNormalizer()->normalizeMany($names);
            $result = $names !== [] ? $names : ['PC'];
            $this->platformNormalizationCache[$cacheKey] = $result;

            return $result;
        }

        if (is_string($rawPlatforms)) {
            $raw = trim($rawPlatforms);

            // IGDB CSV exports may contain platform IDs or a JSON-ish string.
            // Best-effort parsing: JSON array -> list, else split on common delimiters.
            $decoded = json_decode($raw, true);
            if (is_array($decoded)) {
                $values = array_values(array_filter(array_map(function ($v) {
                    if (is_int($v)) {
                        return $this->platformIdToName[$v] ?? (string) $v;
                    }

                    if (is_string($v) && ctype_digit($v)) {
                        $id = (int) $v;

                        return $this->platformIdToName[$id] ?? $v;
                    }

                    return is_scalar($v) ? (string) $v : '';
                }, $decoded), fn ($v) => $v !== ''));

                $values = $this->platformNormalizer()->normalizeMany($values);
                $result = $values !== [] ? $values : ['PC'];
                $this->platformNormalizationCache[$cacheKey] = $result;

                return $result;
            }

            // IGDB dumps commonly represent ID arrays as "{6,48}".
            $ids = $this->parseIgdbIdSetString($raw);
            if ($ids !== []) {
                $names = array_map(fn (int $id) => $this->platformIdToName[$id] ?? (string) $id, $ids);
                $names = $this->platformNormalizer()->normalizeMany($names);
                $result = $names !== [] ? $names : ['PC'];
                $this->platformNormalizationCache[$cacheKey] = $result;

                return $result;
            }

            $parts = preg_split('/[\s,|]+/', $raw) ?: [];
            $parts = array_values(array_filter(array_map('trim', $parts), fn ($v) => $v !== ''));

            $parts = $this->platformNormalizer()->normalizeMany($parts);
            $result = $parts !== [] ? $parts : ['PC'];
            $this->platformNormalizationCache[$cacheKey] = $result;

            return $result;
        }

        return ['PC'];
    }

    private function extractGenres(array $record): array
    {
        $rawGenres = $record['genres'] ?? null;
        if ($rawGenres === null) {
            return [];
        }

        $cacheKey = is_string($rawGenres) ? $rawGenres : json_encode($rawGenres);
        if (isset($this->genreCache[$cacheKey])) {
            return $this->genreCache[$cacheKey];
        }

        $genreIds = [];
        if (is_string($rawGenres)) {
            $decoded = json_decode($rawGenres, true);
            if (is_array($decoded)) {
                $genreIds = array_map('intval', $decoded);
            } else {
                $genreIds = $this->parseIgdbIdSetString($rawGenres);
            }
        } elseif (is_array($rawGenres)) {
            $genreIds = array_map('intval', $rawGenres);
        }

        $genreNames = [];
        foreach ($genreIds as $id) {
            if (isset($this->genreIdToName[$id])) {
                $genreNames[] = $this->genreIdToName[$id];
            }
        }

        $this->genreCache[$cacheKey] = $genreNames;

        return $genreNames;
    }

    private function extractGenresAsJson(array $record): string
    {
        $genres = [];

        // IGDB JSON API can embed genre objects.
        if (isset($record['genres']) && is_array($record['genres'])) {
            foreach ($record['genres'] as $g) {
                if (is_array($g) && isset($g['name']) && is_string($g['name']) && $g['name'] !== '') {
                    $genres[] = $g['name'];
                } elseif (is_string($g) && $g !== '') {
                    $genres[] = $g;
                }
            }
        }

        // IGDB CSV exports typically represent genre IDs as "{5,12}".
        if ($genres === [] && isset($record['genres']) && is_string($record['genres'])) {
            $ids = $this->parseIgdbIdSetString($record['genres']);
            if ($ids !== [] && $this->genreIdToName !== []) {
                $genres = array_values(array_filter(array_map(fn (int $id) => $this->genreIdToName[$id] ?? null, $ids)));
            } else {
                $genres = $ids;
            }
        }

        $genres = array_values(array_unique(array_values(array_filter($genres, fn ($v) => $v !== '' && $v !== null))));

        return json_encode($genres, JSON_THROW_ON_ERROR);
    }

    /**
     * @return array{developer:?string, publisher:?string}
     */
    private function extractDeveloperAndPublisher(array $record): array
    {
        $raw = $record['involved_companies'] ?? null;

        // JSON payloads in this repo's sample already expose these fields.
        $developer = isset($record['developer']) && is_string($record['developer']) && $record['developer'] !== ''
        ? $record['developer']
        : null;
        $publisher = isset($record['publisher']) && is_string($record['publisher']) && $record['publisher'] !== ''
        ? $record['publisher']
        : null;

        if (($developer !== null || $publisher !== null) && ($raw === null || $raw === '')) {
            return [
                'developer' => $developer,
                'publisher' => $publisher,
            ];
        }

        if (! is_string($raw) || $raw === '') {
            return [
                'developer' => $developer,
                'publisher' => $publisher,
            ];
        }

        if (isset($this->involvedCompanyCache[$raw])) {
            $cached = $this->involvedCompanyCache[$raw];

            return [
                'developer' => $developer ?? $cached['developer'],
                'publisher' => $publisher ?? $cached['publisher'],
            ];
        }

        if ($this->involvedCompanyIdToCompanyRole === [] || $this->companyIdToName === []) {
            return [
                'developer' => $developer,
                'publisher' => $publisher,
            ];
        }

        $ids = $this->parseIgdbIdSetString($raw);
        if ($ids === []) {
            return [
                'developer' => $developer,
                'publisher' => $publisher,
            ];
        }

        $developerNames = [];
        $publisherNames = [];

        foreach ($ids as $involvedCompanyId) {
            $row = $this->involvedCompanyIdToCompanyRole[$involvedCompanyId] ?? null;
            if ($row === null) {
                continue;
            }

            $companyName = $this->companyIdToName[$row['company_id']] ?? null;
            if (! is_string($companyName) || $companyName === '') {
                continue;
            }

            if ($row['developer'] === true) {
                $developerNames[] = $companyName;
            }
            if ($row['publisher'] === true) {
                $publisherNames[] = $companyName;
            }
        }

        $developerNames = array_values(array_unique($developerNames));
        $publisherNames = array_values(array_unique($publisherNames));

        $result = [
            'developer' => $developerNames !== [] ? implode(', ', $developerNames) : null,
            'publisher' => $publisherNames !== [] ? implode(', ', $publisherNames) : null,
        ];

        $this->involvedCompanyCache[$raw] = $result;

        return [
            'developer' => $developer ?? $result['developer'],
            'publisher' => $publisher ?? $result['publisher'],
        ];
    }

    private function parseDate(?string $date): ?string
    {
        if (! $date) {
            return null;
        }

        if (is_numeric($date)) {
            return date('Y-m-d', (int) $date);
        }

        // Handle DD/MM/YYYY HH:II format commonly found in IGDB dumps
        if (preg_match('/^\d{2}\/\d{2}\/\d{4} \d{2}:\d{2}$/', $date)) {
            try {
                return \Illuminate\Support\Carbon::createFromFormat('d/m/Y H:i', $date)->format('Y-m-d H:i:s');
            } catch (\Exception $e) {
                return null;
            }
        }

        return $date;
    }

    private function processMediaIfPresent(string $path, string $basename, callable $attach, string $provider): int
    {
        $file = $this->findFile($path, $basename);
        if (! $file) {
            $this->line("  ⚠️  {$basename}: not found, skipping");

            return 0;
        }

        $this->info("  📥 {$basename}...");

        return $this->processMediaCsvStreaming($file, $provider, $attach);
    }

    private function processAuxiliaryIfPresent(string $path, string $basename, string $table, callable $map, string $provider): int
    {
        $file = $this->findFile($path, $basename);
        if (! $file) {
            $this->line("  ⚠️  {$basename}: not found, skipping");

            return 0;
        }

        $this->info("  📥 {$basename}...");

        $handle = fopen($file, 'r');
        if (! $handle) {
            return 0;
        }

        $headers = fgetcsv($handle);
        if (! $headers) {
            fclose($handle);

            return 0;
        }

        $gameIdMap = $this->preloadGameIdMappings($provider);
        if (empty($gameIdMap)) {
            fclose($handle);

            return 0;
        }

        $batch = [];
        $processed = 0;
        $totalRows = $this->countFileRows($file, true);
        $progressBar = $this->output->createProgressBar($totalRows);
        $this->configureProgressBar($progressBar, true);

        while (($row = fgetcsv($handle)) !== false) {
            $record = $this->combineCsvRow($headers, $row);
            if (! $record) {
                continue;
            }

            $gameId = (int) ($record['game'] ?? 0);
            if ($gameId === 0 || ! isset($gameIdMap[$gameId])) {
                continue;
            }

            // Map IGDB ID to local ID
            $record['game'] = $gameIdMap[$gameId];

            $mapped = $map($record);
            if ($mapped) {
                $batch[] = $mapped;
                $processed++;
            }

            if (count($batch) >= self::BATCH_SIZE) {
                $this->bulkInsertOptimized($table, $batch);
                $batch = [];
                $progressBar->advance(self::BATCH_SIZE);
            }
        }

        if ($batch !== []) {
            $this->bulkInsertOptimized($table, $batch);
            $progressBar->advance(count($batch));
        }

        $progressBar->finish();
        $this->newLine();
        fclose($handle);

        return $processed;
    }

    private function addImageMedia(object $videoGame, array $data, string $collection, bool $isThumbnail): void
    {
        try {
            $url = $data['url'] ?? null;
            $imageId = $data['image_id'] ?? null;

            if (! $url && is_string($imageId) && $imageId !== '') {
                $url = "https://images.igdb.com/igdb/image/upload/t_1080p/{$imageId}.jpg";
            }

            if (! $url) {
                return;
            }

            if (str_starts_with($url, '//')) {
                $url = 'https:'.$url;
            }

            // Generate all size variants for IGDB images
            $allUrls = [$url]; // Start with original URL

            if (str_contains($url, 'images.igdb.com')) {
                // Extract base URL pattern
                $baseUrl = preg_replace('/\/t_[a-z_]+\//', '/', $url);

                if (! is_string($imageId) || $imageId === '') {
                    $imageId = $this->extractIgdbImageIdFromUrl($baseUrl);
                }

                if (is_string($imageId) && $imageId !== '') {
                    // Generate all available IGDB sizes
                    $sizes = match ($collection) {
                        'cover_images' => [
                            't_cover_small',      // 90×128
                            't_cover_big',        // 264×374
                            't_720p',             // 1280×720
                            't_1080p',            // 1920×1080 (primary)
                        ],
                        'screenshots' => [
                            't_thumb',            // 90×90
                            't_screenshot_med',   // 569×320
                            't_screenshot_big',   // 889×500
                            't_screenshot_huge',  // 1280×720 (primary)
                            't_1080p',            // 1920×1080
                        ],
                        'artworks' => [
                            't_thumb',            // 90×90
                            't_720p',             // 1280×720
                            't_1080p',            // 1920×1080 (primary)
                        ],
                        default => [
                            't_thumb',            // 90×90
                            't_720p',             // 1280×720 (primary)
                        ],
                    };

                    // Build URL for each size
                    $allUrls = array_map(
                        fn ($size) => "https://images.igdb.com/igdb/image/upload/{$size}/{$imageId}.jpg",
                        $sizes
                    );
                }
            }

            // Aggregate images in-memory per video_game_id
            $gameId = $videoGame->id;

            // Skip if invalid game ID (must be a positive integer)
            if (! $gameId || ! is_numeric($gameId) || $gameId <= 0) {
                return;
            }

            if (! isset($this->imageBatch[$gameId])) {
                $this->imageBatch[$gameId] = [
                    'urls' => [],
                    'metadata' => [
                        'collections' => [],
                        'thumbnails' => [],
                        'details' => [],
                    ],
                ];
            }

            // Add all size variants (avoid duplicates)
            foreach ($allUrls as $variantUrl) {
                if (! in_array($variantUrl, $this->imageBatch[$gameId]['urls'], true)) {
                    $this->imageBatch[$gameId]['urls'][] = $variantUrl;
                }
            }

            // Aggregate metadata
            if (! in_array($collection, $this->imageBatch[$gameId]['metadata']['collections'], true)) {
                $this->imageBatch[$gameId]['metadata']['collections'][] = $collection;
            }
            $this->imageBatch[$gameId]['metadata']['thumbnails'][] = $isThumbnail;

            // Store detail with all size variants
            $detailWithSizes = array_merge($data, ['size_variants' => $allUrls, 'collection' => $collection, 'is_thumbnail' => $isThumbnail]);
            $this->imageBatch[$gameId]['metadata']['details'][] = $detailWithSizes;

            // Flush when batch reaches size limit
            if (count($this->imageBatch) >= self::MEDIA_BATCH_SIZE) {
                $this->flushImageBatch();
            }
        } catch (\Exception) {
            // Skip silently
        }
    }

    private function addVideoMedia(object $videoGame, array $data): void
    {
        try {
            $videoId = $data['video_id'] ?? null;
            if (! $videoId) {
                return;
            }

            // Skip devlogs as requested
            $name = $data['name'] ?? '';
            if (is_string($name) && stripos($name, 'devlog') !== false) {
                return;
            }

            $provider = $data['provider'] ?? 'youtube';

            // Aggregate videos in-memory per video_game_id
            $gameId = $videoGame->id;

            // Skip if invalid game ID (must be a positive integer)
            if (! $gameId || ! is_numeric($gameId) || $gameId <= 0) {
                return;
            }

            if (! isset($this->videoBatch[$gameId])) {
                $this->videoBatch[$gameId] = [
                    'urls' => [],
                    'provider' => $provider,
                    'metadata' => [],
                ];
            }

            // Avoid duplicate video IDs
            // Store full URL if provider is youtube
            $videoUrl = $provider === 'youtube' ? "https://www.youtube.com/watch?v={$videoId}" : $videoId;

            if (! in_array($videoUrl, $this->videoBatch[$gameId]['urls'], true)) {
                $this->videoBatch[$gameId]['urls'][] = $videoUrl;
            }

            $this->videoBatch[$gameId]['metadata'][] = $data;

            // Flush when batch reaches size limit
            if (count($this->videoBatch) >= self::MEDIA_BATCH_SIZE) {
                $this->flushVideoBatch();
            }
        } catch (\Exception) {
            // Skip silently
        }
    }

    /**
     * @param  array<int, string>  $existing
     * @param  array<int, string>  $incoming
     * @return array<int, string>
     */
    private function mergeUniqueStrings(array $existing, array $incoming): array
    {
        $merged = [];
        foreach (array_merge($existing, $incoming) as $value) {
            if (! is_string($value) || $value === '') {
                continue;
            }
            if (! in_array($value, $merged, true)) {
                $merged[] = $value;
            }
        }

        return $merged;
    }

    /**
     * @param  array<string, mixed>  $existing
     * @param  array<string, mixed>  $incoming
     * @return array<string, mixed>
     */
    private function mergeImageMetadata(array $existing, array $incoming): array
    {
        $existingCollections = isset($existing['collections']) && is_array($existing['collections']) ? $existing['collections'] : [];
        $incomingCollections = isset($incoming['collections']) && is_array($incoming['collections']) ? $incoming['collections'] : [];

        $collections = array_values(array_unique(array_values(array_filter(array_merge($existingCollections, $incomingCollections), fn ($v) => is_string($v) && $v !== ''))));

        $existingDetails = isset($existing['all_details']) && is_array($existing['all_details']) ? $existing['all_details'] : [];
        $incomingDetails = isset($incoming['all_details']) && is_array($incoming['all_details']) ? $incoming['all_details'] : [];

        $details = $this->dedupeMediaDetails(array_merge($existingDetails, $incomingDetails));

        return [
            'collections' => $collections,
            'all_details' => $details,
        ];
    }

    /**
     * @param  array<int, mixed>  $details
     * @return array<int, mixed>
     */
    private function dedupeMediaDetails(array $details): array
    {
        $seen = [];
        $result = [];

        foreach ($details as $detail) {
            if (! is_array($detail)) {
                continue;
            }

            $key = null;
            foreach (['id', 'image_id', 'video_id', 'url', 'checksum'] as $candidate) {
                if (isset($detail[$candidate]) && is_scalar($detail[$candidate]) && (string) $detail[$candidate] !== '') {
                    $key = $candidate.':'.(string) $detail[$candidate];
                    break;
                }
            }

            if ($key === null) {
                $key = 'hash:'.md5(json_encode($detail));
            }

            if (isset($seen[$key])) {
                continue;
            }

            $seen[$key] = true;
            $result[] = $detail;
        }

        return $result;
    }

    /**
     * @param  array<int, mixed>  $details
     * @return array<string, mixed>
     */
    private function pickImagePrimaryDetail(array $details): array
    {
        $first = null;
        $best = null;

        foreach ($details as $detail) {
            if (! is_array($detail)) {
                continue;
            }

            $first ??= $detail;

            if (($detail['is_thumbnail'] ?? false) === true) {
                $best = $detail;
                break;
            }

            if (($detail['collection'] ?? null) === 'cover_images') {
                $best ??= $detail;
            }
        }

        return $best ?? $first ?? [];
    }

    /**
     * @param  array<string, mixed>  $existing
     * @param  array<int, mixed>  $incoming
     * @return array<int, mixed>
     */
    private function mergeVideoMetadata(array $existing, array $incoming): array
    {
        $existingItems = is_array($existing) ? $existing : [];
        $items = array_merge($existingItems, $incoming);

        return $this->dedupeMediaDetails($items);
    }

    private function extractIgdbImageIdFromUrl(string $url): ?string
    {
        if (preg_match('~/(?:t_[a-z_]+/)?([a-zA-Z0-9_]+)\.(?:jpg|png|gif|webp)~', $url, $m) === 1) {
            return $m[1];
        }

        return null;
    }

    private function processMediaCsvStreaming(string $file, string $provider, callable $attach): int
    {
        $handle = fopen($file, 'r');
        if (! $handle) {
            $this->warn("Could not open media file: {$file}");

            return 0;
        }

        $headers = fgetcsv($handle);
        if (! $headers) {
            fclose($handle);

            return 0;
        }

        // PRE-LOAD all game ID mappings into memory to avoid N+1 queries
        $this->info('Preloading game ID mappings...');
        $gameIdMap = $this->preloadGameIdMappings($provider);
        $this->info('✓ Loaded '.count($gameIdMap).' game mappings');
        if (empty($gameIdMap)) {
            $this->warn("No game ID mappings found - skipping media import for {$file}");
            fclose($handle);

            return 0;
        }

        // Check for resume support
        // IMPORTANT: Option values are strings. Casting to bool is wrong because (bool) '0' === true.
        // We must cast to int to correctly treat --resume=0 as disabled.
        $resumeEnabled = (int) $this->option('resume') !== 0;
        $resetCheckpoint = (bool) $this->option('reset-checkpoint');

        if ($resetCheckpoint) {
            $this->forgetCheckpoint($file, $provider);
        }

        $checkpoint = $resumeEnabled && ! $resetCheckpoint ? $this->loadCheckpoint($file, $provider) : null;

        if ($checkpoint) {
            $this->maybeSeekToCheckpoint($handle, $file, $provider);
        }

        $totalRows = $this->countFileRows($file, true);
        $progressBar = $this->output->createProgressBar($totalRows);
        $this->configureProgressBar($progressBar, true);

        $processed = $checkpoint['processed'] ?? 0;
        $errors = $checkpoint['errors'] ?? 0;
        $skipped = $checkpoint['skipped'] ?? 0;

        $progressBar->setMessage((string) $errors, 'errors');
        $progressBar->setMessage((string) $skipped, 'skipped');
        $progressBar->setProgress($processed + $skipped);
        $progressBar->display();

        $progressEvery = $this->progressChunk > 0 ? $this->progressChunk : 1000;
        $progressCounter = 0;
        $lastProgressAt = microtime(true);
        $advanceProgress = function () use ($progressBar, &$progressCounter, $progressEvery, &$lastProgressAt): void {
            $progressCounter++;
            $now = microtime(true);
            if ($progressCounter >= $progressEvery || ($now - $lastProgressAt) >= 2.0) {
                $progressBar->advance($progressCounter);
                $progressCounter = 0;
                $lastProgressAt = $now;
            }
        };

        $lastCheckpointAt = microtime(true);
        $lastCheckpointRows = 0;

        $maybeCheckpoint = function (bool $force = false) use ($handle, $file, $provider, $resumeEnabled, &$lastCheckpointAt, &$lastCheckpointRows, &$processed, &$skipped, &$errors): void {
            if (! $resumeEnabled) {
                return;
            }

            $rowsSince = ($processed + $skipped) - $lastCheckpointRows;
            $secondsSince = microtime(true) - $lastCheckpointAt;

            if (! $force && $rowsSince < self::CHECKPOINT_ROWS_INTERVAL && $secondsSince < self::CHECKPOINT_SECONDS_INTERVAL) {
                return;
            }

            $pos = ftell($handle);
            if (! is_int($pos) || $pos < 0) {
                return;
            }

            $this->storeCheckpoint($file, $provider, [
                'pos' => $pos,
                'processed' => $processed,
                'skipped' => $skipped,
                'errors' => $errors,
            ]);

            $lastCheckpointAt = microtime(true);
            $lastCheckpointRows = $processed + $skipped;
        };

        while (($row = fgetcsv($handle)) !== false) {
            $record = $this->combineCsvRow($headers, $row);
            if ($record === null) {
                $skipped++;
                $progressBar->setMessage((string) $skipped, 'skipped');
                $advanceProgress();
                $maybeCheckpoint();

                continue;
            }

            // Fix: Prioritize 'game' or 'game_id' column.
            // NEVER use 'id' as a fallback for game_id, because 'id' is the media item's ID.
            $gameId = (int) ($record['game_id'] ?? $record['game'] ?? 0);

            if ($gameId === 0) {
                $skipped++;

                continue;
            }

            // Check if we have this game in our mapping (VideoGame exists)
            if (! isset($gameIdMap[$gameId])) {
                // Game not imported, skip media
                $skipped++;
                $progressBar->setMessage((string) $skipped, 'skipped');
                $advanceProgress();
                $maybeCheckpoint();

                continue;
            }

            try {
                // We need a VideoGame object/stub to pass to attach.
                // We can just pass an object with ID.
                $gameStub = (object) ['id' => $gameIdMap[$gameId]];

                $attach($gameStub, $record);
                $processed++;
            } catch (\Throwable $e) {
                $errors++;
                $progressBar->setMessage((string) $errors, 'errors');
            }

            $advanceProgress();
            $maybeCheckpoint();
        }

        if ($progressCounter > 0) {
            $progressBar->advance($progressCounter);
        }

        $progressBar->finish();

        // CRITICAL: Flush remaining batches before finishing
        $this->flushImageBatch();
        $this->flushVideoBatch();

        fclose($handle);

        if ($resumeEnabled) {
            $this->forgetCheckpoint($file, $provider);
        }

        return $processed;
    }

    /**
     * @return array<int, int> Map of provider_item_id -> video_game_id
     */
    private function preloadGameIdMappings(string $provider): array
    {
        // We need to map IGDB IDs (external_id) to our local VideoGame IDs.
        // video_games table has (provider, external_id). external_id IS the IGDB ID.
        // So we want: external_id -> id.

        // CRITICAL: external_id is stored as a string in the DB but contains the IGDB integer ID.
        // We must ensure the mapping handles this correctly.
        return DB::table('video_games')
            ->where('provider', $provider)
            ->pluck('id', 'external_id')
            ->mapWithKeys(fn ($id, $extId) => [(int) $extId => (int) $id])
            ->all();
    }

    private function combineCsvRow(array $headers, array $row): ?array
    {
        if (count($headers) !== count($row)) {
            return null;
        }

        return array_combine($headers, $row);
    }

    private function configureProgressBar(ProgressBar $bar, bool $redraw = false, bool $byteProgress = false): void
    {
        if ($byteProgress) {
            $bar->setFormat(' %current%/%max% bytes [%bar%] %percent:3s%%');
            $bar->setRedrawFrequency(1);

            return;
        }

        $format = $redraw ? ' %current%/%max% [%bar%] %percent:3s%%' : 'normal';
        $bar->setFormat($format);
        $bar->setRedrawFrequency(1);
    }

    /**
     * @return array<int>
     */
    private function parseIgdbIdSetString(string $raw): array
    {
        // Format: "{1,2,3}" or "1,2,3"
        $trimmed = trim($raw, '{}');
        if ($trimmed === '') {
            return [];
        }

        return array_map('intval', explode(',', $trimmed));
    }

    private function platformNormalizer(): PlatformNormalizer
    {
        return $this->platformNormalizer ??= new PlatformNormalizer;
    }

    private function igdbRatingHelper(): IgdbRatingHelper
    {
        return $this->igdbRatingHelper ??= new IgdbRatingHelper;
    }

    private function loadPlatformFamilyIdToNameMap(string $directory): void
    {
        $file = $this->findFile($directory, 'platform_families');
        if (! $file) {
            $this->warn('⚠ No platform families file found');

            return;
        }

        $this->info('Loading platform families from '.basename($file).'...');

        $handle = fopen($file, 'r');
        if (! $handle) {
            $this->warn('⚠ Could not open platform families file');

            return;
        }

        $headers = fgetcsv($handle);
        $batch = [];
        while (($row = fgetcsv($handle)) !== false) {
            $data = $this->combineCsvRow($headers, $row);
            if ($data && isset($data['id'], $data['name'])) {
                $id = (int) $data['id'];
                $name = (string) $data['name'];
                $slug = $data['slug'] ?? Str::slug($name);

                $this->platformFamilyIdToName[$id] = $name;

                $batch[] = [
                    'id' => $id,
                    'name' => $name,
                    'slug' => $slug,
                    'created_at' => now(),
                    'updated_at' => now(),
                ];
            }
        }
        fclose($handle);

        if ($batch !== []) {
            $this->bulkInsertOptimized('video_game_platform_families', $batch);
        }

        $this->info('✓ Loaded '.count($this->platformFamilyIdToName).' platform families');
    }

    private function loadPlatformLogoIdToUrlMap(string $directory): void
    {
        $file = $this->findFile($directory, 'platform_logos');
        if (! $file) {
            $this->warn('⚠ No platform logos file found');

            return;
        }

        $this->info('Loading platform logos from '.basename($file).'...');

        $handle = fopen($file, 'r');
        if (! $handle) {
            $this->warn('⚠ Could not open platform logos file');

            return;
        }

        $headers = fgetcsv($handle);
        while (($row = fgetcsv($handle)) !== false) {
            $data = $this->combineCsvRow($headers, $row);
            if ($data && isset($data['id'], $data['url'])) {
                $url = (string) $data['url'];
                if (str_starts_with($url, '//')) {
                    $url = 'https:'.$url;
                }
                // Use high-res variant if possible
                $url = str_replace('/t_thumb/', '/t_original/', $url);
                $this->platformLogoIdToUrl[(int) $data['id']] = $url;
            }
        }
        fclose($handle);
        $this->info('✓ Loaded '.count($this->platformLogoIdToUrl).' platform logo mappings');
    }

    private function loadPlatformIdToNameMap(string $directory): void
    {
        $file = $this->findFile($directory, 'platforms');
        if (! $file) {
            $this->warn('⚠ No platforms file found');

            return;
        }

        $this->info('Loading platforms from '.basename($file).'...');

        $handle = fopen($file, 'r');
        if (! $handle) {
            $this->warn('⚠ Could not open platforms file');

            return;
        }

        $headers = fgetcsv($handle);
        $batch = [];
        $productBatch = [];

        // Ensure directory for logos exists
        $logoDir = public_path('storage/platform-logos');
        if (! File::exists($logoDir)) {
            File::makeDirectory($logoDir, 0755, true);
        }

        while (($row = fgetcsv($handle)) !== false) {
            $data = $this->combineCsvRow($headers, $row);
            if ($data && isset($data['id'], $data['name'])) {
                $id = (int) $data['id'];
                $name = (string) $data['name'];
                $slug = $data['slug'] ?? Str::slug($name);
                $familyId = isset($data['platform_family']) && is_numeric($data['platform_family'])
                    ? (int) $data['platform_family']
                    : null;

                $logoPath = null;
                $logoId = isset($data['platform_logo']) && is_numeric($data['platform_logo'])
                    ? (int) $data['platform_logo']
                    : null;

                if ($logoId && isset($this->platformLogoIdToUrl[$logoId])) {
                    $logoUrl = $this->platformLogoIdToUrl[$logoId];
                    $ext = pathinfo($logoUrl, PATHINFO_EXTENSION) ?: 'jpg';
                    $filename = "{$slug}.{$ext}";
                    $targetPath = "{$logoDir}/{$filename}";

                    // Download if not exists
                    if (! File::exists($targetPath)) {
                        try {
                            $content = @file_get_contents($logoUrl);
                            if ($content) {
                                File::put($targetPath, $content);
                                $logoPath = "platform-logos/{$filename}";
                            }
                        } catch (\Throwable $e) {
                            Log::warning("Failed to download platform logo for {$slug}: ".$e->getMessage());
                        }
                    } else {
                        $logoPath = "platform-logos/{$filename}";
                    }
                }

                $this->platformIdToName[$id] = $name;

                $batch[] = [
                    'id' => $id,
                    'platform_family_id' => $familyId,
                    'name' => $name,
                    'slug' => $slug,
                    'abbreviation' => $data['abbreviation'] ?? null,
                    'summary' => $data['summary'] ?? null,
                    'logo_path' => $logoPath,
                    'created_at' => now(),
                    'updated_at' => now(),
                ];

                $productBatch[] = [
                    'name' => $name,
                    'title' => $name,
                    'slug' => $slug,
                    'normalized_title' => Str::slug($name),
                    'type' => 'console',
                    'category' => 'PLATFORM',
                    'synopsis' => $data['summary'] ?? null,
                    'external_ids' => json_encode(['igdb' => $id], JSON_THROW_ON_ERROR),
                    'metadata' => json_encode([
                        'abbreviation' => $data['abbreviation'] ?? null,
                        'logo_path' => $logoPath,
                    ], JSON_THROW_ON_ERROR),
                    'created_at' => now(),
                    'updated_at' => now(),
                ];
            }
        }
        fclose($handle);

        if ($batch !== []) {
            $this->bulkInsertOptimized('video_game_platforms', $batch);
        }

        if ($productBatch !== []) {
            $this->bulkInsertOptimized('products', $productBatch, null, true, ['slug']);
        }

        $this->info('✓ Loaded '.count($this->platformIdToName).' platforms');
    }

    private function loadGenreIdToNameMap(string $directory): void
    {
        $file = $this->findFile($directory, 'genres');
        if (! $file) {
            $this->warn('⚠ No genres file found');

            return;
        }

        $this->info('Loading genres from '.basename($file).'...');

        $handle = fopen($file, 'r');
        if (! $handle) {
            $this->warn('⚠ Could not open genres file');

            return;
        }

        $headers = fgetcsv($handle);
        while (($row = fgetcsv($handle)) !== false) {
            $data = $this->combineCsvRow($headers, $row);
            if ($data && isset($data['id'], $data['name'])) {
                $this->genreIdToName[(int) $data['id']] = $data['name'];
            }
        }
        fclose($handle);
        $this->info('✓ Loaded '.count($this->genreIdToName).' genres');
    }

    private function recordGraphRelationships(array $record, string $gameName): void
    {
        $gameId = (string) ($record['id'] ?? '');
        if ($gameId === '') {
            return;
        }

        // 1. Similar Games
        $similarIds = $this->parseIgdbIdSetString($record['similar_games'] ?? '');
        foreach ($similarIds as $sid) {
            $this->graphService->recordRelationship('GAME', $gameId, 'GAME', (string) $sid, 'SIMILAR_TO', $gameName);
        }

        // 2. Remakes / Remasters
        $remakeIds = $this->parseIgdbIdSetString($record['remakes'] ?? '');
        foreach ($remakeIds as $rid) {
            $this->graphService->recordRelationship('GAME', (string) $rid, 'GAME', $gameId, 'REMAKE_OF', null, $gameName);
        }

        $remasterIds = $this->parseIgdbIdSetString($record['remasters'] ?? '');
        foreach ($remasterIds as $rid) {
            $this->graphService->recordRelationship('GAME', (string) $rid, 'GAME', $gameId, 'REMASTER_OF', null, $gameName);
        }

        // 3. Parent Game / Version Parent
        $parentId = $record['parent_game'] ?? null;
        if ($parentId) {
            $this->graphService->recordRelationship('GAME', $gameId, 'GAME', (string) $parentId, 'VERSION_OF', $gameName);
        }

        // 4. Franchise
        $franchiseId = $record['franchise'] ?? null;
        if ($franchiseId) {
            $this->graphService->recordRelationship('GAME', $gameId, 'FRANCHISE', (string) $franchiseId, 'PART_OF_FRANCHISE', $gameName);
        }

        // 5. Companies (Developer/Publisher)
        $involvedIds = $this->parseIgdbIdSetString($record['involved_companies'] ?? '');
        foreach ($involvedIds as $iid) {
            $role = $this->involvedCompanyIdToCompanyRole[$iid] ?? null;
            if ($role) {
                $companyId = (string) $role['company_id'];
                $companyName = $this->companyIdToName[$role['company_id']] ?? null;

                if ($role['developer']) {
                    $this->graphService->recordRelationship('GAME', $gameId, 'COMPANY', $companyId, 'DEVELOPED_BY', $gameName, $companyName);
                }
                if ($role['publisher']) {
                    $this->graphService->recordRelationship('GAME', $gameId, 'COMPANY', $companyId, 'PUBLISHED_BY', $gameName, $companyName);
                }
            }
        }
    }

    private function displayProgressTable(): void
    {
        $tables = [
            'products',
            'video_game_titles',
            'video_games',
            'video_game_title_sources',
        ];

        $rows = [];
        foreach ($tables as $table) {
            try {
                $rows[] = [
                    'Table' => $table,
                    'Count' => number_format(DB::table($table)->count()),
                ];
            } catch (\Throwable $e) {
                $rows[] = [
                    'Table' => $table,
                    'Count' => 'Error',
                ];
            }
        }

        $this->newLine();
        $this->info('📊 Current Import Progress:');
        $this->table(['Table', 'Count'], $rows);
        $this->newLine();
    }

    private function loadCompanyAndInvolvedCompanyMaps(string $directory): void
    {
        // Companies
        $file = $this->findFile($directory, 'companies');
        if ($file) {
            $this->info('Loading companies from '.basename($file).'...');
            $handle = fopen($file, 'r');
            if ($handle) {
                $headers = fgetcsv($handle);
                while (($row = fgetcsv($handle)) !== false) {
                    $data = $this->combineCsvRow($headers, $row);
                    if ($data && isset($data['id'], $data['name'])) {
                        $this->companyIdToName[(int) $data['id']] = $data['name'];
                    }
                }
                fclose($handle);
                $this->info('✓ Loaded '.count($this->companyIdToName).' companies');
            } else {
                $this->warn('⚠ Could not open companies file');
            }
        } else {
            $this->warn('⚠ No companies file found');
        }

        // Involved Companies
        $file = $this->findFile($directory, 'involved_companies');
        if ($file) {
            $this->info('Loading involved companies from '.basename($file).'...');
            $handle = fopen($file, 'r');
            if ($handle) {
                $headers = fgetcsv($handle);
                while (($row = fgetcsv($handle)) !== false) {
                    $data = $this->combineCsvRow($headers, $row);
                    if ($data && isset($data['id'], $data['company'])) {
                        $this->involvedCompanyIdToCompanyRole[(int) $data['id']] = [
                            'company_id' => (int) $data['company'],
                            'developer' => ($data['developer'] ?? 'false') === 'true',
                            'publisher' => ($data['publisher'] ?? 'false') === 'true',
                        ];
                    }
                }
                fclose($handle);
                $this->info('✓ Loaded '.count($this->involvedCompanyIdToCompanyRole).' involved companies');
            } else {
                $this->warn('⚠ Could not open involved companies file');
            }
        } else {
            $this->warn('⚠ No involved companies file found');
        }
    }
}
