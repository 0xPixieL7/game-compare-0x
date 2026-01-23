<?php

declare(strict_types=1);

namespace App\Services\Import\Providers;

use App\Jobs\Enrichment\Traits\CategorizesVideoTypes;
use App\Models\Image;
use App\Models\Product;
use App\Models\Video;
use App\Models\VideoGame;
use App\Models\VideoGameSource;
use App\Models\VideoGameTitle;
use App\Models\VideoGameTitleSource;
use App\Services\Import\Contracts\ImportProvider;
use App\Services\Normalization\IgdbRatingHelper;
use App\Services\Normalization\PlatformNormalizer;
use Closure;
use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\File;
use Illuminate\Support\Facades\Log;
use Illuminate\Support\Str;

class IgdbImportProvider implements ImportProvider
{
    use App\Services\Import\Concerns\HasProgressBar;
    use App\Services\Import\Concerns\InteractsWithConsole;
    use CategorizesVideoTypes;

    private const BATCH_SIZE = 4000;

    private const MEDIA_BATCH_SIZE = 4000;

    /**
     * Maximum safe parameter count for PostgreSQL bulk operations.
     */
    private const MAX_SAFE_PARAMS = 65000;

    /**
     * Number of parsed game records to buffer before running a set-based write.
     */
    private const RECORD_BUFFER_SIZE = 2000;

    private const CHECKPOINT_ROWS_INTERVAL = 100;

    private const CHECKPOINT_SECONDS_INTERVAL = 2.0;

    private const SUCCESS = 0;

    private const FAILURE = 1;

    /**
     * In-memory caches to reduce database lookups.
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
     */
    private array $videoGameTitleSourceBatch = [];

    /**
     * Batch queue for aggregating image media per video game.
     */
    private array $imageBatch = [];

    /**
     * Batch queue for aggregating video media per video game.
     */
    private array $videoBatch = [];

    private bool $mergeMedia;

    private bool $fastMode = false;

    private int $progressChunk;

    private array $tableIdOffsets = [];

    private array $tableIdCounters = [];

    /**
     * Map IGDB platform IDs to names.
     */
    private array $platformIdToName = [];

    /**
     * Map IGDB genre IDs to names.
     */
    private array $genreIdToName = [];

    /**
     * Map IGDB company IDs to company names.
     */
    private array $companyIdToName = [];

    /**
     * Map IGDB involved_company IDs to company/role flags.
     */
    private array $involvedCompanyIdToCompanyRole = [];

    public function __construct(
        protected PlatformNormalizer $platformNormalizer,
        protected IgdbRatingHelper $igdbRatingHelper
    ) {}

    public function getName(): string
    {
        return 'igdb';
    }

    public function handle(Command $command): int
    {
        $this->setCommand($command);

        // Start optimized session
        $this->startOptimizedImport();

        // Parse flags
        $this->mergeMedia = (int) $command->option('merge-media') !== 0;
        $this->fastMode = (int) $command->option('fast') !== 0;
        $this->progressChunk = max(0, (int) $command->option('progress-chunk'));
        $fixedOffsets = (bool) $command->option('fixed-offsets');

        // Validate batch size configuration
        $this->validateBatchConfiguration();

        $startTime = microtime(true);
        $inputPath = (string) ($command->option('path') ?: base_path('storage/igdb-dumps'));
        $provider = (string) ($command->option('provider') ?: 'igdb');
        $limit = (int) $command->option('limit');

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

        $gamesFile = $explicitGamesFile ?: $this->findFile($directory, 'games');
        if (! $gamesFile) {
            $this->error('No games CSV/JSON file found.');

            return self::FAILURE;
        }

        // Load reference dumps first
        $this->loadMappings($directory);

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

        $this->info('🚀 Running OpenCritic import (Limit: 5)...');
        $this->command->call('gc:import-opencritic', ['--limit' => 5]);

        $this->info('🚀 Running Retailer Extraction...');
        $this->command->call('app:extract-retailers');

        $this->endOptimizedImport();

        return self::SUCCESS;
    }

    // PLACEHOLDERS START - To be filled by replacement
    private function validateBatchConfiguration(): void
    {
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

        // Ensure directory exists
        if (! File::isDirectory($directory)) {
            return null;
        }

        foreach (File::files($directory) as $file) {
            $filename = $file->getFilename();
            $name = strtolower($filename);
            $ext = strtolower($file->getExtension());

            // Loose check first to optimize
            if (! str_contains($name, $basename)) {
                continue;
            }

            // Strict check: must match "basename.ext" or "timestamp_basename.ext"
            // We use word boundary or underscore limit to prevent "involved_companies" matching "companies"
            if (! preg_match('/(?:^|_)'.preg_quote($basename, '/').'\./', $name)) {
                continue;
            }

            if (! in_array($ext, ['csv', 'json', 'ndjson', 'jsonl'], true)) {
                continue;
            }

            if (str_ends_with($name, '_schema.json') || str_ends_with($name, 'schema.json')) {
                continue;
            }

            if ($file->getSize() === 0) {
                continue;
            }

            $candidates[] = $file;
        }

        if ($candidates === []) {
            return null;
        }

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

            $aName = strtolower($a->getFilename());
            $bName = strtolower($b->getFilename());

            $aTs = preg_match('/^(\d+)_/', $aName, $m1) === 1 ? (int) $m1[1] : 0;
            $bTs = preg_match('/^(\d+)_/', $bName, $m2) === 1 ? (int) $m2[1] : 0;
            if ($aTs !== $bTs) {
                return $bTs <=> $aTs;
            }

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
        $handle = fopen($file, 'r');
        if (! $handle) {
            $this->warn("Could not open games file: {$file}");

            return 0;
        }

        $headers = fgetcsv($handle) ?: [];

        $resumeEnabled = (int) $this->command->option('resume') !== 0;
        if ($this->fastMode) {
            $resumeEnabled = false;
        }
        $resetCheckpoint = (bool) $this->command->option('reset-checkpoint');

        if ($resetCheckpoint) {
            $this->forgetCheckpoint($file, $provider);
        }

        if ($limit <= 0 && $resumeEnabled && ! $resetCheckpoint) {
            $this->maybeSeekToCheckpoint($handle, $file, $provider);
        }

        $processed = 0;
        $errors = 0;

        try {
            $totalRows = $limit > 0 ? $limit : $this->countFileRows($file, false);
            $progressBar = $this->command->getOutput()->createProgressBar($totalRows);
            $this->configureProgressBar($progressBar, true);
            $progressBar->setMessage('0', 'errors');
            $progressBar->setMessage('0', 'skipped');

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

            $read = 0;
            $lastCheckpointAt = microtime(true);
            $lastCheckpointRows = 0;

            /** @var array<int, array<string, mixed>> $recordBuffer */
            $recordBuffer = [];

            $flushRecordBuffer = function () use (&$recordBuffer, $provider, &$errors, $progressBar): void {
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
                    $progressBar->setMessage((string) $errors, 'errors');
                }
            };

            $maybeCheckpoint = function (bool $force = false) use ($handle, $file, $provider, $limit, $resumeEnabled, $flushRecordBuffer, &$lastCheckpointAt, &$lastCheckpointRows, &$processed, &$errors): void {
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
                    'processed' => $processed,
                    'errors' => $errors,
                ]);

                $lastCheckpointAt = microtime(true);
                $lastCheckpointRows = $processed;
            };

            while (($line = fgets($handle)) !== false) {
                $read++;
                $line = trim($line);
                if ($line === '') {
                    $advanceProgress();
                    $maybeCheckpoint();

                    if ($limit > 0 && $read >= $limit) {
                        $maybeCheckpoint(true);
                        break;
                    }

                    continue;
                }

                $decoded = json_decode($line, true);
                if (is_array($decoded)) {
                    try {
                        $recordBuffer[] = $decoded;
                        if (count($recordBuffer) >= self::RECORD_BUFFER_SIZE) {
                            $flushRecordBuffer();
                        }
                    } catch (\Throwable $e) {
                        $errors++;
                        $progressBar->setMessage((string) $errors, 'errors');
                        Log::error('Failed to buffer game record', [
                            'record' => $decoded,
                            'error' => $e->getMessage(),
                        ]);
                    }
                    $processed++;
                    $advanceProgress();

                    $maybeCheckpoint();
                } else {
                    $progressBar->setMessage((string) (++$errors), 'errors');
                    $advanceProgress();

                    $maybeCheckpoint();
                }

                if ($limit > 0 && $read >= $limit) {
                    $maybeCheckpoint(true);
                    break;
                }
            }

            if ($progressCounter > 0) {
                $progressBar->advance($progressCounter);
            }

            $flushRecordBuffer();
            $progressBar->finish();
        } finally {
            fclose($handle);
        }

        if ($limit <= 0 && $resumeEnabled) {
            $this->forgetCheckpoint($file, $provider);
        }

        $this->flushBatches();

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
        $progressBar = $this->command->getOutput()->createProgressBar($total);
        $this->configureProgressBar($progressBar, true);
        $progressBar->setMessage('0', 'errors');
        $progressBar->setMessage('0', 'skipped');
        $errors = 0;
        $skipped = 0;
        $processed = 0;

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

        /** @var array<string, array{name:string, name:string, normalized_title:string}> $productRowsBySlug */
        $productRowsBySlug = [];

        foreach ($records as $record) {
            $gameId = $record['id'] ?? null;
            $gameName = $record['name'] ?? null;
            $gameName = is_string($gameName) && $gameName !== '' ? $gameName : 'Unknown Game';

            $slug = $record['slug'] ?? null;
            $slug = is_string($slug) && $slug !== '' ? $slug : Str::slug($gameName);
            if ($slug === '') {
                $slug = $gameId !== null && $gameId !== '' ? 'game-'.$gameId : 'unknown-game';
            }

            if (! isset($productRowsBySlug[$slug])) {
                $productRowsBySlug[$slug] = [
                    'name' => $gameName,
                    'normalized_title' => Str::slug($gameName),
                    'synopsis' => $record['summary'] ?? $record['storyline'] ?? null,
                ];
            }
        }

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
                'created_at' => $now,
                'updated_at' => $now,
            ];
        }

        foreach ($this->chunkRowsForSafeParams($productRows, self::BATCH_SIZE) as $chunk) {
            DB::table('products')->insertOrIgnore($chunk);
        }

        $slugs = array_keys($productRowsBySlug);
        /** @var array<string, int> $productIdBySlug */
        $productIdBySlug = Product::query()->whereIn('slug', $slugs)->pluck('id', 'slug')->all();

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

        foreach ($this->chunkRowsForSafeParams($titleRows, self::BATCH_SIZE) as $chunk) {
            DB::table('video_game_titles')->insertOrIgnore($chunk);
        }

        /** @var array<string, VideoGameTitle> $titleBySlug */
        $titleBySlug = VideoGameTitle::query()
            ->whereIn('slug', $slugs)
            ->get(['id', 'slug', 'product_id', 'providers'])
            ->keyBy('slug')
            ->all();

        // Ensure provider presence in the title's providers JSON array.
        foreach ($titleBySlug as $slug => $title) {
            $existingProviders = is_array($title->providers) ? $title->providers : [];
            if (! in_array($provider, $existingProviders, true)) {
                $merged = array_values(array_unique(array_merge($existingProviders, [$provider])));
                VideoGameTitle::query()->whereKey($title->id)->update([
                    'providers' => $merged,
                    'updated_at' => $now,
                ]);
                $title->providers = $merged;
            }

            $this->titleCache[(string) $title->product_id] = $title;
        }

        // Finally, enqueue mappings + video games per record.
        foreach ($records as $record) {
            try {
                $gameId = $record['id'] ?? null;
                if ($gameId === null || $gameId === '') {
                    $errors++;
                    Log::error('Failed to import game: missing id', [
                        'record' => $record,
                    ]);

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
                    Log::error('Failed to import game: missing title after upsert', [
                        'slug' => $slug,
                        'record' => $record,
                    ]);

                    continue;
                }

                $this->videoGameTitleSourceBatch[] = [
                    'video_game_title_id' => $title->id,
                    'video_game_source_id' => $source->id,
                    'external_id' => (int) $gameId,
                    'provider_item_id' => (string) $gameId,
                    'raw_payload' => json_encode($record, JSON_THROW_ON_ERROR),
                    'provider' => $provider,
                    // Map explicitly for columns
                    'slug' => $slug,
                    'name' => $gameName,
                    'description' => $record['summary'] ?? $record['storyline'] ?? null,
                    'release_date' => $this->parseDate($record['first_release_date'] ?? null),
                    'platform' => json_encode($this->extractPlatforms($record), JSON_THROW_ON_ERROR),
                    'rating' => $this->igdbRatingHelper()->extractPercentage($record),
                    'rating_count' => $this->igdbRatingHelper()->extractRatingCount($record),
                    'developer' => $this->extractDeveloperAndPublisher($record)['developer'],
                    'publisher' => $this->extractDeveloperAndPublisher($record)['publisher'],
                    'genre' => $this->extractGenresAsJson($record),
                    'created_at' => $now,
                    'updated_at' => $now,
                ];

                $platforms = $this->extractPlatforms($record);
                $companyFields = $this->extractDeveloperAndPublisher($record);
                $rating = $this->igdbRatingHelper()->extractPercentage($record);
                $ratingCount = $this->igdbRatingHelper()->extractRatingCount($record);

                $this->videoGameBatch[] = [
                    'video_game_title_id' => $title->id,
                    'provider' => $provider,
                    'external_id' => (int) $gameId,
                    'slug' => $title->slug,
                    'name' => $gameName, // New column
                    'rating' => $rating, // New column
                    'release_date' => $this->parseDate($record['first_release_date'] ?? null), // New column
                    'attributes' => json_encode([
                        'platform' => $platforms,
                        'slug' => $title->slug,
                        'name' => $gameName,
                        'summary' => $record['summary'] ?? null,
                        'storyline' => $record['storyline'] ?? null,
                        'release_date' => $this->parseDate($record['first_release_date'] ?? null),
                        'rating' => $rating,
                        'rating_count' => $ratingCount,
                        'developer' => $companyFields['developer'],
                        'publisher' => $companyFields['publisher'],
                        // Always persist a JSON array ("[]" when unknown) to match application casts.
                        'genre' => $this->extractGenresAsJson($record),
                        'media' => null,
                        // Provider-specific payloads are mirrored on `video_game_title_sources`.
                        'source_payload' => null,
                    ], JSON_THROW_ON_ERROR),
                    'created_at' => $now,
                    'updated_at' => $now,
                ];

                if (count($this->videoGameBatch) >= self::BATCH_SIZE || count($this->videoGameTitleSourceBatch) >= self::BATCH_SIZE) {
                    $this->flushBatches();
                }
            } catch (\Throwable $e) {
                $errors++;
                Log::error('Failed to import game', [
                    'record' => $record,
                    'error' => $e->getMessage(),
                ]);
            }
        }

        return $errors;
    }

    private function processGameRecord(array $record, string $provider): void {}

    private function flushBatches(): void
    {
        $this->flushVideoGameTitleSourceBatch();
        $this->flushVideoGameBatch();
        $this->flushImageBatch();
        $this->flushVideoBatch();
    }

    private function flushVideoGameTitleSourceBatch(): void
    {
        if ($this->videoGameTitleSourceBatch === []) {
            return;
        }

        foreach ($this->chunkRowsForSafeParams($this->videoGameTitleSourceBatch, self::BATCH_SIZE) as $chunk) {
            DB::table('video_game_title_sources')->upsert(
                $chunk,
                ['video_game_title_id', 'video_game_source_id', 'provider_item_id'],
                [
                    'raw_payload', 'updated_at', 'provider', 'external_id',
                    // Add columns to update
                    'slug', 'name', 'description', 'release_date', 'platform',
                    'rating', 'rating_count', 'developer', 'publisher', 'genre',
                ]
            );
        }

        $this->videoGameTitleSourceBatch = [];
    }

    private function flushVideoGameBatch(): void
    {
        if (empty($this->videoGameBatch)) {
            return;
        }

        foreach ($this->chunkRowsForSafeParams($this->videoGameBatch, self::BATCH_SIZE) as $chunk) {
            DB::table('video_games')->upsert(
                $chunk,
                ['provider', 'external_id'], // Use unique constraint columns
                [] // Do nothing on conflict
            );
        }

        $this->videoGameBatch = [];
    }

    private function flushImageBatch(): void
    {
        if (empty($this->imageBatch)) {
            return;
        }

        $videoGameIds = array_values(array_map('intval', array_keys($this->imageBatch)));

        $existingByGameId = [];
        if ($this->mergeMedia) {
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
                ]);

            foreach ($existingRows as $row) {
                $existingByGameId[(int) ($row->imageable_id ?? $row->video_game_id)] = $row;
            }
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
            if (! $this->mergeMedia) {
                $existing = null;
            }

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

        $this->imageBatch = [];
    }

    private function flushVideoBatch(): void
    {
        if (empty($this->videoBatch)) {
            return;
        }

        $videoGameIds = array_values(array_map('intval', array_keys($this->videoBatch)));

        $existingByGameId = [];
        if ($this->mergeMedia) {
            $existingRows = DB::table('videos')
                ->whereIn('video_game_id', $videoGameIds)
                ->get(['video_game_id', 'urls', 'provider', 'metadata']);

            foreach ($existingRows as $row) {
                $existingByGameId[(int) $row->video_game_id] = $row;
            }
        }

        $upsertData = [];
        foreach ($this->videoBatch as $videoGameId => $batch) {
            $existing = $existingByGameId[(int) $videoGameId] ?? null;
            if (! $this->mergeMedia) {
                $existing = null;
            }

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
            $externalId = $mergedUrls[0] ?? null;

            $videoCollections = [];
            foreach ($mergedMetadata as $meta) {
                if (is_array($meta) && isset($meta['name'])) {
                    $videoCollections[] = $this->categorizeVideoType((string) $meta['name']);
                }
            }
            $videoCollections = array_values(array_unique($videoCollections));
            if (empty($videoCollections)) {
                $videoCollections = ['trailers'];
            }

            $upsertData[] = [
                'videoable_type' => \App\Models\VideoGame::class,
                'videoable_id' => $videoGameId,
                'video_game_id' => $videoGameId,
                'uuid' => (string) \Illuminate\Support\Str::uuid(),
                'collection_names' => json_encode($videoCollections),
                'primary_collection' => $videoCollections[0],
                'url' => $primaryUrl,
                'external_id' => $externalId,
                'video_id' => $externalId,
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

        $this->videoBatch = [];
    }

    private function checkpointPath(string $file, string $provider): string
    {
        $hash = md5($file.$provider);

        return storage_path("app/import_checkpoint_{$hash}.json");
    }

    private function loadCheckpoint(string $file, string $provider): ?array
    {
        $path = $this->checkpointPath($file, $provider);
        if (! File::exists($path)) {
            return null;
        }

        $data = json_decode(File::get($path), true);

        return is_array($data) ? $data : null;
    }

    private function storeCheckpoint(string $file, string $provider, array $data): void
    {
        File::put($this->checkpointPath($file, $provider), json_encode($data));
    }

    private function forgetCheckpoint(string $file, string $provider): void
    {
        File::delete($this->checkpointPath($file, $provider));
    }

    private function maybeSeekToCheckpoint(mixed $handle, string $file, string $provider): void
    {
        $checkpoint = $this->loadCheckpoint($file, $provider);
        if (! $checkpoint) {
            return;
        }

        $pos = $checkpoint['pos'] ?? 0;
        if ($pos > 0) {
            fseek($handle, $pos);
            $this->info("Resuming from checkpoint at byte position {$pos}...");
        }
    }

    private function countFileRows(string $file, bool $hasHeader): int
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
     * Chunk rows to avoid exceeding SQLite/Prepared statement variable limits.
     *
     * @param  array<int, mixed>  $rows
     * @return array<int, array<int, mixed>>
     */
    private function chunkRowsForSafeParams(array $rows, int $preferredChunkSize): array
    {
        if ($rows === []) {
            return [];
        }

        // Conservative estimate of params per row (e.g. 14 columns).
        // SQLite limit is typically 999 or 32766 depending on version.
        // We stick to a safe limit.
        $firstRow = reset($rows);
        $columns = count((array) $firstRow);
        $maxPerChunk = floor(self::MAX_SAFE_PARAMS / max(1, $columns));

        $chunkSize = (int) min($preferredChunkSize, $maxPerChunk);

        return array_chunk($rows, $chunkSize);
    }

    private function parseDate(?string $date): ?string
    {
        if ($date === null || $date === '') {
            return null;
        }

        if (is_numeric($date)) {
            return date('Y-m-d', (int) $date);
        }

        try {
            return \Illuminate\Support\Carbon::parse($date)->format('Y-m-d');
        } catch (\Throwable) {
            return null;
        }
    }

    private function processMediaIfPresent(string $path, string $basename, callable $attach, string $provider): int
    {
        // Check if file exists using findFile
        $file = $this->findFile($path, $basename);
        if (! $file) {
            // $this->line("  ⚠️  {$basename}: not found, skipping"); // Optional log
            return 0;
        }

        $this->info("  📥 {$basename}...");

        return $this->processMediaCsvStreaming($file, $provider, $attach);
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
            if (! in_array($videoId, $this->videoBatch[$gameId]['urls'], true)) {
                $this->videoBatch[$gameId]['urls'][] = $videoId;
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

    private function mergeUniqueStrings(array $existing, array $incoming): array
    {
        return array_values(array_unique(array_merge($existing, $incoming)));
    }

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
        $gameIdMap = $this->preloadGameIdMappings($provider);
        if (empty($gameIdMap)) {
            $this->warn("No game ID mappings found - skipping media import for {$file}");
            fclose($handle);

            return 0;
        }

        // Check for resume support
        $resumeEnabled = (int) $this->command->option('resume') !== 0;
        $resetCheckpoint = (bool) $this->command->option('reset-checkpoint');

        if ($resetCheckpoint) {
            $this->forgetCheckpoint($file, $provider);
        }

        if ($resumeEnabled && ! $resetCheckpoint) {
            $this->maybeSeekToCheckpoint($handle, $file, $provider);
        }

        $totalRows = $this->countFileRows($file, true);
        $progressBar = $this->command->getOutput()->createProgressBar($totalRows);
        $this->configureProgressBar($progressBar, true);
        $progressBar->setMessage('0', 'errors');
        $progressBar->setMessage('0', 'skipped');

        $processed = 0;
        $errors = 0;
        $skipped = 0;

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

            $gameId = (int) ($record['game_id'] ?? $record['id'] ?? 0);
            if ($gameId === 0 && isset($record['game'])) {
                $gameId = (int) $record['game'];
            }

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
        fclose($handle);

        if ($resumeEnabled) {
            $this->forgetCheckpoint($file, $provider);
        }

        return $processed;
    }

    private function preloadGameIdMappings(string $provider): array
    {
        return DB::table('video_games')
            ->where('provider', $provider)
            ->pluck('id', 'external_id')
            ->all();
    }

    private function combineCsvRow(array $headers, array $row): ?array
    {
        if (count($headers) !== count($row)) {
            return null;
        }

        return array_combine($headers, $row);
    }

    private function parseIgdbIdSetString(string $raw): array
    {
        $trimmed = trim($raw, '{}');
        if ($trimmed === '') {
            return [];
        }

        return array_map('intval', explode(',', $trimmed));
    }

    private function loadMappings(string $directory): void
    {
        // Platforms
        $this->loadCsvMap($directory, 'platforms', function (array $data) {
            if (isset($data['id'], $data['name'])) {
                $this->platformIdToName[(int) $data['id']] = $data['name'];
            }
        });

        // Genres
        $this->loadCsvMap($directory, 'genres', function (array $data) {
            if (isset($data['id'], $data['name'])) {
                $this->genreIdToName[(int) $data['id']] = $data['name'];
            }
        });

        // Companies
        $this->loadCsvMap($directory, 'companies', function (array $data) {
            if (isset($data['id'], $data['name'])) {
                $this->companyIdToName[(int) $data['id']] = $data['name'];
            }
        });

        // Involved Companies
        $this->loadCsvMap($directory, 'involved_companies', function (array $data) {
            if (isset($data['id'], $data['company'])) {
                $this->involvedCompanyIdToCompanyRole[(int) $data['id']] = [
                    'company_id' => (int) $data['company'],
                    'developer' => ($data['developer'] ?? 'false') === 'true',
                    'publisher' => ($data['publisher'] ?? 'false') === 'true',
                ];
            }
        });
    }

    private function loadCsvMap(string $directory, string $basename, Closure $callback): void
    {
        $file = $this->findFile($directory, $basename);
        if (! $file) {
            return;
        }

        $this->info("Loading {$basename} map from {$file}...");

        $handle = fopen($file, 'r');
        if (! $handle) {
            return;
        }

        $headers = fgetcsv($handle);
        if (! $headers) {
            fclose($handle);

            return;
        }

        while (($row = fgetcsv($handle)) !== false) {
            $data = $this->combineCsvRow($headers, $row);
            if ($data) {
                $callback($data);
            }
        }
        fclose($handle);
    }

    private function extractPlatforms(array $record): array
    {
        if (isset($record['platforms']) && is_array($record['platforms'])) {
            $names = [];
            foreach ($record['platforms'] as $platform) {
                if (is_array($platform) && isset($platform['name'])) {
                    $names[] = (string) $platform['name'];
                }
            }

            $names = $this->platformNormalizer->normalizeMany($names);

            return $names !== [] ? $names : ['PC'];
        }

        if (isset($record['platforms']) && is_string($record['platforms'])) {
            $raw = trim($record['platforms']);

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

                $values = $this->platformNormalizer->normalizeMany($values);

                return $values !== [] ? $values : ['PC'];
            }

            $ids = $this->parseIgdbIdSetString($raw);
            if ($ids !== []) {
                $names = array_map(fn (int $id) => $this->platformIdToName[$id] ?? (string) $id, $ids);
                $names = $this->platformNormalizer->normalizeMany($names);

                return $names !== [] ? $names : ['PC'];
            }

            $parts = preg_split('/[\s,|]+/', $raw) ?: [];
            $parts = array_values(array_filter(array_map('trim', $parts), fn ($v) => $v !== ''));

            $parts = $this->platformNormalizer->normalizeMany($parts);

            return $parts !== [] ? $parts : ['PC'];
        }

        return ['PC'];
    }

    private function extractGenresAsJson(array $record): string
    {
        $genres = [];

        if (isset($record['genres']) && is_array($record['genres'])) {
            foreach ($record['genres'] as $g) {
                if (is_array($g) && isset($g['name']) && is_string($g['name']) && $g['name'] !== '') {
                    $genres[] = $g['name'];
                } elseif (is_string($g) && $g !== '') {
                    $genres[] = $g;
                }
            }
        }

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
        $developer = isset($record['developer']) && is_string($record['developer']) && $record['developer'] !== ''
        ? $record['developer']
        : null;
        $publisher = isset($record['publisher']) && is_string($record['publisher']) && $record['publisher'] !== ''
        ? $record['publisher']
        : null;

        if ($developer !== null || $publisher !== null) {
            return [
                'developer' => $developer,
                'publisher' => $publisher,
            ];
        }

        $raw = $record['involved_companies'] ?? null;
        if (! is_string($raw) || $raw === '') {
            return [
                'developer' => null,
                'publisher' => null,
            ];
        }

        if ($this->involvedCompanyIdToCompanyRole === [] || $this->companyIdToName === []) {
            return [
                'developer' => null,
                'publisher' => null,
            ];
        }

        $ids = $this->parseIgdbIdSetString($raw);
        if ($ids === []) {
            return [
                'developer' => null,
                'publisher' => null,
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

        return [
            'developer' => $developerNames !== [] ? implode(', ', $developerNames) : null,
            'publisher' => $publisherNames !== [] ? implode(', ', $publisherNames) : null,
        ];
    }
}
