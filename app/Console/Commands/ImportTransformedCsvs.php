<?php

declare(strict_types=1);

namespace App\Console\Commands;

use App\Jobs\Enrichment\EnrichVideoGameJob;
use App\Models\VideoGame;
use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\File;
use Illuminate\Support\Facades\Log;

class ImportTransformedCsvs extends Command
{
    protected $signature = 'import:transformed-csvs
                            {--batch=2000 : Batch size for inserts}
                            {--skip-on-error : Skip rows that cause errors instead of failing}
                            {--table= : Import only specific table}
                            {--enrich : Dispatch enrichment jobs after import}';

    protected $description = 'Import transformed CSV files with batching (respects FK order), speed optimizations, and logging';

    private int $batchSize;

    private int $errorSkippedRows = 0;

    private string $logChannel = 'daily';

    /**
     * Map of Table Name => CSV Filename
     * Order matters for Foreign Key constraints!
     */
    private array $importSequence = [
        // 1. Independent Reference Tables
        'users' => 'users.csv',
        'video_game_sources' => 'video_game_sources.csv',
        'currencies' => 'currencies.csv',
        'countries' => 'countries.csv', // Depends on currencies
        'tax_profiles' => 'tax_profiles.csv',

        // 2. Core Data (Transformed)
        'products' => 'products_TRANSFORMED.csv',
        'video_game_titles' => 'video_game_titles_TRANSFORMED.csv',
        'video_games' => 'video_games_TRANSFORMED.csv',

        // 3. Dependent Data
        'video_game_title_sources' => 'video_game_title_sources_TRANSFORMED.csv',
        'exchange_rates' => 'exchange_rates.csv', // Depends on currencies

        // 4. Media
        'media' => 'media.csv',
        'videos' => 'videos_TRANSFORMED.csv',
        'images' => 'images_TRANSFORMED.csv',

        // 5. Regional/Pricing Data
        // 'sku_regions' CSV actually maps to 'video_game_prices' table in new schema
        'video_game_prices' => 'sku_regions.csv',
    ];

    /**
     * Columns to strip from certain tables during import
     * to avoid "Undefined column" errors.
     */
    private array $excludedColumns = [
        'users' => ['discord_id', 'github_id', 'google_id', 'avatar_url', 'timezone'],
        'video_game_sources' => [],
        'media' => ['derived_from_type', 'derived_from_id'],
        'images' => [],
        'video_games' => [], // Description exists in schema
        'video_game_prices' => ['country_id', 'currency_id'],
    ];

    public function handle(): int
    {
        ini_set('memory_limit', '512M');
        $this->batchSize = (int) $this->option('batch');
        $basePath = storage_path('sqlite_exports');

        // Setup dedicated log channel on the fly
        config(['logging.channels.import_debug' => [
            'driver' => 'single',
            'path' => storage_path('logs/import_debug.log'),
            'level' => 'debug',
        ]]);
        $this->logChannel = 'import_debug';

        // Optimize for speed
        DB::disableQueryLog();
        DB::connection()->unsetEventDispatcher();

        $this->info('╔═══════════════════════════════════════════════════════════════╗');
        $this->info('║   CSV Import - Optimized Batch Processing                    ║');
        $this->info('╚═══════════════════════════════════════════════════════════════╝');
        $this->newLine();

        Log::channel($this->logChannel)->info('Starting CSV Import: '.date('Y-m-d H:i:s'));

        // Validate files exist
        $this->validateFiles($basePath);

        $this->newLine();

        $this->newLine();

        // Import in FK-safe order
        $specificTable = $this->option('table');

        foreach ($this->importSequence as $table => $csvFile) {
            if ($specificTable && $table !== $specificTable) {
                continue;
            }

            if ($table === 'video_game_prices') {
                $this->importVideoGamePrices($basePath);
            } else {
                $this->line("DEBUG: Processing table '{$table}' from file '{$csvFile}'");
                $this->importTable($table, $basePath.'/'.$csvFile);
            }
        }

        $this->newLine();

        if ($this->errorSkippedRows > 0) {
            $this->warn("⚠️  Skipped {$this->errorSkippedRows} rows due to errors (see logs)");
        }

        $this->info('✅ Import complete!');
        Log::channel($this->logChannel)->info('CSV Import Completed: '.date('Y-m-d H:i:s'), [
            'error_skipped' => $this->errorSkippedRows,
        ]);

        // Dispatch enrichment jobs if requested
        if ($this->option('enrich')) {
            $this->dispatchEnrichmentJobs();
        }

        return self::SUCCESS;
    }

    private function dispatchEnrichmentJobs(): void
    {
        $this->newLine();
        $this->info('🔄 Dispatching enrichment jobs for games needing prices/media...');

        $count = 0;
        $startTime = microtime(true);

        // Find games that don't have prices OR don't have images
        VideoGame::query()
            ->where(function ($query) {
                $query->whereDoesntHave('prices')
                    ->orWhereDoesntHave('images');
            })
            ->select('id')
            ->chunkById(100, function ($games) use (&$count) {
                foreach ($games as $game) {
                    EnrichVideoGameJob::dispatch($game->id);
                    $count++;
                }
            });

        $duration = round(microtime(true) - $startTime, 2);
        $this->info("   ✅ Queued {$count} enrichment jobs in {$duration}s");
        Log::channel($this->logChannel)->info('Enrichment jobs dispatched', [
            'count' => $count,
            'duration' => $duration,
        ]);
    }

    private function importVideoGamePrices(string $basePath): void
    {
        $this->info('📥 Importing video_game_prices (Merging Regions + Prices)...');
        Log::channel($this->logChannel)->info('Starting import for table: video_game_prices');

        $startTime = microtime(true);

        // 1. Build Product ID -> Video Game ID Map
        $this->info('   Map: Building Product -> VideoGame ID map...');
        $productToGameMap = DB::table('video_game_titles')
            ->join('video_games', 'video_games.video_game_title_id', '=', 'video_game_titles.id')
            ->pluck('video_games.id', 'video_game_titles.product_id')
            ->all();

        $this->info('   Map: Loaded '.count($productToGameMap).' mappings.');

        // 2. Load SKU Regions into Memory
        // Key: sku_region_id, Value: [product_id, ...data]
        $this->info('   Map: Loading SKU Regions...');
        $regionsMap = [];
        $skuRegionsPath = $basePath.'/sku_regions.csv';

        if (($handle = fopen($skuRegionsPath, 'r')) !== false) {
            $headers = fgetcsv($handle); // Skip header
            while (($row = fgetcsv($handle)) !== false) {
                // defined as: id,product_id,region_code,retailer,currency,sku,is_active,metadata,created_at,updated_at,country_id,currency_id
                // index matches header usually:
                // 0: id, 1: product_id, 2: region_code, 3: retailer, 4: currency, 5: sku, 6: is_active, 10: country_id
                if (count($row) < 7) {
                    continue;
                }

                $regionsMap[$row[0]] = [
                    'product_id' => $row[1],
                    'region_code' => $row[2],
                    'retailer' => $row[3],
                    'currency' => $row[4],
                    'sku' => $row[5],
                    'is_active' => $row[6],
                    'country_id' => $row[10] ?? null,
                ];
            }
            fclose($handle);
        }
        $this->info('   Map: Loaded '.count($regionsMap).' regions.');

        // 3. Stream Prices and Join
        $pricesPath = $basePath.'/region_prices.csv';
        if (! file_exists($pricesPath)) {
            $this->error('   ❌ Missing region_prices.csv');

            return;
        }

        // Count lines for progress
        $lineCount = 0;
        if ($handle = fopen($pricesPath, 'r')) {
            while (fgets($handle) !== false) {
                $lineCount++;
            }
            fclose($handle);
        }

        $handle = fopen($pricesPath, 'r');
        fgetcsv($handle); // Skip header: id,sku_region_id,recorded_at,fiat_amount...

        $bar = $this->output->createProgressBar($lineCount);
        $bar->start();

        $batch = [];
        $totalRows = 0;

        DB::beginTransaction();

        $debugCount = 0;
        try {
            // region_prices columns:
            // 0:id, 1:sku_region_id, 2:recorded_at, 3:fiat_amount, 11:currency_id, 12:country_id, 13:local_amount
            while (($row = fgetcsv($handle)) !== false) {
                $skuRegionId = $row[1] ?? null;
                $region = $regionsMap[$skuRegionId] ?? null;

                if (! $region) {
                    continue;
                } // Orphaned price

                $productId = $region['product_id'];
                $videoGameId = $productToGameMap[$productId] ?? null;

                if (! $videoGameId) {
                    continue;
                } // No matching game

                // Determine Price (Minor Units)
                // Prefer local_amount (13), fallback to fiat_amount (3)
                $amountMajor = $row[13] !== '' ? $row[13] : $row[3];
                if (! is_numeric($amountMajor)) {
                    continue;
                }

                $amountMinor = (int) (floatval($amountMajor) * 100);

                $batch[] = [
                    'video_game_id' => $videoGameId,
                    'product_id' => $productId, // Optional, but schema has it
                    'currency' => $region['currency'],
                    'amount_minor' => $amountMinor,
                    'recorded_at' => $row[2], // Keep as string, DB handles cast? or parse
                    'retailer' => $region['retailer'],
                    'tax_inclusive' => false, // Default
                    'region_code' => $region['region_code'],
                    'country_code' => null, // Could map country_id -> country_code if needed
                    'sku' => $region['sku'],
                    'is_active' => filter_var($region['is_active'], FILTER_VALIDATE_BOOLEAN),
                    'created_at' => now(), // New import
                    'updated_at' => now(),
                ];

                if (count($batch) >= $this->batchSize) {
                    DB::table('video_game_prices')->insert($batch); // insertOrIgnore not needed if new unique IDs? use insert for speed
                    $totalRows += count($batch);
                    $bar->advance(count($batch));
                    $batch = [];

                    if ($totalRows % ($this->batchSize * 5) === 0) {
                        DB::commit();
                        DB::beginTransaction();
                    }
                }
            }

            if (! empty($batch)) {
                DB::table('video_game_prices')->insert($batch);
                $totalRows += count($batch);
                $bar->advance(count($batch));
            }

            DB::commit();

        } catch (\Exception $e) {
            DB::rollBack();
            $this->error('Error: '.$e->getMessage());
            Log::error($e);
        }

        $bar->finish();
        fclose($handle);

        $duration = round(microtime(true) - $startTime, 2);
        $this->newLine();
        $this->info("   ✅ Imported {$totalRows} prices in {$duration}s");
    }

    private function validateFiles(string $basePath): void
    {
        $this->info('📂 Validating CSV files...');

        $missing = [];
        foreach ($this->importSequence as $table => $csvFile) {
            // Special check for merge files
            if ($table === 'video_game_prices') {
                if (! File::exists($basePath.'/sku_regions.csv')) {
                    $missing[] = 'sku_regions.csv';
                }
                if (! File::exists($basePath.'/region_prices.csv')) {
                    $missing[] = 'region_prices.csv';
                }

                continue;
            }

            $fullPath = $basePath.'/'.$csvFile;
            if (! File::exists($fullPath)) {
                $missing[] = $csvFile;
            }
        }

        if (! empty($missing)) {
            $this->error('❌ Missing CSV files:');
            foreach ($missing as $file) {
                $this->line("   - $file");
            }
            Log::channel($this->logChannel)->error('Missing CSV files', ['files' => $missing]);
            exit(1);
        }

        $this->info('✅ All CSV files found');
        $this->newLine();
    }

    /**
     * Cache of valid IDs to prevent FK violations
     */
    private array $validProductIds = [];

    private array $validTitleIds = [];

    private array $validGameIds = [];

    private function importTable(string $table, string $csvPath): void
    {
        $this->info("📥 Importing {$table}...");
        Log::channel($this->logChannel)->info("Starting import for table: {$table}");

        // Pre-load valid IDs if needed for filtering
        $this->loadValidIdsForTable($table);

        $startTime = microtime(true);
        $totalRows = 0;
        $skippedRows = 0;

        // Count lines for progress bar (fast estimate)
        $lineCount = 0;
        $handle = fopen($csvPath, 'r');
        if ($handle) {
            while (! feof($handle)) {
                $line = fgets($handle);
                if ($line !== false) {
                    $lineCount++;
                }
            }
            fclose($handle);
        }
        $lineCount = max(0, $lineCount - 1); // Subtract header

        $handle = fopen($csvPath, 'r');
        if ($handle === false) {
            $this->error("   ❌ Failed to open {$csvPath}");
            Log::channel($this->logChannel)->error("Failed to open CSV: {$csvPath}");

            return;
        }

        // Read headers
        $headers = fgetcsv($handle, 0, ',', '"', '\\');
        if ($headers === false) {
            $this->error("   ❌ Failed to read headers from {$csvPath}");
            Log::channel($this->logChannel)->error("Failed to read headers: {$csvPath}");
            fclose($handle);

            return;
        }

        $bar = $this->output->createProgressBar($lineCount);
        $bar->setFormat(' %current%/%max% [%bar%] %percent:3s%% %elapsed:6s%/%estimated:-6s% %memory:6s%');
        $bar->start();

        $batch = [];

        DB::beginTransaction();

        try {
            while (($row = fgetcsv($handle, 0, ',', '"', '\\')) !== false) {
                // Handle mismatched column counts
                if (count($row) !== count($headers)) {
                    $row = array_pad(array_slice($row, 0, count($headers)), count($headers), '');
                }

                // Build associative array
                $data = array_combine($headers, $row);

                // Convert data and filter columns
                $data = $this->normalizeRow($data, $table);

                // Skip if validation failed (e.g. invalid video URL)
                if ($data === null) {
                    $skippedRows++;
                    $bar->advance();

                    continue;
                }

                // Check dependencies (FK validity)
                if ($this->shouldSkip($table, $data)) {
                    $skippedRows++;
                    $bar->advance();

                    continue;
                }

                $batch[] = $data;

                // Insert batch when size reached
                if (count($batch) >= $this->batchSize) {
                    $this->insertBatch($table, $batch);
                    $totalRows += count($batch);
                    $bar->advance(count($batch));
                    $batch = [];

                    // Commit larger chunks
                    if ($totalRows % ($this->batchSize * 10) === 0) {
                        DB::commit();
                        DB::beginTransaction();
                    }
                }
            }

            // Insert remaining rows
            if (! empty($batch)) {
                $this->insertBatch($table, $batch);
                $totalRows += count($batch);
                $bar->advance(count($batch));
            }

            DB::commit();

        } catch (\Exception $e) {
            DB::rollBack();
            $bar->finish();
            $this->newLine();
            $this->error("❌ Error importing {$table}: ".$e->getMessage());
            Log::channel($this->logChannel)->error("Error importing {$table}", ['exception' => $e]);

            return;
        }

        $bar->finish();
        fclose($handle);

        $duration = round(microtime(true) - $startTime, 2);
        $rowsPerSecond = $duration > 0 ? round($totalRows / $duration) : 0;

        $this->newLine();
        $this->info("   ✅ Imported {$totalRows} rows (Skipped {$skippedRows}) ({$rowsPerSecond} rows/sec) in {$duration}s");
        Log::channel($this->logChannel)->info("Finished importing {$table}", [
            'rows' => $totalRows,
            'skipped' => $skippedRows,
            'duration' => $duration,
            'speed' => "{$rowsPerSecond} rows/sec",
        ]);
        $this->newLine();
    }

    private function loadValidIdsForTable(string $table): void
    {
        if ($table === 'video_game_titles') {
            $this->info('   Map: Loading valid Product IDs...');
            // Load as keys for O(1) lookup
            $this->validProductIds = array_flip(DB::table('products')->pluck('id')->all());
        } elseif ($table === 'video_games' || $table === 'video_game_title_sources') {
            if (empty($this->validTitleIds)) {
                $this->info('   Map: Loading valid Title IDs...');
                $this->validTitleIds = array_flip(DB::table('video_game_titles')->pluck('id')->all());
            }
        } elseif ($table === 'videos' || $table === 'images') {
            if (empty($this->validGameIds)) {
                $this->info('   Map: Loading valid Game IDs...');
                $this->validGameIds = array_flip(DB::table('video_games')->pluck('id')->all());
            }
        }
    }

    private function shouldSkip(string $table, array $row): bool
    {
        if ($table === 'video_game_titles') {
            // Must have valid product_id
            if (! isset($this->validProductIds[$row['product_id']])) {
                return true;
            }
        } elseif ($table === 'video_games') {
            // Must have valid video_game_title_id
            if (! isset($this->validTitleIds[$row['video_game_title_id']])) {
                Log::channel($this->logChannel)->warning('Skipping video_game: Invalid title_id via shouldSkip', ['row_id' => $row['id'] ?? 'unknown', 'title_id' => $row['video_game_title_id']]);

                return true;
            }
        } elseif ($table === 'video_game_title_sources') {
            if (! isset($this->validTitleIds[$row['video_game_title_id']])) {
                return true;
            }
        } elseif ($table === 'videos' || $table === 'images') {
            // If polymorphic map to Game, check ID
            if (isset($row['videoable_type']) && $row['videoable_type'] === 'App\Models\VideoGame') {
                if (! isset($this->validGameIds[$row['videoable_id']])) {
                    return true;
                }
            }
            if (isset($row['imageable_type']) && $row['imageable_type'] === 'App\Models\VideoGame') {
                if (! isset($this->validGameIds[$row['imageable_id']])) {
                    return true;
                }
            }
        }

        return false;
    }

    private function insertBatch(string $table, array $batch): void
    {
        // Debug log first batch for media table
        static $mediaFirstBatch = false;
        if ($table === 'media' && ! $mediaFirstBatch && ! empty($batch)) {
            $mediaFirstBatch = true;
            Log::channel($this->logChannel)->debug('Media first row data', [
                'row' => $batch[0] ?? [],
                'keys' => array_keys($batch[0] ?? []),
            ]);
        }

        if ($this->option('skip-on-error')) {
            // Insert row by row to isolate errors
            foreach ($batch as $row) {
                try {
                    DB::table($table)->insertOrIgnore([$row]);
                } catch (\Exception $e) {
                    Log::channel($this->logChannel)->warning("Skipped row in {$table}", [
                        'error' => $e->getMessage(),
                        'row_id' => $row['id'] ?? 'unknown',
                    ]);
                    $this->errorSkippedRows++;
                }
            }
        } else {
            // Use insertOrIgnore for safety (skip duplicates)
            DB::table($table)->insertOrIgnore($batch);
        }
    }

    private function normalizeRow(array $row, string $table): ?array
    {
        // Filter out excluded columns for this table
        if (isset($this->excludedColumns[$table])) {
            foreach ($this->excludedColumns[$table] as $column) {
                unset($row[$column]);
            }
        }

        // Special validation and mapping for specific tables
        if ($table === 'video_game_sources') {
            // Map keys to provider if missing - prioritize provider_key
            if (empty($row['provider'])) {
                $row['provider'] = $row['provider_key'] ?? $row['slug'] ?? null;
            }

            // Allow all columns to pass through (provider_key, slug, category, display_name, metadata)
        } elseif ($table === 'video_games') {
            // Map description to summary (as requested by user)
            if (empty($row['summary']) && ! empty($row['description'])) {
                $row['summary'] = $row['description'];
            }
            // Ensure description is unset so it doesn't cause "column not found" error
            unset($row['description']);
        } elseif ($table === 'media') {
            // Renumber media IDs to start from 0 instead of 1
            // This prevents FK violations when images have empty media_id (converts to 0)
            if (isset($row['id']) && is_numeric($row['id'])) {
                $row['id'] = (int) $row['id'] - 1;
            }

            // Handle JSON columns - decode and re-encode to ensure valid JSON
            $jsonColumns = ['manipulations', 'custom_properties', 'generated_conversions', 'responsive_images'];
            foreach ($jsonColumns as $column) {
                if (isset($row[$column]) && is_string($row[$column]) && ! empty($row[$column])) {
                    // Try to decode the JSON
                    $decoded = json_decode($row[$column], true);

                    if (json_last_error() !== JSON_ERROR_NONE) {
                        // If JSON decode fails, use an appropriate default based on column
                        if ($column === 'custom_properties') {
                            // For custom_properties, try to salvage what we can or use empty object
                            $row[$column] = '{}';
                        } else {
                            // For arrays (manipulations, generated_conversions, responsive_images)
                            $row[$column] = '[]';
                        }
                    } else {
                        // Re-encode to ensure clean JSON
                        $row[$column] = json_encode($decoded, JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE);
                    }
                } elseif (isset($row[$column]) && empty($row[$column])) {
                    // Set empty strings to appropriate defaults
                    $row[$column] = ($column === 'custom_properties') ? '{}' : '[]';
                }
            }
        }

        if ($table === 'videos' && empty($row['url'])) {
            return null; // Skip invalid video
        }

        foreach ($row as $key => $value) {
            // Convert empty strings to null
            if ($value === '' || $value === null) {
                $row[$key] = null;

                continue;
            }

            // Handle JSON fields - skip type conversion for these
            if (in_array($key, ['metadata', 'providers', 'platform', 'genre', 'media', 'source_payload', 'raw_payload', 'urls', 'manipulations', 'custom_properties', 'generated_conversions', 'responsive_images'])) {
                continue;
            }

            // Handle boolean fields
            if (in_array($key, ['is_thumbnail', 'tax_inclusive', 'is_active', 'is_retail_buy'])) {
                $row[$key] = (int) filter_var($value, FILTER_VALIDATE_BOOLEAN);

                continue;
            }

            // Handle numeric fields
            if (in_array($key, ['id', 'product_id', 'video_game_title_id', 'video_game_source_id', 'videoable_id', 'imageable_id', 'video_game_id', 'media_id', 'external_id', 'provider_item_id', 'width', 'height', 'duration', 'rating_count', 'amount_minor', 'sku_region_id', 'sales_volume', 'user_id', 'country_id', 'currency_id', 'order_column', 'model_id', 'size'])) {
                // Special case: media_id for images can be NULL (not all images have media records)
                if ($table === 'images' && $key === 'media_id' && ($value === '' || $value === null)) {
                    static $firstLog = false;
                    if (! $firstLog) {
                        Log::channel($this->logChannel)->debug('Images: Setting empty media_id to NULL', ['original' => $value]);
                        $firstLog = true;
                    }
                    $row[$key] = null;

                    continue;
                }

                $row[$key] = $value !== null && $value !== '' ? (int) $value : null;

                continue;
            }

            // Handle decimal fields
            if (in_array($key, ['rating', 'rate', 'fiat_amount', 'btc_value', 'fx_rate_snapshot', 'btc_rate_snapshot', 'local_amount'])) {
                $row[$key] = $value !== null && $value !== '' ? (float) $value : null;

                continue;
            }

            // Handle date/datetime fields
            if (in_array($key, ['created_at', 'updated_at', 'release_date', 'published_at', 'fetched_at', 'recorded_at', 'email_verified_at', 'two_factor_confirmed_at'])) {
                try {
                    $row[$key] = $value ? \Carbon\Carbon::parse($value) : null;
                } catch (\Exception $e) {
                    $row[$key] = null;
                }

                continue;
            }
        }

        return $row;
    }
}
