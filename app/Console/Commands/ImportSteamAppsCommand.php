<?php

declare(strict_types=1);

namespace App\Console\Commands;

use App\Jobs\Enrichment\FetchSteamDataJob;
use App\Models\VideoGame;
use App\Services\Import\Concerns\CanOptimizeImport;
use App\Services\Import\Concerns\HasProgressBar;
use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\File;
use Illuminate\Support\Facades\Log;

class ImportSteamAppsCommand extends Command
{
    use CanOptimizeImport;
    use HasProgressBar;

    protected $signature = 'steam:import-apps
        {--file=steam_apps_pretty.json : Path to Steam apps JSON file}
        {--limit=0 : Limit number of apps to process (0 = no limit)}
        {--resume=1 : Resume from last processed app (1/0)}
        {--resume=1 : Resume from last processed app (1/0)}
        {--batch-size=1000 : Number of apps to process per batch}
        {--dry-run : Preview without writing to database}
        {--skip-existing : Skip apps that already have Steam links}
        {--enqueue-enrichment : (Deprecated) Use --enrich instead}
        {--enrich : Fetch full price and media data inline (async/parallel)}
        {--workers=1 : Number of parallel workers}
        {--chunk= : Internal: process specific chunk (format: N/TOTAL)}
        {--regions-set=major : Region set to use: major (40+ regions), all (DB list), or comma-separated list (e.g. en-US,ja-JP)}';

    protected $description = 'Import Steam app IDs from steam_apps_pretty.json and link to existing video games';

    private const CHECKPOINT_FILE = 'storage/steam_import_checkpoint.json';

    private const STEAM_EXTERNAL_LINK_CATEGORY = 1;

    // Comprehensive regions maximizing price disparity and market coverage (40+ countries)
    private const MAJOR_REGIONS = [
        'US', 'GB', 'JP', 'KR', 'CN', 'HK', 'TW',             // Tier 1 Markets
        'DE', 'FR', 'ES', 'IT', 'NL', 'BE', 'AT', 'PT', 'IE', // Europe (Euro)
        'CA', 'AU', 'NZ',                                     // Commonwealth
        'BR', 'MX', 'CL', 'CO', 'PE',                         // LATAM
        'IN', 'ID', 'PH', 'TH', 'VN', 'MY', 'SG',             // SE Asia
        'RU', 'UA', 'KZ', 'TR', 'PL',                         // CIS/Eastern Europe
        'NO', 'SE', 'DK', 'CH', 'IS', 'FI',                   // Nordics/High-Income
        'ZA',                                                 // Africa
    ];

    private int $processed = 0;

    private int $matched = 0;

    private int $skipped = 0;

    private int $created = 0;

    private int $jobsEnqueued = 0;

    private int $apiCalls = 0;

    private array $nameCache = [];

    private ?\App\Services\Price\Steam\SteamStoreService $steamService = null;

    public function handle(): int
    {
        $filePath = $this->option('file');
        $limit = (int) $this->option('limit');
        $resume = (bool) $this->option('resume');
        $batchSize = (int) $this->option('batch-size');
        $dryRun = (bool) $this->option('dry-run');
        $skipExisting = (bool) $this->option('skip-existing');
        $enqueueEnrichment = (bool) $this->option('enqueue-enrichment');
        $enrich = (bool) $this->option('enrich') || $enqueueEnrichment;
        $regionsSet = $this->option('regions-set');
        $workers = (int) $this->option('workers');
        $chunk = $this->option('chunk');

        // Determine target regions
        $regions = match ($regionsSet) {
            'all' => $this->getAllSteamRegions(),
            'top30', 'major' => self::MAJOR_REGIONS,
            default => array_filter(array_map('trim', explode(',', $regionsSet))),
        };

        // Resolve file path
        if (! str_starts_with($filePath, '/')) {
            $filePath = base_path($filePath);
        }

        if (! File::exists($filePath)) {
            $this->error("File not found: {$filePath}");

            return self::FAILURE;
        }

        // Normalize regions (handle en-US -> ['country' => 'US', 'language' => 'en'])
        $normalizedRegions = array_map(function ($r) {
            if (str_contains($r, '-')) {
                $parts = explode('-', $r);

                return [
                    'country' => strtoupper(end($parts)),
                    'language' => strtolower($parts[0]),
                ];
            }

            return [
                'country' => strtoupper($r),
                'language' => null,
            ];
        }, $regions);

        // Child process mode
        if ($chunk) {
            return $this->runAsChildWorker($filePath, $chunk, $limit, $enrich, $normalizedRegions);
        }

        $this->info("Reading Steam apps from: {$filePath}");

        if ($dryRun) {
            $this->warn('DRY RUN MODE - No database changes will be made');
        }

        if ($enrich) {
            $this->info('Enrichment enabled: Will fetch Prices + Media using workers.');
        }

        // Parallel Mode
        if ($workers > 1) {
            return $this->runParallelImport($filePath, $workers, $limit, $enrich, $regionsSet);
        }

        // --- Single Process Mode (Legacy Logic) ---

        // Start optimized import (PostgreSQL performance tuning)
        if (! $dryRun) {
            $this->startOptimizedImport();
        }

        // Load checkpoint
        $checkpoint = $this->loadCheckpoint($resume);
        $lastAppId = $checkpoint['last_app_id'] ?? 0;

        if ($resume && $lastAppId > 0) {
            $this->info("Resuming from app ID: {$lastAppId}");
        }

        // Build name lookup cache for matching
        $this->info('Building video game name cache...');
        $this->buildNameCache($skipExisting);
        $this->info('Cache built with '.count($this->nameCache).' games');

        // Parse JSON file
        $this->info('Parsing Steam apps JSON...');
        $jsonContent = File::get($filePath);
        $data = json_decode($jsonContent, true);

        if (! isset($data['response']['apps'])) {
            $this->error('Invalid JSON structure - missing response.apps');

            return self::FAILURE;
        }

        $apps = $data['response']['apps'];
        $totalApps = count($apps);

        $this->info("Found {$totalApps} Steam apps");

        // Filter to resume point
        if ($lastAppId > 0) {
            $apps = array_filter($apps, fn ($app) => $app['appid'] > $lastAppId);
            $this->info('Filtered to '.count($apps).' apps after checkpoint');
        }

        // Apply limit
        if ($limit > 0) {
            $apps = array_slice($apps, 0, $limit);
            $this->info("Limited to {$limit} apps");
        }

        // Process in batches
        $bar = $this->output->createProgressBar(count($apps));
        $bar->setFormat(' %current%/%max% [%bar%] %percent:3s%% | Matched: %matched% | Jobs: %jobs% | API Calls: %api% | Mem: %memory%');
        $bar->setMessage((string) $this->matched, 'matched');
        $bar->setMessage((string) $this->jobsEnqueued, 'jobs');
        $bar->setMessage((string) $this->apiCalls, 'api');

        $batch = [];
        $lastCheckpoint = time();

        foreach ($apps as $app) {
            $batch[] = $app;

            if (count($batch) >= $batchSize) {
                $this->processBatch($batch, $dryRun, $enrich, $regions);
                $batchCount = count($batch);
                $batch = [];

                // Update progress
                $bar->setMessage((string) $this->matched, 'matched');
                $bar->setMessage((string) $this->jobsEnqueued, 'jobs');
                $bar->setMessage((string) $this->apiCalls, 'api');
                $bar->advance($batchCount);

                // Save checkpoint every 60 seconds
                if (time() - $lastCheckpoint >= 60) {
                    $this->saveCheckpoint($app['appid']);
                    $lastCheckpoint = time();
                }
            }
        }

        // Process remaining batch
        if (! empty($batch)) {
            $this->processBatch($batch, $dryRun, $enrich, $regions);
            $bar->advance(count($batch));
        }

        $bar->finish();
        $this->newLine(2);

        // Summary
        $this->info('Import Summary:');
        $this->table(
            ['Metric', 'Count'],
            [
                ['Processed', $this->processed],
                ['Matched', $this->matched],
                ['Created', $this->created],
                ['Skipped', $this->skipped],
                ['Jobs Enqueued', $this->jobsEnqueued],
                ['Est. API Calls', $this->apiCalls],
            ]
        );

        if ($this->apiCalls > 100000) {
            $this->warn("⚠️  WARNING: Estimated API calls ({$this->apiCalls}) exceed the Steam daily limit of 100,000.");
        }

        // End optimized import (restore PostgreSQL settings)
        if (! $dryRun) {
            $this->endOptimizedImport();
            $this->outputPerformanceReport();
        }

        // Clear checkpoint on success
        if (! $dryRun) {
            $this->clearCheckpoint();
        }

        return self::SUCCESS;
    }

    /**
     * Get all Steam-available regions from the countries table.
     *
     * @return array<int, string>
     */
    private function getAllSteamRegions(): array
    {
        return DB::table('countries')
            ->orderBy('code')
            ->pluck('code')
            ->toArray();
    }

    /**
     * Build in-memory cache of video game names mapped to IDs.
     * Optimized for fast name-based lookup during import.
     */
    private function buildNameCache(bool $skipExisting): void
    {
        $query = VideoGame::query()
            ->select('id', 'name', 'external_id', 'provider');

        // If skipping existing, exclude games that already have Steam links
        if ($skipExisting) {
            $query->whereDoesntHave('externalLinks', function ($q) {
                $q->where('category', self::STEAM_EXTERNAL_LINK_CATEGORY);
            });
        }

        // Stream results to avoid memory issues
        $query->chunk(5000, function ($games) {
            foreach ($games as $game) {
                $normalizedName = $this->normalizeName((string) $game->name);
                $this->nameCache[$normalizedName] = $game->id;
            }
        });
    }

    /**
     * Run parallel import.
     */
    private function runParallelImport(string $filePath, int $workers, int $limit, bool $enrich, string $regionsSet): int
    {
        $this->info("🚀 Starting parallel import with {$workers} workers...");

        $phpBinary = PHP_BINARY;
        $artisanPath = base_path('artisan');

        $processes = [];

        for ($i = 1; $i <= $workers; $i++) {
            $cmd = [
                $phpBinary,
                $artisanPath,
                'steam:import-apps',
                "--file={$filePath}",
                "--limit={$limit}",
                '--workers=1',
                "--chunk={$i}/{$workers}",
                "--regions-set={$regionsSet}",
                '--resume=0', // Workers handle their own scope
            ];

            if ($enrich) {
                $cmd[] = '--enrich';
            }

            $process = new \Symfony\Component\Process\Process($cmd);
            $process->setTimeout(null);
            $process->start();
            $processes[$i] = $process;

            $this->info("Started worker {$i}");
        }

        $this->info('Waiting for workers...');

        // Monitor
        while (count($processes) > 0) {
            foreach ($processes as $key => $proc) {
                if (! $proc->isRunning()) {
                    $this->info("Worker {$key} finished with exit code: ".$proc->getExitCode());
                    if ($proc->getExitCode() !== 0) {
                        $this->error($proc->getErrorOutput());
                    } else {
                        // Optional: Parse output for stats
                        $output = $proc->getOutput();
                        if (preg_match('/WORKER_STATS:(\d+):(\d+)/', $output, $m)) {
                            $this->info("Worker {$key} processed {$m[1]} apps, enriched {$m[2]}");
                        }
                    }
                    unset($processes[$key]);
                }
            }
            sleep(1);
        }

        return self::SUCCESS;
    }

    /**
     * Run as a child worker.
     */
    private function runAsChildWorker(string $filePath, string $chunkSpec, int $limit, bool $enrich, array $regions): int
    {
        [$index, $total] = explode('/', $chunkSpec);
        $index = (int) $index;
        $total = (int) $total;

        $this->buildNameCache(false); // Can't easily skip-existing in chunked mode efficiently without checking DB, so load all names.

        $jsonContent = File::get($filePath);
        $data = json_decode($jsonContent, true);
        $apps = $data['response']['apps'] ?? [];

        if ($limit > 0) {
            $apps = array_slice($apps, 0, $limit);
        }

        // Split apps among workers
        $chunkSize = ceil(count($apps) / $total);
        $offset = ($index - 1) * $chunkSize;
        $myApps = array_slice($apps, (int) $offset, (int) $chunkSize);

        $this->processed = 0;
        $this->matched = 0;

        $this->steamService = $enrich ? app(\App\Services\Price\Steam\SteamStoreService::class) : null;

        $batch = [];
        $batchSize = 200; // Smaller batch for parallel to avoid lock contention

        foreach ($myApps as $app) {
            $batch[] = $app;
            if (count($batch) >= $batchSize) {
                $this->processBatch($batch, false, $enrich, $regions);
                $batch = [];
            }
        }

        if (! empty($batch)) {
            $this->processBatch($batch, false, $enrich, $regions);
        }

        // Output stats for parent
        $this->line("WORKER_STATS:{$this->processed}:{$this->matched}");

        return self::SUCCESS;
    }

    /**
     * Process a batch of Steam apps.
     */
    private function processBatch(
        array $batch,
        bool $dryRun,
        bool $enrich, // Renamed from enqueueEnrichment
        array $regions
    ): void {
        $links = [];

        foreach ($batch as $app) {
            $this->processed++;

            $appId = (string) $app['appid'];
            $appName = (string) ($app['name'] ?? '');

            if (empty($appName)) {
                $this->skipped++;

                continue;
            }

            // Try to match to existing video game
            $normalizedName = $this->normalizeName($appName);
            $videoGameId = $this->nameCache[$normalizedName] ?? null;

            if (! $videoGameId) {
                $this->skipped++;

                continue;
            }

            $this->matched++;

            // Prepare external link record
            $links[] = [
                'video_game_id' => $videoGameId,
                'category' => self::STEAM_EXTERNAL_LINK_CATEGORY,
                'external_id' => $appId,
                'url' => "https://store.steampowered.com/app/{$appId}",
                'created_at' => now(),
                'updated_at' => now(),
            ];

            if ($enrich && ! $dryRun && $this->steamService) {
                // INLINE ENRICHMENT (Async/Parallel Worker Mode)
                $this->enrichGameInline($videoGameId, (int) $appId, $regions);
            }
        }

        // Bulk insert external links
        if (! $dryRun && ! empty($links)) {
            try {
                // Using bulkInsertOptimized from CanOptimizeImport trait
                $this->bulkInsertOptimized(
                    'video_game_external_links',
                    $links,
                    null,
                    true, // ignore duplicates
                    ['video_game_id', 'category', 'external_id'],
                    ['url', 'updated_at']
                );
                $this->created += count($links);
            } catch (\Exception $e) {
                Log::error('Steam import batch insert failed: '.$e->getMessage());
            }
        }
    }

    /**
     * Enrich a game inline (Fetch Price + Media).
     * This replaces FetchSteamDataJob in parallel mode.
     */
    private function enrichGameInline(int $videoGameId, int $steamAppId, array $regions): void
    {
        try {
            // Find the primary region (prefer US if present, else first in list)
            $primaryIndex = 0;
            foreach ($regions as $i => $r) {
                if ($r['country'] === 'US') {
                    $primaryIndex = $i;
                    break;
                }
            }
            $primary = $regions[$primaryIndex];

            // 1. Fetch Full Details for Primary Region - Price + Media
            (new \App\Jobs\Enrichment\FetchSteamDataJob(
                $videoGameId,
                $steamAppId,
                false,
                $primary['language']
            ))->handle($this->steamService);

            // 2. Fetch Other Regions (Price Only)
            foreach ($regions as $i => $r) {
                if ($i === $primaryIndex) {
                    continue; // Already fetched
                }

                // Small delay between region calls for same game to be polite
                usleep(50000); // 50ms

                try {
                    (new \App\Jobs\Enrichment\FetchSteamPriceForRegionJob(
                        $videoGameId,
                        $steamAppId,
                        $r['country'],
                        $r['language']
                    ))->handle($this->steamService);
                } catch (\Exception $e) {
                    // Ignore individual region failures
                }
            }

            // Global Rate Limit per game (US + Regions)
            usleep(100000); // 100ms global cooldown per game

        } catch (\Throwable $e) {
            Log::error("Failed to enrich game {$videoGameId}: ".$e->getMessage());
        }
    }

    /**
     * Normalize game name for matching.
     * Removes special characters, converts to lowercase, removes common suffixes.
     */
    private function normalizeName(string $name): string
    {
        // Convert to lowercase
        $name = mb_strtolower($name);

        // Handle smart quotes and special dashes
        $name = str_replace(['‘', '’', '“', '”', '–', '—'], ["'", "'", '"', '"', '-', '-'], $name);

        // Remove trademark symbols and common special characters used as delimiters
        // Including comma which often precedes subtitles
        $name = str_replace(['™', '®', '©', ':', '-', '_', '.', ','], ' ', $name);

        // Remove years and tags in parentheses like "(2003)" or "(Classic, 2005)"
        $name = preg_replace('/\((?:[^)]*?(?:19\d{2}|20\d{2}|legacy|classic|remastered|digital|anniversary)[^)]*?)\)/i', ' ', $name);

        // Remove common edition suffixes
        $suffixes = [
            ' complete edition',
            ' game of the year edition',
            ' goty edition',
            ' deluxe edition',
            ' ultimate edition',
            ' remastered',
            ' definitive edition',
            ' enhanced edition',
            ' directors cut',
            ' standard edition',
            ' anniversary edition',
            ' gold edition',
            ' digital deluxe edition',
            ' gold',
            ' double pack',
            ' collector\'s edition',
            ' collectors edition',
        ];

        foreach ($suffixes as $suffix) {
            if (str_ends_with($name, $suffix)) {
                $name = substr($name, 0, -strlen($suffix));
            }
        }

        // Handle & vs and
        $name = str_replace(' & ', ' and ', $name);

        // Remove extra whitespace
        $name = preg_replace('/\s+/', ' ', $name);

        return trim($name);
    }

    /**
     * Load checkpoint from storage.
     */
    private function loadCheckpoint(bool $resume): array
    {
        if (! $resume) {
            return [];
        }

        $path = base_path(self::CHECKPOINT_FILE);

        if (! File::exists($path)) {
            return [];
        }

        $content = File::get($path);

        return json_decode($content, true) ?? [];
    }

    /**
     * Save checkpoint to storage.
     */
    private function saveCheckpoint(int $lastAppId): void
    {
        $data = [
            'last_app_id' => $lastAppId,
            'processed' => $this->processed,
            'matched' => $this->matched,
            'created' => $this->created,
            'skipped' => $this->skipped,
            'timestamp' => now()->toIso8601String(),
        ];

        $path = base_path(self::CHECKPOINT_FILE);
        File::put($path, json_encode($data, JSON_PRETTY_PRINT));
    }

    /**
     * Clear checkpoint file.
     */
    private function clearCheckpoint(): void
    {
        $path = base_path(self::CHECKPOINT_FILE);

        if (File::exists($path)) {
            File::delete($path);
        }
    }
}
