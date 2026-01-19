<?php

declare(strict_types=1);

namespace App\Console\Commands;

use App\Jobs\Enrichment\FetchSteamDataJob;
use App\Jobs\Enrichment\FetchSteamPriceForRegionJob;
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
        {--batch-size=1000 : Number of apps to process per batch}
        {--dry-run : Preview without writing to database}
        {--skip-existing : Skip apps that already have Steam links}
        {--enqueue-enrichment : Dispatch jobs to fetch prices and media from Steam}
        {--regions-set=top30 : Region set to use: top30 (default), all, or custom list}';

    protected $description = 'Import Steam app IDs from steam_apps_pretty.json and link to existing video games';

    private const CHECKPOINT_FILE = 'storage/steam_import_checkpoint.json';

    private const STEAM_EXTERNAL_LINK_CATEGORY = 1;

    // Top 30 regions maximizing price disparity and market coverage
    private const TOP_30_REGIONS = [
        'US', 'GB', 'DE', 'FR', 'CA', 'AU', 'JP', 'KR', 'CN', // Major Markets
        'AR', 'BR', 'CL', 'CO', 'PE', 'MX',                   // LATAM (Cheap)
        'IN', 'ID', 'PH', 'TH', 'VN',                         // SE Asia (Cheap)
        'UA', 'KZ', 'TR', 'PL',                               // CIS/East EU (Cheap/Mid)
        'CH', 'NO', 'SE', 'DK', 'IS',                         // Nordic/High (Expensive)
        'ZA',                                                 // Africa
    ];

    private int $processed = 0;

    private int $matched = 0;

    private int $skipped = 0;

    private int $created = 0;

    private int $jobsEnqueued = 0;

    private int $apiCalls = 0;

    private array $nameCache = [];

    public function handle(): int
    {
        $filePath = $this->option('file');
        $limit = (int) $this->option('limit');
        $resume = (bool) $this->option('resume');
        $batchSize = (int) $this->option('batch-size');
        $dryRun = (bool) $this->option('dry-run');
        $skipExisting = (bool) $this->option('skip-existing');
        $enqueueEnrichment = (bool) $this->option('enqueue-enrichment');
        $regionsSet = $this->option('regions-set');

        // Determine target regions
        $regions = match ($regionsSet) {
            'all' => $this->getAllSteamRegions(),
            'top30' => self::TOP_30_REGIONS,
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

        $this->info("Reading Steam apps from: {$filePath}");

        if ($dryRun) {
            $this->warn('DRY RUN MODE - No database changes will be made');
        }

        if ($enqueueEnrichment) {
            $this->info('Enrichment jobs will fetch prices for '.count($regions).' regions');
            $this->comment('Regions: '.implode(', ', array_slice($regions, 0, 10)).(count($regions) > 10 ? '...' : ''));
        }

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
                $this->processBatch($batch, $dryRun, $enqueueEnrichment, $regions);
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
            $this->processBatch($batch, $dryRun, $enqueueEnrichment, $regions);
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
                $normalizedName = $this->normalizeName($game->name);
                $this->nameCache[$normalizedName] = $game->id;
            }
        });
    }

    /**
     * Process a batch of Steam apps.
     */
    private function processBatch(
        array $batch,
        bool $dryRun,
        bool $enqueueEnrichment,
        array $regions
    ): void {
        $links = [];
        $jobsToDispatch = [];
        $priceJobsToDispatch = [];

        foreach ($batch as $app) {
            $this->processed++;

            $appId = (string) $app['appid'];
            $appName = $app['name'] ?? '';

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

            if ($enqueueEnrichment && ! $dryRun) {
                // Job 1: Fetch Media + US Price (Heavy, 1 API Call)
                $jobsToDispatch[] = new FetchSteamDataJob($videoGameId, (int) $appId);
                $this->apiCalls++;

                // Jobs 2+: Fetch Price ONLY for other regions (Light, 29 API Calls)
                foreach ($regions as $region) {
                    if ($region === 'US') {
                        continue; // Already fetched in Job 1
                    }

                    $priceJobsToDispatch[] = new FetchSteamPriceForRegionJob(
                        $videoGameId,
                        (int) $appId,
                        $region
                    );
                    $this->apiCalls++;
                }
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
                $this->error('Batch insert failed - check logs');
            }
        } elseif ($dryRun) {
            $this->created += count($links);
        }

        // Dispatch Jobs
        if (! $dryRun && $enqueueEnrichment) {
            // Dispatch Media Jobs
            foreach ($jobsToDispatch as $job) {
                dispatch($job)->onQueue('prices-steam');
                $this->jobsEnqueued++;
            }

            // Dispatch Price Jobs
            foreach ($priceJobsToDispatch as $job) {
                dispatch($job)->onQueue('prices-steam');
                $this->jobsEnqueued++;
            }
        } elseif ($dryRun && $enqueueEnrichment) {
            $this->jobsEnqueued += count($jobsToDispatch) + count($priceJobsToDispatch);
            // apiCalls is already incremented in the loop above for accurate estimation
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

        // Remove trademark symbols and special characters
        $name = str_replace(['™', '®', '©', ':', '-', '_', '.'], ' ', $name);

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
        ];

        foreach ($suffixes as $suffix) {
            if (str_ends_with($name, $suffix)) {
                $name = substr($name, 0, -strlen($suffix));
            }
        }

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
