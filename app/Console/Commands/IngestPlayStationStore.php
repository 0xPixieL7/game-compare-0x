<?php

declare(strict_types=1);

namespace App\Console\Commands;

use App\Services\Providers\PlayStationStoreProvider;
use Illuminate\Console\Command;
use PlaystationStoreApi\Enum\CategoryEnum;

class IngestPlayStationStore extends Command
{
    protected $signature = 'ingest:playstation
        {--regions=en-us : Comma-separated regions (e.g., en-us,en-gb,ja-jp)}
        {--category=ps5-games : Category to fetch (ps5-games, ps4-games, etc.)}
        {--max-pages=1 : Maximum pages to fetch from catalog}
        {--mode=auto : Operation mode: auto, discover, ingest}
        {--file= : Path to concept IDs file (required for ingest mode if not auto)}
        {--workers=1 : Number of parallel workers for ingestion}
        {--chunk= : Internal: process specific chunk (format: N/TOTAL)}';

    protected $description = 'Ingest game prices from PlayStation Store across multiple regions (Async/Parallel supported)';

    public function handle(): int
    {
        $regionsInput = $this->option('regions');
        $regions = array_map('trim', explode(',', $regionsInput));
        $categoryStr = $this->option('category');
        $category = $this->resolveCategoryEnum($categoryStr);
        $maxPages = (int) $this->option('max-pages');
        $mode = $this->option('mode');
        $file = $this->option('file');
        $workers = (int) $this->option('workers');
        $chunk = $this->option('chunk');

        // Worker Mode
        if ($chunk) {
            return $this->runAsChildWorker($file, $chunk, $regions);
        }

        $this->info('Starting PlayStation Store ingestion...');
        $this->info("Mode: {$mode}");

        // MODE: DISCOVER (or AUTO)
        if ($mode === 'discover' || $mode === 'auto') {
            $this->info("Phase 1: Discovery ({$categoryStr}, max {$maxPages} pages)");
            $provider = new PlayStationStoreProvider($regions);
            
            // Allow file override or default
            $targetFile = $file ?? storage_path("app/ps_concepts_{$categoryStr}.json");
            
            $conceptIds = $provider->fetchCatalogConceptIds($regions[0], $category, $maxPages);
            $count = count($conceptIds);
            
            $this->info("Discovered {$count} concepts.");
            
            file_put_contents($targetFile, json_encode($conceptIds));
            $this->info("Saved concept IDs to: {$targetFile}");
            
            if ($mode === 'discover') {
                return self::SUCCESS;
            }
            
            // Pass file to next phase
            $file = $targetFile;
        }

        // MODE: INGEST (or AUTO)
        if ($workers > 1) {
            return $this->runParallelImport($file, $workers, $regionsInput);
        }

        // Single Process Ingest
        $this->info("Phase 2: Ingestion (Single Process)");
        
        $conceptIds = json_decode(file_get_contents($file), true);
        if (!$conceptIds) {
            $this->error("No concepts to process in file: {$file}");
            return self::FAILURE;
        }
        
        $provider = new PlayStationStoreProvider($regions);
        $bar = $this->output->createProgressBar(count($conceptIds));
        $bar->start();

        $stats = ['created' => 0, 'updated' => 0, 'skipped' => 0, 'errors' => 0];

        foreach ($conceptIds as $conceptId) {
            try {
                $result = $provider->ingestConceptWithMultiRegionPricing($conceptId);
                
                if ($result['created']) $stats['created']++;
                elseif ($result['updated']) $stats['updated']++;
                else $stats['skipped']++;
                
            } catch (\Throwable $e) {
                $stats['errors']++;
                // Log::error(...)
            }
            $bar->advance();
        }

        $bar->finish();
        $this->newLine(2);
        
        $this->table(
            ['Metric', 'Count'],
            [
                ['Created', $stats['created']],
                ['Updated', $stats['updated']],
                ['Skipped', $stats['skipped']],
                ['Errors', $stats['errors']],
            ]
        );

        return self::SUCCESS;
    }

    /**
     * Run parallel import.
     */
    private function runParallelImport(string $filePath, int $workers, string $regionsInput): int
    {
        $this->info("🚀 Starting parallel ingestion with {$workers} workers...");

        $phpBinary = PHP_BINARY;
        $artisanPath = base_path('artisan');
        $processes = [];

        for ($i = 1; $i <= $workers; $i++) {
            $cmd = [
                $phpBinary,
                $artisanPath,
                'ingest:playstation',
                "--file={$filePath}",
                "--workers=1",
                "--chunk={$i}/{$workers}",
                "--regions={$regionsInput}",
                "--mode=ingest", // Force ingest mode for workers
            ];

            $process = new \Symfony\Component\Process\Process($cmd);
            $process->setTimeout(null);
            $process->start();
            $processes[$i] = $process;
            
            $this->info("Started worker {$i}");
        }

        // Monitoring Loop
        while (count($processes) > 0) {
            foreach ($processes as $key => $proc) {
                if (!$proc->isRunning()) {
                    $this->info("Worker {$key} finished (Exit: " . $proc->getExitCode() . ")");
                    if ($proc->getExitCode() !== 0) {
                        $this->error($proc->getErrorOutput());
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
    private function runAsChildWorker(string $filePath, string $chunkSpec, array $regions): int
    {
        list($index, $total) = explode('/', $chunkSpec);
        
        $conceptIds = json_decode(file_get_contents($filePath), true);
        if (!$conceptIds) return self::FAILURE;

        $chunkSize = ceil(count($conceptIds) / $total);
        $offset = ($index - 1) * $chunkSize;
        $myIds = array_slice($conceptIds, (int)$offset, (int)$chunkSize);

        $provider = new PlayStationStoreProvider($regions);
        
        foreach ($myIds as $conceptId) {
            try {
                $provider->ingestConceptWithMultiRegionPricing($conceptId);
            } catch (\Throwable $e) {
                // Squelch errors in worker, logs capture them
            }
        }

        return self::SUCCESS;
    }


    private function resolveCategoryEnum(string $category): CategoryEnum
    {
        return match (strtolower($category)) {
            'ps5-games', 'ps5_games' => CategoryEnum::PS5_GAMES,
            'ps4-games', 'ps4_games' => CategoryEnum::PS4_GAMES,
            default => CategoryEnum::PS5_GAMES,
        };
    }
}
