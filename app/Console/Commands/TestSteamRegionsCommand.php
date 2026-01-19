<?php

declare(strict_types=1);

namespace App\Console\Commands;

use App\Services\Price\Steam\SteamStoreService;
use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;

/**
 * Test Steam API with diverse regions to understand response format and timing.
 * 
 * This is a diagnostic tool to validate:
 * - API response format across regions
 * - Rate limiting behavior
 * - Price availability per region
 * - API call timing and performance
 */
class TestSteamRegionsCommand extends Command
{
    protected $signature = 'steam:test-regions 
        {--app-ids=570,730,440 : Comma-separated Steam app IDs to test}
        {--regions=US,GB,JP,BR,AU,DE,IN,MX,RU,ZA : Comma-separated country codes}';

    protected $description = 'Test Steam API pricing across multiple regions with sample games';

    public function handle(SteamStoreService $steam): int
    {
        $appIdsInput = $this->option('app-ids');
        $appIds = array_map('trim', explode(',', $appIdsInput));
        
        $regionsInput = $this->option('regions');
        $regions = array_map('trim', explode(',', $regionsInput));

        $this->info('🧪 Testing Steam API with '.count($appIds).' game(s) across '.count($regions).' region(s)');
        $this->info('Total API calls: '.(count($appIds) * count($regions)));
        $this->newLine();

        $results = [];
        $callCount = 0;
        $startTime = now();

        foreach ($appIds as $appId) {
            $this->info("Testing App ID: {$appId}");
            
            foreach ($regions as $region) {
                $callStart = microtime(true);
                $callCount++;
                
                // Test price fetch
                $priceData = $steam->getPrice($appId, $region);
                
                $callDuration = round((microtime(true) - $callStart) * 1000, 2);
                
                if ($priceData) {
                    $results[] = [
                        'app_id' => $appId,
                        'region' => $region,
                        'currency' => $priceData['currency'],
                        'price' => $priceData['amount_minor'] / 100,
                        'duration_ms' => $callDuration,
                        'status' => '✓',
                    ];
                    
                    $this->line("  [{$region}] {$priceData['currency']} ".number_format($priceData['amount_minor'] / 100, 2)." ({$callDuration}ms)");
                } else {
                    $results[] = [
                        'app_id' => $appId,
                        'region' => $region,
                        'currency' => null,
                        'price' => null,
                        'duration_ms' => $callDuration,
                        'status' => '✗',
                    ];
                    
                    $this->line("  [{$region}] <fg=red>No price available</> ({$callDuration}ms)");
                }
                
                // Small delay to be respectful to API
                usleep(100000); // 100ms between calls
            }
            
            $this->newLine();
        }

        $totalDuration = now()->diffInSeconds($startTime);
        
        // Summary
        $this->newLine();
        $this->info('📊 Summary');
        $this->table(
            ['Metric', 'Value'],
            [
                ['Total API Calls', $callCount],
                ['Successful', collect($results)->where('status', '✓')->count()],
                ['Failed', collect($results)->where('status', '✗')->count()],
                ['Total Time', $totalDuration.'s'],
                ['Avg Time/Call', round($totalDuration / $callCount, 2).'s'],
                ['Calls/Minute', round($callCount / ($totalDuration / 60), 1)],
            ]
        );

        // Show unique currencies found
        $currencies = collect($results)->pluck('currency')->filter()->unique()->sort()->values();
        $this->info('Currencies found: '.$currencies->implode(', '));
        
        // Show which regions had no prices
        $failedRegions = collect($results)
            ->where('status', '✗')
            ->pluck('region')
            ->unique()
            ->sort()
            ->values();
        
        if ($failedRegions->isNotEmpty()) {
            $this->newLine();
            $this->warn('Regions with no prices: '.$failedRegions->implode(', '));
        }

        return self::SUCCESS;
    }
}
