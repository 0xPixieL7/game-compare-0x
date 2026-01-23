<?php

declare(strict_types=1);

namespace App\Jobs;

use App\Services\ExchangeRates\ExchangeRateService;
use App\Services\Price\CryptoRebaseService;
use Illuminate\Bus\Queueable;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Bus\Dispatchable;
use Illuminate\Queue\InteractsWithQueue;
use Illuminate\Queue\SerializesModels;
use Illuminate\Support\Facades\Cache;
use Illuminate\Support\Facades\Log;

class SynchronizeGlobalMarketDataJob implements ShouldQueue
{
    use Dispatchable, InteractsWithQueue, Queueable, SerializesModels;

    public int $tries = 1;
    public int $timeout = 300;

    /**
     * Create a new job instance.
     */
    public function __construct(
        private bool $force = false
    ) {}

    /**
     * Execute the job.
     */
    public function handle(ExchangeRateService $exchangeRates, CryptoRebaseService $rebaseService): void
    {
        $lockKey = 'market_data_sync_lock';
        $cooldownKey = 'market_data_sync_cooldown';

        // Prevent multiple simultaneous syncs
        if (!$this->force && Cache::has($lockKey)) {
            return;
        }

        // 15-minute cooldown for automatic triggers to prevent hammering APIs
        if (!$this->force && Cache::has($cooldownKey)) {
            return;
        }

        Cache::put($lockKey, true, 300);
        Cache::put($cooldownKey, true, 900);

        try {
            Log::info('Event-driven market data synchronization started.');
            
            // 1. Sync rates
            $exchangeRates->refreshBtcRates();
            $exchangeRates->refreshForexRates();
            
            // 2. Rebase prices
            $count = $rebaseService->rebaseAllActivePrices();
            
            Log::info("Market data synchronization complete. Rebased {$count} prices.");
        } finally {
            Cache::forget($lockKey);
        }
    }
}
