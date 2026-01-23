<?php

namespace App\Console\Commands;

use App\Services\ExchangeRates\ExchangeRateService;
use Illuminate\Console\Command;

class SyncExchangeRatesCommand extends Command
{
    protected $signature = 'rates:sync {--crypto-only : Only sync crypto rates}';
    protected $description = 'Sync exchange rates from all configured providers';

    public function handle(ExchangeRateService $service)
    {
        $this->info('🔄 Syncing exchange rates...');

        $this->info('Syncing BTC rates...');
        $service->refreshBtcRates();

        if (!$this->option('crypto-only')) {
            $this->info('Syncing Forex rates...');
            $service->refreshForexRates();
        }

        $this->info('✅ Synchronization complete!');
        return self::SUCCESS;
    }
}
