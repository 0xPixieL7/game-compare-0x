<?php

namespace App\Console\Commands;

use App\Services\Price\CryptoRebaseService;
use Illuminate\Console\Command;

class RebasePricesToBTCCommand extends Command
{
    protected $signature = 'prices:rebase-btc';
    protected $description = 'Rebase all active game prices to BTC';

    public function handle(CryptoRebaseService $service)
    {
        $this->info('🛡️ Rebasing all prices to BTC...');
        
        $count = $service->rebaseAllActivePrices();
        
        $this->info("✅ Successfully rebased {$count} prices.");
        return self::SUCCESS;
    }
}
