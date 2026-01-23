<?php

namespace Database\Seeders;

use App\Models\User;
// use Illuminate\Database\Console\Seeds\WithoutModelEvents;
use Illuminate\Database\Seeder;

class DatabaseSeeder extends Seeder
{
    /**
     * Seed the application's database.
     */
    public function run(): void
    {
        // Initial Data Baselines
        $this->call([
            ComprehensiveCurrencySeeder::class,
            ComprehensiveCountrySeeder::class,
        ]);

        // Immediate Data Availability
        $this->command->info('Syncing market data and rebasing prices...');
        \App\Jobs\SynchronizeGlobalMarketDataJob::dispatchSync(true);


    }
}
