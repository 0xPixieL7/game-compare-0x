<?php

namespace Database\Seeders;

use Illuminate\Database\Seeder;

class PriceInfrastructureSeeder extends Seeder
{
    /**
     * Run the database seeds.
     *
     * Seeds all price comparison infrastructure tables from CSV exports.
     */
    public function run(): void
    {
        $this->command->info('🚀 Seeding price infrastructure from CSV exports...');

        // Order matters: currencies must be seeded before exchange_rates (FK constraint)
        $this->call([
            CurrenciesSeeder::class,
            ExchangeRatesSeeder::class,
            TaxProfilesSeeder::class,
        ]);

        $this->command->info('✅ Price infrastructure seeding complete');
    }
}
