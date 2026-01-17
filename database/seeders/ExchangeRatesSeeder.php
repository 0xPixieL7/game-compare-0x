<?php

namespace Database\Seeders;

use Illuminate\Database\Seeder;
use Illuminate\Support\Facades\DB;

class ExchangeRatesSeeder extends Seeder
{
    /**
     * Run the database seeds.
     */
    public function run(): void
    {
        $csvPath = storage_path('sqlite_exports/exchange_rates.csv');

        if (! file_exists($csvPath)) {
            $this->command->warn("⚠️  CSV file not found: {$csvPath}");

            return;
        }

        $handle = fopen($csvPath, 'r');
        if ($handle === false) {
            $this->command->error("Failed to open CSV file: {$csvPath}");

            return;
        }

        // Read header
        $header = fgetcsv($handle);
        if ($header === false) {
            $this->command->error('Failed to read CSV header');
            fclose($handle);

            return;
        }

        $rates = [];
        $count = 0;

        while (($row = fgetcsv($handle)) !== false) {
            $data = array_combine($header, $row);

            $rates[] = [
                'base_currency' => $data['base_currency'],
                'quote_currency' => $data['quote_currency'],
                'rate' => (float) $data['rate'],
                'fetched_at' => $data['fetched_at'],
                'provider' => $data['provider'] ?: null,
                'metadata' => $data['metadata'] ?: null,
                'created_at' => $data['created_at'],
                'updated_at' => $data['updated_at'],
            ];

            $count++;

            // Batch insert every 100 records
            if (count($rates) >= 100) {
                DB::table('exchange_rates')->insertOrIgnore($rates);
                $rates = [];
            }
        }

        // Insert remaining records
        if (! empty($rates)) {
            DB::table('exchange_rates')->insertOrIgnore($rates);
        }

        fclose($handle);

        $this->command->info("✅ Imported {$count} exchange rates from CSV");
    }
}
