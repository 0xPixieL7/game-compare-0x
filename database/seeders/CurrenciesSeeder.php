<?php

namespace Database\Seeders;

use Illuminate\Database\Seeder;
use Illuminate\Support\Facades\DB;

class CurrenciesSeeder extends Seeder
{
    /**
     * Run the database seeds.
     */
    public function run(): void
    {
        $csvPath = storage_path('sqlite_exports/currencies.csv');

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

        $currencies = [];
        $count = 0;

        while (($row = fgetcsv($handle)) !== false) {
            $data = array_combine($header, $row);

            $currencies[] = [
                'code' => $data['code'],
                'name' => $data['name'],
                'symbol' => $data['symbol'] ?: null,
                'decimals' => (int) $data['decimals'],
                'is_crypto' => (bool) $data['is_crypto'],
                'metadata' => $data['metadata'] ?: null,
                'created_at' => $data['created_at'],
                'updated_at' => $data['updated_at'],
            ];

            $count++;

            // Batch insert every 100 records
            if (count($currencies) >= 100) {
                DB::table('currencies')->insertOrIgnore($currencies);
                $currencies = [];
            }
        }

        // Insert remaining records
        if (! empty($currencies)) {
            DB::table('currencies')->insertOrIgnore($currencies);
        }

        fclose($handle);

        $this->command->info("✅ Imported {$count} currencies from CSV");
    }
}
