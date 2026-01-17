<?php

namespace Database\Seeders;

use Illuminate\Database\Seeder;
use Illuminate\Support\Facades\DB;

class TaxProfilesSeeder extends Seeder
{
    /**
     * Run the database seeds.
     */
    public function run(): void
    {
        $csvPath = storage_path('sqlite_exports/tax_profiles.csv');

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

        $profiles = [];
        $count = 0;

        while (($row = fgetcsv($handle)) !== false) {
            $data = array_combine($header, $row);

            $profiles[] = [
                'region_code' => $data['region_code'],
                'vat_rate' => (float) $data['vat_rate'],
                'effective_from' => $data['effective_from'] ?: null,
                'notes' => $data['notes'] ?: null,
                'created_at' => $data['created_at'],
                'updated_at' => $data['updated_at'],
            ];

            $count++;

            // Batch insert every 100 records
            if (count($profiles) >= 100) {
                DB::table('tax_profiles')->insertOrIgnore($profiles);
                $profiles = [];
            }
        }

        // Insert remaining records
        if (! empty($profiles)) {
            DB::table('tax_profiles')->insertOrIgnore($profiles);
        }

        fclose($handle);

        $this->command->info("✅ Imported {$count} tax profiles from CSV");
    }
}
