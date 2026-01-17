<?php

declare(strict_types=1);

namespace Database\Seeders;

use App\Models\Country;
use Illuminate\Database\Seeder;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Log;

class CountriesSeeder extends Seeder
{
    /**
     * Run the database seeds.
     */
    public function run(): void
    {
        $csvPath = storage_path('sqlite_exports/countries.csv');

        if (! file_exists($csvPath)) {
            Log::error("Countries CSV not found at: {$csvPath}");
            $this->command->error("Countries CSV not found at: {$csvPath}");

            return;
        }

        $this->command->info("Reading countries from: {$csvPath}");

        DB::transaction(function () use ($csvPath) {
            $handle = fopen($csvPath, 'r');

            // Read and verify header
            $header = fgetcsv($handle);
            $expectedHeader = ['id', 'code', 'name', 'currency_id', 'region', 'metadata', 'created_at', 'updated_at'];

            if ($header !== $expectedHeader) {
                $this->command->warn('CSV header does not match expected format.');
                $this->command->line('Expected: '.implode(', ', $expectedHeader));
                $this->command->line('Got: '.implode(', ', $header));
            }

            $countries = [];
            $rowCount = 0;

            while (($row = fgetcsv($handle)) !== false) {
                $rowCount++;

                // Parse CSV row
                $countries[] = [
                    'code' => $row[1],
                    'name' => $row[2],
                    'currency_id' => (int) $row[3],
                    'region' => ! empty($row[4]) ? $row[4] : null,
                    'metadata' => ! empty($row[5]) ? json_decode($row[5], true) : null,
                    'created_at' => now(),
                    'updated_at' => now(),
                ];

                // Insert in batches of 50
                if (count($countries) >= 50) {
                    Country::insert($countries);
                    $this->command->info("Inserted {$rowCount} countries...");
                    $countries = [];
                }
            }

            // Insert remaining countries
            if (! empty($countries)) {
                Country::insert($countries);
            }

            fclose($handle);

            $totalCount = Country::count();
            $this->command->info("✅ Successfully imported {$totalCount} countries.");

            // Display currency distribution
            $this->command->newLine();
            $this->command->info('Currency Distribution:');

            $distribution = DB::table('countries')
                ->select('currencies.code', DB::raw('COUNT(*) as country_count'))
                ->join('currencies', 'countries.currency_id', '=', 'currencies.id')
                ->groupBy('currencies.code')
                ->orderBy('country_count', 'desc')
                ->get();

            foreach ($distribution as $row) {
                $this->command->line("  {$row->code}: {$row->country_count} countries");
            }
        });
    }
}
