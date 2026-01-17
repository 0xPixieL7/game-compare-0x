<?php

declare(strict_types=1);

namespace Database\Seeders;

use Illuminate\Database\Seeder;
use Illuminate\Support\Facades\DB;

class RetailerCountriesSeeder extends Seeder
{
    /**
     * Run the database seeds.
     *
     * Adds countries from all regions where Steam Store and PlayStation Store operate.
     */
    public function run(): void
    {
        $this->command->info('Adding countries for Steam Store and PlayStation Store regions...');

        // Get currency mappings (code to ID)
        $currencyMap = DB::table('currencies')->pluck('id', 'code')->toArray();

        $countries = [
            // Americas - Steam & PS Store
            ['code' => 'US', 'name' => 'United States', 'currency' => 'USD', 'region' => 'North America'],
            ['code' => 'CA', 'name' => 'Canada', 'currency' => 'CAD', 'region' => 'North America'],
            ['code' => 'MX', 'name' => 'Mexico', 'currency' => 'MXN', 'region' => 'Latin America'],
            ['code' => 'BR', 'name' => 'Brazil', 'currency' => 'BRL', 'region' => 'Latin America'],
            ['code' => 'AR', 'name' => 'Argentina', 'currency' => 'ARS', 'region' => 'Latin America'],
            ['code' => 'CL', 'name' => 'Chile', 'currency' => 'CLP', 'region' => 'Latin America'],
            ['code' => 'CO', 'name' => 'Colombia', 'currency' => 'COP', 'region' => 'Latin America'],
            ['code' => 'PE', 'name' => 'Peru', 'currency' => 'PEN', 'region' => 'Latin America'],
            ['code' => 'UY', 'name' => 'Uruguay', 'currency' => 'UYU', 'region' => 'Latin America'],
            ['code' => 'CR', 'name' => 'Costa Rica', 'currency' => 'USD', 'region' => 'Latin America'],
            ['code' => 'PA', 'name' => 'Panama', 'currency' => 'USD', 'region' => 'Latin America'],
            ['code' => 'EC', 'name' => 'Ecuador', 'currency' => 'USD', 'region' => 'Latin America'],
            ['code' => 'BO', 'name' => 'Bolivia', 'currency' => 'USD', 'region' => 'Latin America'],
            ['code' => 'PY', 'name' => 'Paraguay', 'currency' => 'USD', 'region' => 'Latin America'],

            // Europe - Steam & PS Store (EUR countries)
            ['code' => 'DE', 'name' => 'Germany', 'currency' => 'EUR', 'region' => 'Europe'],
            ['code' => 'FR', 'name' => 'France', 'currency' => 'EUR', 'region' => 'Europe'],
            ['code' => 'IT', 'name' => 'Italy', 'currency' => 'EUR', 'region' => 'Europe'],
            ['code' => 'ES', 'name' => 'Spain', 'currency' => 'EUR', 'region' => 'Europe'],
            ['code' => 'NL', 'name' => 'Netherlands', 'currency' => 'EUR', 'region' => 'Europe'],
            ['code' => 'BE', 'name' => 'Belgium', 'currency' => 'EUR', 'region' => 'Europe'],
            ['code' => 'AT', 'name' => 'Austria', 'currency' => 'EUR', 'region' => 'Europe'],
            ['code' => 'PT', 'name' => 'Portugal', 'currency' => 'EUR', 'region' => 'Europe'],
            ['code' => 'IE', 'name' => 'Ireland', 'currency' => 'EUR', 'region' => 'Europe'],
            ['code' => 'FI', 'name' => 'Finland', 'currency' => 'EUR', 'region' => 'Europe'],
            ['code' => 'GR', 'name' => 'Greece', 'currency' => 'EUR', 'region' => 'Europe'],
            ['code' => 'LU', 'name' => 'Luxembourg', 'currency' => 'EUR', 'region' => 'Europe'],
            ['code' => 'MT', 'name' => 'Malta', 'currency' => 'EUR', 'region' => 'Europe'],
            ['code' => 'CY', 'name' => 'Cyprus', 'currency' => 'EUR', 'region' => 'Europe'],

            // Europe - Non-EUR
            ['code' => 'GB', 'name' => 'United Kingdom', 'currency' => 'GBP', 'region' => 'Europe'],
            ['code' => 'CH', 'name' => 'Switzerland', 'currency' => 'CHF', 'region' => 'Europe'],
            ['code' => 'NO', 'name' => 'Norway', 'currency' => 'NOK', 'region' => 'Europe'],
            ['code' => 'SE', 'name' => 'Sweden', 'currency' => 'SEK', 'region' => 'Europe'],
            ['code' => 'DK', 'name' => 'Denmark', 'currency' => 'DKK', 'region' => 'Europe'],
            ['code' => 'PL', 'name' => 'Poland', 'currency' => 'PLN', 'region' => 'Europe'],
            ['code' => 'CZ', 'name' => 'Czech Republic', 'currency' => 'CZK', 'region' => 'Europe'],
            ['code' => 'HU', 'name' => 'Hungary', 'currency' => 'HUF', 'region' => 'Europe'],
            ['code' => 'RO', 'name' => 'Romania', 'currency' => 'RON', 'region' => 'Europe'],
            ['code' => 'TR', 'name' => 'Turkey', 'currency' => 'TRY', 'region' => 'Europe'],
            ['code' => 'RU', 'name' => 'Russia', 'currency' => 'RUB', 'region' => 'Europe'],
            ['code' => 'UA', 'name' => 'Ukraine', 'currency' => 'UAH', 'region' => 'Europe'],
            ['code' => 'HR', 'name' => 'Croatia', 'currency' => 'EUR', 'region' => 'Europe'],
            ['code' => 'IS', 'name' => 'Iceland', 'currency' => 'USD', 'region' => 'Europe'],

            // Asia-Pacific - Steam & PS Store
            ['code' => 'JP', 'name' => 'Japan', 'currency' => 'JPY', 'region' => 'Asia-Pacific'],
            ['code' => 'KR', 'name' => 'South Korea', 'currency' => 'KRW', 'region' => 'Asia-Pacific'],
            ['code' => 'CN', 'name' => 'China', 'currency' => 'CNY', 'region' => 'Asia-Pacific'],
            ['code' => 'HK', 'name' => 'Hong Kong', 'currency' => 'HKD', 'region' => 'Asia-Pacific'],
            ['code' => 'TW', 'name' => 'Taiwan', 'currency' => 'TWD', 'region' => 'Asia-Pacific'],
            ['code' => 'SG', 'name' => 'Singapore', 'currency' => 'SGD', 'region' => 'Asia-Pacific'],
            ['code' => 'MY', 'name' => 'Malaysia', 'currency' => 'MYR', 'region' => 'Asia-Pacific'],
            ['code' => 'TH', 'name' => 'Thailand', 'currency' => 'THB', 'region' => 'Asia-Pacific'],
            ['code' => 'ID', 'name' => 'Indonesia', 'currency' => 'IDR', 'region' => 'Asia-Pacific'],
            ['code' => 'PH', 'name' => 'Philippines', 'currency' => 'PHP', 'region' => 'Asia-Pacific'],
            ['code' => 'VN', 'name' => 'Vietnam', 'currency' => 'VND', 'region' => 'Asia-Pacific'],
            ['code' => 'IN', 'name' => 'India', 'currency' => 'INR', 'region' => 'Asia-Pacific'],
            ['code' => 'AU', 'name' => 'Australia', 'currency' => 'AUD', 'region' => 'Asia-Pacific'],
            ['code' => 'NZ', 'name' => 'New Zealand', 'currency' => 'NZD', 'region' => 'Asia-Pacific'],

            // Middle East - PS Store (many use USD but have local stores)
            ['code' => 'AE', 'name' => 'United Arab Emirates', 'currency' => 'AED', 'region' => 'Middle East'],
            ['code' => 'SA', 'name' => 'Saudi Arabia', 'currency' => 'SAR', 'region' => 'Middle East'],
            ['code' => 'QA', 'name' => 'Qatar', 'currency' => 'QAR', 'region' => 'Middle East'],
            ['code' => 'KW', 'name' => 'Kuwait', 'currency' => 'KWD', 'region' => 'Middle East'],
            ['code' => 'BH', 'name' => 'Bahrain', 'currency' => 'BHD', 'region' => 'Middle East'],
            ['code' => 'OM', 'name' => 'Oman', 'currency' => 'USD', 'region' => 'Middle East'],
            ['code' => 'IL', 'name' => 'Israel', 'currency' => 'ILS', 'region' => 'Middle East'],
            ['code' => 'LB', 'name' => 'Lebanon', 'currency' => 'USD', 'region' => 'Middle East'],

            // Africa - Steam & PS Store
            ['code' => 'ZA', 'name' => 'South Africa', 'currency' => 'ZAR', 'region' => 'Africa'],
            ['code' => 'EG', 'name' => 'Egypt', 'currency' => 'EGP', 'region' => 'Africa'],
            ['code' => 'NG', 'name' => 'Nigeria', 'currency' => 'NGN', 'region' => 'Africa'],
            ['code' => 'KE', 'name' => 'Kenya', 'currency' => 'KES', 'region' => 'Africa'],

            // CIS Region - Steam
            ['code' => 'KZ', 'name' => 'Kazakhstan', 'currency' => 'KZT', 'region' => 'CIS'],
        ];

        $inserted = 0;
        $skipped = 0;

        DB::transaction(function () use ($countries, $currencyMap, &$inserted, &$skipped) {
            foreach ($countries as $country) {
                $exists = DB::table('countries')->where('code', $country['code'])->exists();

                if ($exists) {
                    $skipped++;

                    continue;
                }

                if (! isset($currencyMap[$country['currency']])) {
                    $this->command->warn("⚠️  Currency {$country['currency']} not found for {$country['name']} - skipping");

                    continue;
                }

                DB::table('countries')->insert([
                    'code' => $country['code'],
                    'name' => $country['name'],
                    'currency_id' => $currencyMap[$country['currency']],
                    'region' => $country['region'],
                    'metadata' => json_encode([
                        'steam_supported' => true,
                        'psn_supported' => true,
                    ]),
                    'created_at' => now(),
                    'updated_at' => now(),
                ]);

                $inserted++;
            }
        });

        $this->command->info("✅ Added {$inserted} new countries");
        $this->command->info("⏭️  Skipped {$skipped} existing countries");

        // Display summary by region
        $this->command->newLine();
        $this->command->info('Regional breakdown:');

        $regions = DB::table('countries')
            ->select('region', DB::raw('COUNT(*) as count'))
            ->groupBy('region')
            ->orderBy('count', 'desc')
            ->get();

        foreach ($regions as $region) {
            $this->command->line("  • {$region->region}: {$region->count} countries");
        }

        $totalCountries = DB::table('countries')->count();
        $this->command->newLine();
        $this->command->info("📊 Total countries in database: {$totalCountries}");
    }
}
