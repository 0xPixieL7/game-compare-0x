<?php

namespace Database\Seeders;

use App\Models\Country;
use App\Models\Currency;
use Illuminate\Database\Seeder;

class ComprehensiveCountrySeeder extends Seeder
{
    /**
     * Seed all countries/regions for international pricing APIs
     * Covers: GG.deals, ITAD, PlayStation Store, Xbox Store regions
     */
    public function run(): void
    {
        $countries = [
            // North America
            ['code' => 'US', 'name' => 'United States', 'currency' => 'USD', 'region' => 'North America'],
            ['code' => 'CA', 'name' => 'Canada', 'currency' => 'CAD', 'region' => 'North America'],
            ['code' => 'MX', 'name' => 'Mexico', 'currency' => 'MXN', 'region' => 'North America'],

            // South America
            ['code' => 'BR', 'name' => 'Brazil', 'currency' => 'BRL', 'region' => 'South America'],
            ['code' => 'AR', 'name' => 'Argentina', 'currency' => 'ARS', 'region' => 'South America'],
            ['code' => 'CL', 'name' => 'Chile', 'currency' => 'CLP', 'region' => 'South America'],
            ['code' => 'CO', 'name' => 'Colombia', 'currency' => 'COP', 'region' => 'South America'],
            ['code' => 'PE', 'name' => 'Peru', 'currency' => 'PEN', 'region' => 'South America'],

            // Western Europe (Euro)
            ['code' => 'DE', 'name' => 'Germany', 'currency' => 'EUR', 'region' => 'Europe'],
            ['code' => 'FR', 'name' => 'France', 'currency' => 'EUR', 'region' => 'Europe'],
            ['code' => 'ES', 'name' => 'Spain', 'currency' => 'EUR', 'region' => 'Europe'],
            ['code' => 'IT', 'name' => 'Italy', 'currency' => 'EUR', 'region' => 'Europe'],
            ['code' => 'NL', 'name' => 'Netherlands', 'currency' => 'EUR', 'region' => 'Europe'],
            ['code' => 'BE', 'name' => 'Belgium', 'currency' => 'EUR', 'region' => 'Europe'],
            ['code' => 'AT', 'name' => 'Austria', 'currency' => 'EUR', 'region' => 'Europe'],
            ['code' => 'PT', 'name' => 'Portugal', 'currency' => 'EUR', 'region' => 'Europe'],
            ['code' => 'IE', 'name' => 'Ireland', 'currency' => 'EUR', 'region' => 'Europe'],
            ['code' => 'FI', 'name' => 'Finland', 'currency' => 'EUR', 'region' => 'Europe'],
            ['code' => 'GR', 'name' => 'Greece', 'currency' => 'EUR', 'region' => 'Europe'],
            ['code' => 'LU', 'name' => 'Luxembourg', 'currency' => 'EUR', 'region' => 'Europe'],

            // Western Europe (non-Euro)
            ['code' => 'GB', 'name' => 'United Kingdom', 'currency' => 'GBP', 'region' => 'Europe'],
            ['code' => 'CH', 'name' => 'Switzerland', 'currency' => 'CHF', 'region' => 'Europe'],
            ['code' => 'NO', 'name' => 'Norway', 'currency' => 'NOK', 'region' => 'Europe'],
            ['code' => 'SE', 'name' => 'Sweden', 'currency' => 'SEK', 'region' => 'Europe'],
            ['code' => 'DK', 'name' => 'Denmark', 'currency' => 'DKK', 'region' => 'Europe'],
            ['code' => 'IS', 'name' => 'Iceland', 'currency' => 'ISK', 'region' => 'Europe'],

            // Eastern Europe
            ['code' => 'PL', 'name' => 'Poland', 'currency' => 'PLN', 'region' => 'Europe'],
            ['code' => 'CZ', 'name' => 'Czech Republic', 'currency' => 'CZK', 'region' => 'Europe'],
            ['code' => 'HU', 'name' => 'Hungary', 'currency' => 'HUF', 'region' => 'Europe'],
            ['code' => 'RO', 'name' => 'Romania', 'currency' => 'RON', 'region' => 'Europe'],
            ['code' => 'BG', 'name' => 'Bulgaria', 'currency' => 'BGN', 'region' => 'Europe'],
            ['code' => 'SK', 'name' => 'Slovakia', 'currency' => 'EUR', 'region' => 'Europe'],
            ['code' => 'SI', 'name' => 'Slovenia', 'currency' => 'EUR', 'region' => 'Europe'],
            ['code' => 'HR', 'name' => 'Croatia', 'currency' => 'EUR', 'region' => 'Europe'],
            ['code' => 'EE', 'name' => 'Estonia', 'currency' => 'EUR', 'region' => 'Europe'],
            ['code' => 'LV', 'name' => 'Latvia', 'currency' => 'EUR', 'region' => 'Europe'],
            ['code' => 'LT', 'name' => 'Lithuania', 'currency' => 'EUR', 'region' => 'Europe'],
            ['code' => 'RU', 'name' => 'Russia', 'currency' => 'RUB', 'region' => 'Europe'],
            ['code' => 'TR', 'name' => 'Turkey', 'currency' => 'TRY', 'region' => 'Europe'],
            ['code' => 'UA', 'name' => 'Ukraine', 'currency' => 'EUR', 'region' => 'Europe'],

            // Asia Pacific (East Asia)
            ['code' => 'JP', 'name' => 'Japan', 'currency' => 'JPY', 'region' => 'Asia Pacific'],
            ['code' => 'KR', 'name' => 'South Korea', 'currency' => 'KRW', 'region' => 'Asia Pacific'],
            ['code' => 'CN', 'name' => 'China', 'currency' => 'CNY', 'region' => 'Asia Pacific'],
            ['code' => 'HK', 'name' => 'Hong Kong', 'currency' => 'HKD', 'region' => 'Asia Pacific'],
            ['code' => 'TW', 'name' => 'Taiwan', 'currency' => 'TWD', 'region' => 'Asia Pacific'],
            ['code' => 'MO', 'name' => 'Macau', 'currency' => 'HKD', 'region' => 'Asia Pacific'],

            // Asia Pacific (Southeast Asia)
            ['code' => 'SG', 'name' => 'Singapore', 'currency' => 'SGD', 'region' => 'Asia Pacific'],
            ['code' => 'TH', 'name' => 'Thailand', 'currency' => 'THB', 'region' => 'Asia Pacific'],
            ['code' => 'MY', 'name' => 'Malaysia', 'currency' => 'MYR', 'region' => 'Asia Pacific'],
            ['code' => 'PH', 'name' => 'Philippines', 'currency' => 'PHP', 'region' => 'Asia Pacific'],
            ['code' => 'ID', 'name' => 'Indonesia', 'currency' => 'IDR', 'region' => 'Asia Pacific'],
            ['code' => 'VN', 'name' => 'Vietnam', 'currency' => 'USD', 'region' => 'Asia Pacific'],

            // Asia Pacific (South Asia)
            ['code' => 'IN', 'name' => 'India', 'currency' => 'INR', 'region' => 'Asia Pacific'],

            // Oceania
            ['code' => 'AU', 'name' => 'Australia', 'currency' => 'AUD', 'region' => 'Oceania'],
            ['code' => 'NZ', 'name' => 'New Zealand', 'currency' => 'NZD', 'region' => 'Oceania'],

            // Middle East
            ['code' => 'AE', 'name' => 'United Arab Emirates', 'currency' => 'AED', 'region' => 'Middle East'],
            ['code' => 'SA', 'name' => 'Saudi Arabia', 'currency' => 'SAR', 'region' => 'Middle East'],
            ['code' => 'IL', 'name' => 'Israel', 'currency' => 'ILS', 'region' => 'Middle East'],

            // Africa
            ['code' => 'ZA', 'name' => 'South Africa', 'currency' => 'ZAR', 'region' => 'Africa'],
            ['code' => 'NG', 'name' => 'Nigeria', 'currency' => 'NGN', 'region' => 'Africa'],

            // Special/Legacy codes for provider compatibility
            ['code' => 'ZH', 'name' => 'China (Legacy)', 'currency' => 'CNY', 'region' => 'Asia Pacific'],
            ['code' => 'EU', 'name' => 'European Union (Generic)', 'currency' => 'EUR', 'region' => 'Europe'],
        ];

        $created = 0;
        $updated = 0;
        $skipped = 0;

        foreach ($countries as $entry) {
            $currency = Currency::where('code', $entry['currency'])->first();

            if (! $currency) {
                $this->command->warn("⚠️  Currency {$entry['currency']} not found for {$entry['name']}, skipping...");
                $skipped++;

                continue;
            }

            $country = Country::updateOrCreate(
                ['code' => $entry['code']],
                [
                    'name' => $entry['name'],
                    'currency_id' => $currency->id,
                    'region' => $entry['region'],
                ]
            );

            if ($country->wasRecentlyCreated) {
                $created++;
            } else {
                $updated++;
            }
        }

        $this->command->info("✅ Created {$created} new countries");
        $this->command->info("✅ Updated {$updated} existing countries");
        if ($skipped > 0) {
            $this->command->warn("⚠️  Skipped {$skipped} countries (currency not found)");
        }
        $this->command->info('✅ Total countries: '.(Country::count()));
    }
}
