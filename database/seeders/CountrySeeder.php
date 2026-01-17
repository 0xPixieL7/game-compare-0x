<?php

namespace Database\Seeders;

use App\Models\Country;
use App\Models\Currency;
use App\Models\LocalCurrency;
use Illuminate\Database\Seeder;
use Illuminate\Support\Facades\DB;

class CountrySeeder extends Seeder
{
    public function run(): void
    {
        // If the table was pre-populated with explicit IDs (e.g. CSV import), Postgres
        // sequences can get out of sync and cause duplicate primary key errors on insert.
        if (DB::getDriverName() === 'pgsql') {
            DB::statement(
                "select setval(pg_get_serial_sequence('countries','id'), coalesce((select max(id) from countries), 0) + 1, false)"
            );

            DB::statement(
                "select setval(pg_get_serial_sequence('local_currencies','id'), coalesce((select max(id) from local_currencies), 0) + 1, false)"
            );
        }

        // Keep this list aligned with PS_STORE_REGIONS locale coverage.
        // We seed ISO-ish country/region codes used as `sku_regions.region_code`.
        $countries = [
            // Americas
            ['code' => 'US', 'name' => 'United States', 'currency' => 'USD', 'region' => 'North America'],
            ['code' => 'CA', 'name' => 'Canada', 'currency' => 'CAD', 'region' => 'North America'],
            ['code' => 'BR', 'name' => 'Brazil', 'currency' => 'BRL', 'region' => 'South America'],
            ['code' => 'AR', 'name' => 'Argentina', 'currency' => 'ARS', 'region' => 'South America'],
            ['code' => 'MX', 'name' => 'Mexico', 'currency' => 'MXN', 'region' => 'North America'],

            // Europe
            ['code' => 'GB', 'name' => 'United Kingdom', 'currency' => 'GBP', 'region' => 'Europe'],
            ['code' => 'DE', 'name' => 'Germany', 'currency' => 'EUR', 'region' => 'Europe'],
            ['code' => 'FR', 'name' => 'France', 'currency' => 'EUR', 'region' => 'Europe'],
            ['code' => 'ES', 'name' => 'Spain', 'currency' => 'EUR', 'region' => 'Europe'],
            ['code' => 'IT', 'name' => 'Italy', 'currency' => 'EUR', 'region' => 'Europe'],
            ['code' => 'NL', 'name' => 'Netherlands', 'currency' => 'EUR', 'region' => 'Europe'],
            ['code' => 'PL', 'name' => 'Poland', 'currency' => 'PLN', 'region' => 'Europe'],
            ['code' => 'RU', 'name' => 'Russia', 'currency' => 'RUB', 'region' => 'Europe'],
            ['code' => 'TR', 'name' => 'Turkey', 'currency' => 'TRY', 'region' => 'Europe'],
            ['code' => 'SE', 'name' => 'Sweden', 'currency' => 'SEK', 'region' => 'Europe'],
            ['code' => 'FI', 'name' => 'Finland', 'currency' => 'EUR', 'region' => 'Europe'],
            ['code' => 'DK', 'name' => 'Denmark', 'currency' => 'DKK', 'region' => 'Europe'],
            ['code' => 'NO', 'name' => 'Norway', 'currency' => 'NOK', 'region' => 'Europe'],

            // Asia Pacific
            ['code' => 'JP', 'name' => 'Japan', 'currency' => 'JPY', 'region' => 'Asia Pacific'],
            ['code' => 'KR', 'name' => 'South Korea', 'currency' => 'KRW', 'region' => 'Asia Pacific'],
            ['code' => 'HK', 'name' => 'Hong Kong', 'currency' => 'HKD', 'region' => 'Asia Pacific'],
            ['code' => 'TW', 'name' => 'Taiwan', 'currency' => 'TWD', 'region' => 'Asia Pacific'],
            // Defensive: some provider configs historically used "zh-ZH"; treat it as a distinct region code.
            ['code' => 'ZH', 'name' => 'China (Generic)', 'currency' => 'CNY', 'region' => 'Asia Pacific'],

            // Oceania
            ['code' => 'AU', 'name' => 'Australia', 'currency' => 'AUD', 'region' => 'Oceania'],
            ['code' => 'NZ', 'name' => 'New Zealand', 'currency' => 'NZD', 'region' => 'Oceania'],

            // Africa
            ['code' => 'ZA', 'name' => 'South Africa', 'currency' => 'ZAR', 'region' => 'Africa'],

            // General / multi-country
            ['code' => 'DE', 'name' => 'Germany', 'currency' => 'EUR', 'region' => 'Europe'],
            ['code' => 'ES', 'name' => 'Spain', 'currency' => 'EUR', 'region' => 'Europe'],
            ['code' => 'FR', 'name' => 'France', 'currency' => 'EUR', 'region' => 'Europe'],
            ['code' => 'RU', 'name' => 'Russia', 'currency' => 'RUB', 'region' => 'Europe'],
            ['code' => 'BG', 'name' => 'Bulgaria', 'currency' => 'EUR', 'region' => 'Europe'],
            ['code' => 'GR', 'name' => 'Greece', 'currency' => 'EUR', 'region' => 'Europe'],
            ['code' => 'RO', 'name' => 'Romania', 'currency' => 'EUR', 'region' => 'Europe'],
            ['code' => 'CZ', 'name' => 'Czech Republic', 'currency' => 'CZK', 'region' => 'Europe'],
            ['code' => 'HU', 'name' => 'Hungary', 'currency' => 'HUF', 'region' => 'Europe'],
            ['code' => 'PT', 'name' => 'Portugal', 'currency' => 'EUR', 'region' => 'Europe'],
            ['code' => 'CH', 'name' => 'Switzerland', 'currency' => 'CHF', 'region' => 'Europe'],
        ];

        foreach ($countries as $entry) {
            $currency = Currency::query()->where('code', $entry['currency'])->first();

            if (! $currency) {
                continue;
            }

            $country = Country::query()->firstOrCreate([
                'code' => $entry['code'],
            ], [
                'name' => $entry['name'],
                'currency_id' => $currency->id,
                'region' => $entry['region'],
            ]);

            LocalCurrency::firstOrCreate(
                ['currency_id' => $currency->id, 'code' => $country->code.'_'.$currency->code],
                ['name' => $country->code.' '.$currency->code]
            );
        }
    }
}
