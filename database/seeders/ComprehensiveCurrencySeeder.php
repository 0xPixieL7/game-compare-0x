<?php

namespace Database\Seeders;

use App\Models\Currency;
use Illuminate\Database\Seeder;

class ComprehensiveCurrencySeeder extends Seeder
{
    /**
     * Seed all currencies for international pricing APIs (GG.deals, ITAD, etc.)
     */
    public function run(): void
    {
        $currencies = [
            // Major World Currencies (existing + new)
            ['code' => 'USD', 'name' => 'US Dollar', 'symbol' => '$', 'decimals' => 2, 'is_crypto' => false],
            ['code' => 'EUR', 'name' => 'Euro', 'symbol' => '€', 'decimals' => 2, 'is_crypto' => false],
            ['code' => 'GBP', 'name' => 'British Pound', 'symbol' => '£', 'decimals' => 2, 'is_crypto' => false],
            ['code' => 'JPY', 'name' => 'Japanese Yen', 'symbol' => '¥', 'decimals' => 0, 'is_crypto' => false],
            ['code' => 'AUD', 'name' => 'Australian Dollar', 'symbol' => 'A$', 'decimals' => 2, 'is_crypto' => false],
            ['code' => 'CAD', 'name' => 'Canadian Dollar', 'symbol' => 'C$', 'decimals' => 2, 'is_crypto' => false],
            ['code' => 'CHF', 'name' => 'Swiss Franc', 'symbol' => 'Fr', 'decimals' => 2, 'is_crypto' => false],
            ['code' => 'CNY', 'name' => 'Chinese Yuan', 'symbol' => '¥', 'decimals' => 2, 'is_crypto' => false],
            ['code' => 'BRL', 'name' => 'Brazilian Real', 'symbol' => 'R$', 'decimals' => 2, 'is_crypto' => false],
            ['code' => 'RUB', 'name' => 'Russian Ruble', 'symbol' => '₽', 'decimals' => 2, 'is_crypto' => false],
            ['code' => 'ZAR', 'name' => 'South African Rand', 'symbol' => 'R', 'decimals' => 2, 'is_crypto' => false],
            ['code' => 'NGN', 'name' => 'Nigerian Naira', 'symbol' => '₦', 'decimals' => 2, 'is_crypto' => false],

            // European Currencies (non-Euro)
            ['code' => 'SEK', 'name' => 'Swedish Krona', 'symbol' => 'kr', 'decimals' => 2, 'is_crypto' => false],
            ['code' => 'NOK', 'name' => 'Norwegian Krone', 'symbol' => 'kr', 'decimals' => 2, 'is_crypto' => false],
            ['code' => 'DKK', 'name' => 'Danish Krone', 'symbol' => 'kr', 'decimals' => 2, 'is_crypto' => false],
            ['code' => 'PLN', 'name' => 'Polish Złoty', 'symbol' => 'zł', 'decimals' => 2, 'is_crypto' => false],
            ['code' => 'CZK', 'name' => 'Czech Koruna', 'symbol' => 'Kč', 'decimals' => 2, 'is_crypto' => false],
            ['code' => 'HUF', 'name' => 'Hungarian Forint', 'symbol' => 'Ft', 'decimals' => 0, 'is_crypto' => false],
            ['code' => 'RON', 'name' => 'Romanian Leu', 'symbol' => 'lei', 'decimals' => 2, 'is_crypto' => false],
            ['code' => 'BGN', 'name' => 'Bulgarian Lev', 'symbol' => 'лв', 'decimals' => 2, 'is_crypto' => false],
            ['code' => 'TRY', 'name' => 'Turkish Lira', 'symbol' => '₺', 'decimals' => 2, 'is_crypto' => false],
            ['code' => 'ISK', 'name' => 'Icelandic Króna', 'symbol' => 'kr', 'decimals' => 0, 'is_crypto' => false],

            // Asian & Pacific Currencies
            ['code' => 'KRW', 'name' => 'South Korean Won', 'symbol' => '₩', 'decimals' => 0, 'is_crypto' => false],
            ['code' => 'HKD', 'name' => 'Hong Kong Dollar', 'symbol' => 'HK$', 'decimals' => 2, 'is_crypto' => false],
            ['code' => 'TWD', 'name' => 'Taiwan Dollar', 'symbol' => 'NT$', 'decimals' => 2, 'is_crypto' => false],
            ['code' => 'SGD', 'name' => 'Singapore Dollar', 'symbol' => 'S$', 'decimals' => 2, 'is_crypto' => false],
            ['code' => 'NZD', 'name' => 'New Zealand Dollar', 'symbol' => 'NZ$', 'decimals' => 2, 'is_crypto' => false],
            ['code' => 'INR', 'name' => 'Indian Rupee', 'symbol' => '₹', 'decimals' => 2, 'is_crypto' => false],
            ['code' => 'THB', 'name' => 'Thai Baht', 'symbol' => '฿', 'decimals' => 2, 'is_crypto' => false],
            ['code' => 'PHP', 'name' => 'Philippine Peso', 'symbol' => '₱', 'decimals' => 2, 'is_crypto' => false],
            ['code' => 'IDR', 'name' => 'Indonesian Rupiah', 'symbol' => 'Rp', 'decimals' => 0, 'is_crypto' => false],
            ['code' => 'MYR', 'name' => 'Malaysian Ringgit', 'symbol' => 'RM', 'decimals' => 2, 'is_crypto' => false],

            // Middle East
            ['code' => 'AED', 'name' => 'UAE Dirham', 'symbol' => 'د.إ', 'decimals' => 2, 'is_crypto' => false],
            ['code' => 'SAR', 'name' => 'Saudi Riyal', 'symbol' => '﷼', 'decimals' => 2, 'is_crypto' => false],
            ['code' => 'ILS', 'name' => 'Israeli New Shekel', 'symbol' => '₪', 'decimals' => 2, 'is_crypto' => false],

            // Americas (additional)
            ['code' => 'MXN', 'name' => 'Mexican Peso', 'symbol' => 'Mex$', 'decimals' => 2, 'is_crypto' => false],
            ['code' => 'ARS', 'name' => 'Argentine Peso', 'symbol' => '$', 'decimals' => 2, 'is_crypto' => false],
            ['code' => 'CLP', 'name' => 'Chilean Peso', 'symbol' => '$', 'decimals' => 0, 'is_crypto' => false],
            ['code' => 'COP', 'name' => 'Colombian Peso', 'symbol' => '$', 'decimals' => 2, 'is_crypto' => false],
            ['code' => 'PEN', 'name' => 'Peruvian Sol', 'symbol' => 'S/', 'decimals' => 2, 'is_crypto' => false],

            // Cryptocurrencies (limited to 3-char codes per DB schema, 16 decimals standard)
            ['code' => 'BTC', 'name' => 'Bitcoin', 'symbol' => '₿', 'decimals' => 16, 'is_crypto' => true],
            ['code' => 'ETH', 'name' => 'Ethereum', 'symbol' => 'Ξ', 'decimals' => 16, 'is_crypto' => true],
        ];

        $count = 0;
        $updated = 0;

        foreach ($currencies as $currencyData) {
            $currency = Currency::updateOrCreate(
                ['code' => $currencyData['code']],
                [
                    'name' => $currencyData['name'],
                    'symbol' => $currencyData['symbol'],
                    'decimals' => $currencyData['decimals'],
                    'is_crypto' => $currencyData['is_crypto'],
                ]
            );

            if ($currency->wasRecentlyCreated) {
                $count++;
            } else {
                $updated++;
            }
        }

        $this->command->info("✅ Created {$count} new currencies");
        $this->command->info("✅ Updated {$updated} existing currencies");
        $this->command->info('✅ Total currencies: '.(Currency::count()));
    }
}
