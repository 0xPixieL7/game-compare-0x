<?php

declare(strict_types=1);

namespace Database\Seeders;

use Illuminate\Database\Seeder;
use Illuminate\Support\Facades\DB;

class RetailerCurrenciesSeeder extends Seeder
{
    /**
     * Run the database seeds.
     *
     * Adds currencies from all regions where Steam Store and PlayStation Store operate.
     */
    public function run(): void
    {
        $this->command->info('Adding currencies for Steam Store and PlayStation Store regions...');

        $currencies = [
            // Major currencies (likely already exist, but included for completeness)
            ['code' => 'USD', 'name' => 'United States Dollar', 'symbol' => '$', 'decimals' => 2, 'is_crypto' => false],
            ['code' => 'EUR', 'name' => 'Euro', 'symbol' => '€', 'decimals' => 2, 'is_crypto' => false],
            ['code' => 'GBP', 'name' => 'British Pound Sterling', 'symbol' => '£', 'decimals' => 2, 'is_crypto' => false],
            ['code' => 'JPY', 'name' => 'Japanese Yen', 'symbol' => '¥', 'decimals' => 0, 'is_crypto' => false],
            ['code' => 'AUD', 'name' => 'Australian Dollar', 'symbol' => 'A$', 'decimals' => 2, 'is_crypto' => false],
            ['code' => 'CAD', 'name' => 'Canadian Dollar', 'symbol' => 'C$', 'decimals' => 2, 'is_crypto' => false],
            ['code' => 'BRL', 'name' => 'Brazilian Real', 'symbol' => 'R$', 'decimals' => 2, 'is_crypto' => false],
            ['code' => 'RUB', 'name' => 'Russian Ruble', 'symbol' => '₽', 'decimals' => 2, 'is_crypto' => false],
            ['code' => 'ZAR', 'name' => 'South African Rand', 'symbol' => 'R', 'decimals' => 2, 'is_crypto' => false],

            // Latin America (Steam & PS Store)
            ['code' => 'ARS', 'name' => 'Argentine Peso', 'symbol' => '$', 'decimals' => 2, 'is_crypto' => false],
            ['code' => 'CLP', 'name' => 'Chilean Peso', 'symbol' => '$', 'decimals' => 0, 'is_crypto' => false],
            ['code' => 'COP', 'name' => 'Colombian Peso', 'symbol' => '$', 'decimals' => 2, 'is_crypto' => false],
            ['code' => 'MXN', 'name' => 'Mexican Peso', 'symbol' => '$', 'decimals' => 2, 'is_crypto' => false],
            ['code' => 'PEN', 'name' => 'Peruvian Sol', 'symbol' => 'S/', 'decimals' => 2, 'is_crypto' => false],
            ['code' => 'UYU', 'name' => 'Uruguayan Peso', 'symbol' => '$U', 'decimals' => 2, 'is_crypto' => false],

            // Europe (Steam & PS Store)
            ['code' => 'CHF', 'name' => 'Swiss Franc', 'symbol' => 'CHF', 'decimals' => 2, 'is_crypto' => false],
            ['code' => 'NOK', 'name' => 'Norwegian Krone', 'symbol' => 'kr', 'decimals' => 2, 'is_crypto' => false],
            ['code' => 'SEK', 'name' => 'Swedish Krona', 'symbol' => 'kr', 'decimals' => 2, 'is_crypto' => false],
            ['code' => 'DKK', 'name' => 'Danish Krone', 'symbol' => 'kr', 'decimals' => 2, 'is_crypto' => false],
            ['code' => 'PLN', 'name' => 'Polish Zloty', 'symbol' => 'zł', 'decimals' => 2, 'is_crypto' => false],
            ['code' => 'CZK', 'name' => 'Czech Koruna', 'symbol' => 'Kč', 'decimals' => 2, 'is_crypto' => false],
            ['code' => 'HUF', 'name' => 'Hungarian Forint', 'symbol' => 'Ft', 'decimals' => 0, 'is_crypto' => false],
            ['code' => 'RON', 'name' => 'Romanian Leu', 'symbol' => 'lei', 'decimals' => 2, 'is_crypto' => false],
            ['code' => 'TRY', 'name' => 'Turkish Lira', 'symbol' => '₺', 'decimals' => 2, 'is_crypto' => false],
            ['code' => 'UAH', 'name' => 'Ukrainian Hryvnia', 'symbol' => '₴', 'decimals' => 2, 'is_crypto' => false],

            // Asia-Pacific (Steam & PS Store)
            ['code' => 'KRW', 'name' => 'South Korean Won', 'symbol' => '₩', 'decimals' => 0, 'is_crypto' => false],
            ['code' => 'CNY', 'name' => 'Chinese Yuan Renminbi', 'symbol' => '¥', 'decimals' => 2, 'is_crypto' => false],
            ['code' => 'HKD', 'name' => 'Hong Kong Dollar', 'symbol' => 'HK$', 'decimals' => 2, 'is_crypto' => false],
            ['code' => 'TWD', 'name' => 'New Taiwan Dollar', 'symbol' => 'NT$', 'decimals' => 2, 'is_crypto' => false],
            ['code' => 'SGD', 'name' => 'Singapore Dollar', 'symbol' => 'S$', 'decimals' => 2, 'is_crypto' => false],
            ['code' => 'MYR', 'name' => 'Malaysian Ringgit', 'symbol' => 'RM', 'decimals' => 2, 'is_crypto' => false],
            ['code' => 'THB', 'name' => 'Thai Baht', 'symbol' => '฿', 'decimals' => 2, 'is_crypto' => false],
            ['code' => 'IDR', 'name' => 'Indonesian Rupiah', 'symbol' => 'Rp', 'decimals' => 0, 'is_crypto' => false],
            ['code' => 'PHP', 'name' => 'Philippine Peso', 'symbol' => '₱', 'decimals' => 2, 'is_crypto' => false],
            ['code' => 'VND', 'name' => 'Vietnamese Dong', 'symbol' => '₫', 'decimals' => 0, 'is_crypto' => false],
            ['code' => 'INR', 'name' => 'Indian Rupee', 'symbol' => '₹', 'decimals' => 2, 'is_crypto' => false],
            ['code' => 'NZD', 'name' => 'New Zealand Dollar', 'symbol' => 'NZ$', 'decimals' => 2, 'is_crypto' => false],

            // Middle East (PS Store - often uses USD but local currencies exist)
            ['code' => 'AED', 'name' => 'United Arab Emirates Dirham', 'symbol' => 'د.إ', 'decimals' => 2, 'is_crypto' => false],
            ['code' => 'SAR', 'name' => 'Saudi Riyal', 'symbol' => '﷼', 'decimals' => 2, 'is_crypto' => false],
            ['code' => 'QAR', 'name' => 'Qatari Riyal', 'symbol' => '﷼', 'decimals' => 2, 'is_crypto' => false],
            ['code' => 'KWD', 'name' => 'Kuwaiti Dinar', 'symbol' => 'د.ك', 'decimals' => 3, 'is_crypto' => false],
            ['code' => 'BHD', 'name' => 'Bahraini Dinar', 'symbol' => 'د.ب', 'decimals' => 3, 'is_crypto' => false],
            ['code' => 'ILS', 'name' => 'Israeli New Shekel', 'symbol' => '₪', 'decimals' => 2, 'is_crypto' => false],

            // Africa (Some Steam regions)
            ['code' => 'EGP', 'name' => 'Egyptian Pound', 'symbol' => '£', 'decimals' => 2, 'is_crypto' => false],
            ['code' => 'NGN', 'name' => 'Nigerian Naira', 'symbol' => '₦', 'decimals' => 2, 'is_crypto' => false],
            ['code' => 'KES', 'name' => 'Kenyan Shilling', 'symbol' => 'KSh', 'decimals' => 2, 'is_crypto' => false],

            // CIS Region (Steam)
            ['code' => 'KZT', 'name' => 'Kazakhstani Tenge', 'symbol' => '₸', 'decimals' => 2, 'is_crypto' => false],

            // Cryptocurrency (for BTC normalization)
            ['code' => 'BTC', 'name' => 'Bitcoin', 'symbol' => '₿', 'decimals' => 8, 'is_crypto' => true],
            ['code' => 'ETH', 'name' => 'Ethereum', 'symbol' => 'Ξ', 'decimals' => 18, 'is_crypto' => true],
        ];

        $inserted = 0;
        $skipped = 0;

        DB::transaction(function () use ($currencies, &$inserted, &$skipped) {
            foreach ($currencies as $currency) {
                $exists = DB::table('currencies')->where('code', $currency['code'])->exists();

                if ($exists) {
                    $skipped++;

                    continue;
                }

                DB::table('currencies')->insert([
                    'code' => $currency['code'],
                    'name' => $currency['name'],
                    'symbol' => $currency['symbol'],
                    'decimals' => $currency['decimals'],
                    'is_crypto' => $currency['is_crypto'],
                    'metadata' => null,
                    'created_at' => now(),
                    'updated_at' => now(),
                ]);

                $inserted++;
            }
        });

        $this->command->info("✅ Added {$inserted} new currencies");
        $this->command->info("⏭️  Skipped {$skipped} existing currencies");

        // Display summary
        $this->command->newLine();
        $this->command->info('Currency breakdown:');
        $this->command->line('  • Americas: USD, CAD, BRL, ARS, CLP, COP, MXN, PEN, UYU');
        $this->command->line('  • Europe: EUR, GBP, CHF, NOK, SEK, DKK, PLN, CZK, HUF, RON, TRY, UAH, RUB');
        $this->command->line('  • Asia-Pacific: JPY, KRW, CNY, HKD, TWD, SGD, MYR, THB, IDR, PHP, VND, INR, AUD, NZD');
        $this->command->line('  • Middle East: AED, SAR, QAR, KWD, BHD, ILS');
        $this->command->line('  • Africa: ZAR, EGP, NGN, KES');
        $this->command->line('  • CIS: KZT');
        $this->command->line('  • Crypto: BTC, ETH');

        $totalCurrencies = DB::table('currencies')->count();
        $this->command->newLine();
        $this->command->info("📊 Total currencies in database: {$totalCurrencies}");
    }
}
