<?php

namespace App\Services;

use Illuminate\Support\Facades\Cache;
use Illuminate\Support\Facades\DB;

class CurrencyCountryService
{
    /**
     * Get currency code for a country code.
     * Uses database tables with caching.
     */
    public function getCurrencyForCountry(string $countryCode): string
    {
        return Cache::remember("currency_for_{$countryCode}", 3600, function () use ($countryCode) {
            // Try to find direct mapping or use common patterns
            $mapping = [
                'US' => 'USD', 'UK' => 'GBP', 'JP' => 'JPY', 'KR' => 'KRW', 
                'BR' => 'BRL', 'CA' => 'CAD', 'AU' => 'AUD', 'NZ' => 'NZD', 
                'RU' => 'RUB', 'IN' => 'INR', 'TR' => 'TRY', 'ZA' => 'ZAR',
                'SG' => 'SGD', 'UA' => 'UAH', 'PL' => 'PLN', 'SE' => 'SEK',
                'CH' => 'CHF', 'SA' => 'SAR', 'AE' => 'AED', 'UY' => 'UYU',
                'AR' => 'ARS', 'EG' => 'EGP', 'MX' => 'MXN', 'NO' => 'NOK',
                'DK' => 'DKK', 'CL' => 'CLP', 'CO' => 'COP', 'TH' => 'THB',
                'VN' => 'VND', 'ID' => 'IDR', 'MY' => 'MYR', 'PH' => 'PHP',
                'HK' => 'HKD', 'TW' => 'TWD', 'KZ' => 'KZT', 'PE' => 'PEN',
                // Eurozone countries
                'FR' => 'EUR', 'DE' => 'EUR', 'IT' => 'EUR', 'ES' => 'EUR',
                'NL' => 'EUR', 'BE' => 'EUR', 'AT' => 'EUR', 'IE' => 'EUR',
                'FI' => 'EUR', 'PT' => 'EUR', 'GR' => 'EUR', 'EE' => 'EUR',
                'LV' => 'EUR', 'LT' => 'EUR', 'SK' => 'EUR', 'SI' => 'EUR',
                'CY' => 'EUR', 'MT' => 'EUR', 'LU' => 'EUR',
            ];

            return $mapping[$countryCode] ?? 'USD';
        });
    }

    /**
     * Get all country-currency mappings.
     */
    public function getAllCountryCurrencyMappings(): array
    {
        return Cache::remember('all_country_currency_mappings', 3600, function () {
            // Get all countries from database
            $countries = DB::table('countries')->get(['code', 'name']);
            
            $mappings = [];
            foreach ($countries as $country) {
                $mappings[$country->code] = [
                    'name' => $country->name,
                    'currency' => $this->getCurrencyForCountry($country->code),
                ];
            }
            
            return $mappings;
        });
    }

    /**
     * Get currency details (symbol, name) from database.
     */
    public function getCurrencyDetails(string $currencyCode): ?array
    {
        return Cache::remember("currency_details_{$currencyCode}", 3600, function () use ($currencyCode) {
            $currency = DB::table('currencies')
                ->where('code', $currencyCode)
                ->first(['code', 'name', 'symbol']);

            if (!$currency) {
                return null;
            }

            return [
                'code' => $currency->code,
                'name' => $currency->name,
                'symbol' => $currency->symbol,
            ];
        });
    }

    /**
     * Format price with currency symbol from database.
     */
    public function formatPrice(int $amountMinor, string $currencyCode): string
    {
        $currency = $this->getCurrencyDetails($currencyCode);
        
        // Zero-decimal currencies
        $zeroDecimal = ['JPY', 'KRW', 'CLP', 'VND', 'XOF', 'XAF'];
        
        if (in_array($currencyCode, $zeroDecimal)) {
            $symbol = $currency['symbol'] ?? $currencyCode;
            return $symbol . ' ' . number_format($amountMinor);
        }

        $major = $amountMinor / 100;
        $symbol = $currency['symbol'] ?? $currencyCode;
        
        return $symbol . number_format($major, 2);
    }

    /**
     * Check if currency exists in database.
     */
    public function currencyExists(string $currencyCode): bool
    {
        return Cache::remember("currency_exists_{$currencyCode}", 3600, function () use ($currencyCode) {
            return DB::table('currencies')->where('code', $currencyCode)->exists();
        });
    }

    /**
     * Get country name from code.
     */
    public function getCountryName(string $countryCode): ?string
    {
        return Cache::remember("country_name_{$countryCode}", 3600, function () use ($countryCode) {
            $country = DB::table('countries')->where('code', $countryCode)->first(['name']);
            return $country?->name;
        });
    }
}
