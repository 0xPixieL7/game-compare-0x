<?php

declare(strict_types=1);

namespace App\Jobs\Enrichment\Traits;

/**
 * Trait for guessing currency from region/country codes.
 *
 * Used when APIs don't return currency (e.g., free-to-play games on Steam)
 * but we still need to store a price record with a currency code.
 */
trait GuessesCurrency
{
    /**
     * Guess currency code from a region/country code.
     */
    protected function guessCurrencyFromRegion(string $region): string
    {
        return match (strtoupper($region)) {
            // Europe
            'GB', 'UK' => 'GBP',
            'DE', 'FR', 'ES', 'IT', 'NL', 'BE', 'AT', 'PT', 'IE', 'FI', 'GR' => 'EUR',
            'CH' => 'CHF',
            'PL' => 'PLN',
            'RU' => 'RUB',
            'UA' => 'UAH',
            'SE' => 'SEK',
            'NO' => 'NOK',
            'DK' => 'DKK',
            'CZ' => 'CZK',
            'HU' => 'HUF',
            'RO' => 'RON',
            'TR' => 'TRY',

            // Americas
            'US' => 'USD',
            'CA' => 'CAD',
            'MX' => 'MXN',
            'BR' => 'BRL',
            'AR' => 'ARS',
            'CL' => 'CLP',
            'CO' => 'COP',
            'PE' => 'PEN',

            // Asia Pacific
            'JP' => 'JPY',
            'CN' => 'CNY',
            'KR' => 'KRW',
            'TW' => 'TWD',
            'HK' => 'HKD',
            'SG' => 'SGD',
            'AU' => 'AUD',
            'NZ' => 'NZD',
            'IN' => 'INR',
            'ID' => 'IDR',
            'MY' => 'MYR',
            'TH' => 'THB',
            'PH' => 'PHP',
            'VN' => 'VND',

            // Middle East / Africa
            'IL' => 'ILS',
            'AE' => 'AED',
            'SA' => 'SAR',
            'ZA' => 'ZAR',

            // Default
            default => 'USD',
        };
    }

    /**
     * Get target regions for multi-region pricing.
     *
     * @return array<string>
     */
    protected function getTargetRegions(): array
    {
        return ['US', 'GB', 'DE', 'JP', 'BR', 'CA', 'AU'];
    }
}
