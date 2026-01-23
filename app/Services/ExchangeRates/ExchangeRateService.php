<?php

declare(strict_types=1);

namespace App\Services\ExchangeRates;

use App\Models\ExchangeRate;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Log;

class ExchangeRateService
{
    public function __construct(
        private BybitClient $bybit,
        private ForexClient $forex,
        private TradingViewClient $tradingView
    ) {}

    /**
     * Refresh BTC rates for common fiat currencies and all currencies in use.
     */
    public function refreshBtcRates(): void
    {
        // Primary list for direct high-precision rates
        $fiats = ['USD', 'EUR', 'GBP', 'AUD', 'CAD', 'BRL', 'JPY'];

        foreach ($fiats as $fiat) {
            try {
                // Try TradingView first for high precision
                $this->tradingView->getBtcRates($fiat);

                // Fallback to Bybit if needed or as verified secondary source
                $rate = $this->bybit->getCryptoToFiatRate('BTC', $fiat);

                $this->storeRate('BTC', $fiat, $rate, 'bybit');
            } catch (\Exception $e) {
                Log::error("Failed to refresh BTC/{$fiat} rate", ['error' => $e->getMessage()]);
            }
        }

        // For other currencies, we'll derive them from BTC/USD and USD/Fiat
        $allCurrencies = DB::table('video_game_prices')->distinct()->pluck('currency')->toArray();
        $btcUsd = $this->getRate('BTC', 'USD');

        if ($btcUsd) {
            $usdRates = $this->forex->getAllRatesForBase('USD');
            foreach ($allCurrencies as $currency) {
                if (in_array($currency, $fiats)) {
                    continue;
                }

                if (isset($usdRates[$currency])) {
                    $derivedRate = $btcUsd * (float) $usdRates[$currency];
                    $this->storeRate('BTC', $currency, $derivedRate, 'derived-via-usd');
                }
            }
        }
    }

    /**
     * Refresh fiat forex rates for all currencies in use.
     */
    public function refreshForexRates(): void
    {
        $allCurrencies = DB::table('video_game_prices')->distinct()->pluck('currency')->toArray();
        $usdRates = $this->forex->getAllRatesForBase('USD');

        foreach ($allCurrencies as $currency) {
            if ($currency === 'USD') {
                continue;
            }

            if (isset($usdRates[$currency])) {
                $this->storeRate('USD', $currency, (float) $usdRates[$currency], 'forex-api');
            }
        }
    }

    /**
     * Get conversion rate from base to quote.
     */
    public function getRate(string $baseCurrency, string $quoteCurrency): ?float
    {
        if ($baseCurrency === $quoteCurrency) {
            return 1.0;
        }

        // Try direct store lookup
        $rate = ExchangeRate::getLatestRate($baseCurrency, $quoteCurrency);

        if ($rate && ! $rate->isStale()) {
            return (float) $rate->rate;
        }

        // Try inverse store lookup
        $inverseRate = ExchangeRate::getLatestRate($quoteCurrency, $baseCurrency);
        if ($inverseRate && ! $inverseRate->isStale()) {
            return 1 / (float) $inverseRate->rate;
        }

        // Try getting it live or derived
        try {
            if ($baseCurrency === 'BTC') {
                return $this->deriveBtcRate($quoteCurrency);
            }

            if ($quoteCurrency === 'BTC') {
                $rate = $this->deriveBtcRate($baseCurrency);

                return $rate ? 1 / $rate : null;
            }

            return $this->forex->getFiatToFiatRate($baseCurrency, $quoteCurrency);
        } catch (\Exception $e) {
            return $rate ? (float) $rate->rate : null;
        }
    }

    /**
     * Derive BTC rate for a currency, possibly via USD.
     */
    private function deriveBtcRate(string $currency): ?float
    {
        try {
            // Try direct Bybit lookup first
            return $this->bybit->getCryptoToFiatRate('BTC', $currency);
        } catch (\Exception $e) {
            // Fallback to BTC -> USD -> Currency
            $btcUsd = $this->getRate('BTC', 'USD');
            $usdTarget = $this->forex->getFiatToFiatRate('USD', $currency);

            if ($btcUsd && $usdTarget) {
                return $btcUsd * $usdTarget;
            }
        }

        return null;
    }

    /**
     * Store a rate in the database.
     */
    private function storeRate(string $base, string $quote, float $rate, string $provider): void
    {
        ExchangeRate::updateOrCreate(
            [
                'base_currency' => $base,
                'quote_currency' => $quote,
                'provider' => $provider,
            ],
            [
                'rate' => $rate,
                'fetched_at' => now(),
            ]
        );
    }
}
