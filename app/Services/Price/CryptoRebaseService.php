<?php

declare(strict_types=1);

namespace App\Services\Price;

use App\Models\VideoGamePrice;
use App\Services\ExchangeRates\ExchangeRateService;
use Illuminate\Support\Facades\Log;

class CryptoRebaseService
{
    public function __construct(
        private ExchangeRateService $exchangeRates
    ) {}

    /**
     * Rebase a single price record to BTC.
     */
    public function rebasePrice(VideoGamePrice $price, ?float $rate = null): void
    {
        $currency = $price->currency;
        $amount = $price->amount_minor / 100;

        // Get BTC price in the local currency
        $btcInLocal = $rate ?? $this->exchangeRates->getRate('BTC', $currency);
        
        if (!$btcInLocal || $btcInLocal <= 0) {
            Log::warning("Cannot rebase price: No BTC rate for {$currency}");
            return;
        }

        $amountBtc = $amount / $btcInLocal;
        $sats = (int) round($amountBtc * 100_000_000);

        $price->update([
            'amount_btc' => $amountBtc,
            'btc_value_sats' => $sats,
            'metadata' => array_merge($price->metadata ?? [], [
                'rebased_at' => now()->toIso8601String(),
                'btc_rate_used' => $btcInLocal,
                'rebased_by' => 'CryptoRebaseService',
            ]),
        ]);
    }

    /**
     * Rebase all active prices with optimization.
     */
    public function rebaseAllActivePrices(): int
    {
        $prices = VideoGamePrice::where('is_active', true)->get();
        $currencies = $prices->pluck('currency')->unique();
        
        // Pre-fetch all rates
        $rates = [];
        foreach ($currencies as $currency) {
            $rates[$currency] = $this->exchangeRates->getRate('BTC', $currency);
        }

        $count = 0;
        foreach ($prices as $price) {
            try {
                $currency = $price->currency;
                if (!isset($rates[$currency]) || !$rates[$currency]) {
                    continue;
                }

                $this->rebasePrice($price, $rates[$currency]);
                $count++;
            } catch (\Exception $e) {
                Log::error("Failed to rebase price id {$price->id}", ['error' => $e->getMessage()]);
            }
        }

        return $count;
    }
}
