<?php

declare(strict_types=1);

namespace App\Services\ExchangeRates;

use App\Models\ExchangeRate;
use Illuminate\Contracts\Cache\Repository as CacheRepository;
use Illuminate\Http\Client\PendingRequest;
use Illuminate\Support\Facades\Cache;
use Illuminate\Support\Facades\Http;
use Illuminate\Support\Facades\Log;
use Illuminate\Support\Str;

class TradingViewClient
{
    private const DEFAULT_BASE_URL = 'https://scanner.tradingview.com';

    private const DEFAULT_CACHE_TTL = 600;

    private const DEFAULT_TIMEOUT = 10;

    private PendingRequest $client;

    private ?string $sessionId;

    private ?string $sessionIdSign;

    private int $cacheTtl;

    public function __construct()
    {
        $this->sessionId = config('services.tradingview.session_id');
        $this->sessionIdSign = config('services.tradingview.session_id_sign');
        $this->cacheTtl = (int) config('services.tradingview.cache_ttl', self::DEFAULT_CACHE_TTL);

        $this->client = Http::baseUrl(config('services.tradingview.base_url', self::DEFAULT_BASE_URL))
            ->timeout((int) config('services.tradingview.timeout', self::DEFAULT_TIMEOUT))
            ->withHeaders([
                'accept' => 'text/plain, */*; q=0.01',
                'accept-language' => 'en-US,en;q=0.9',
                'content-type' => 'application/json',
                'origin' => 'https://www.tradingview.com',
                'referer' => 'https://www.tradingview.com/',
                'user-agent' => 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            ]);
    }

    /**
     * @return array{currency: string, requested_currency: string, fallback: bool, rates: array<int, array{symbol: string, exchange: string, close: float, volume_quote: float, currency: string}>}
     */
    public function getBtcRates(string $currency, bool $allowFetch = true): array
    {
        $normalizedCurrency = strtoupper($currency);
        $cacheKey = "tradingview:btc:{$normalizedCurrency}";
        $cacheStore = $this->cacheStore();
        $cached = $cacheStore->get($cacheKey);

        if (is_array($cached)) {
            return $cached;
        }

        $stored = $this->getStoredRate($normalizedCurrency);
        $fallback = false;
        $requestedCurrency = $normalizedCurrency;

        if (! $stored) {
            $stored = $this->getStoredRate('USD');
            $fallback = $stored !== null;
            $normalizedCurrency = $stored ? 'USD' : $normalizedCurrency;
        }

        if ($stored) {
            $result = $this->buildResultFromStoredRate($stored, $requestedCurrency, $fallback);
            $cacheStore->put($cacheKey, $result, $this->cacheTtl);

            if (! $allowFetch) {
                return $result;
            }
        } elseif (! $allowFetch) {
            return $this->emptyResult($requestedCurrency);
        }

        $result = $this->fetchRates($requestedCurrency);

        if ($result['rates'] === []) {
            $result = $this->fetchRates('USD');
            $result['fallback'] = true;
            $result['requested_currency'] = $requestedCurrency;
        }

        $this->storeMostLiquidRate($result);
        $cacheStore->put($cacheKey, $result, $this->cacheTtl);

        return $result;
    }

    /**
     * @return array{currency: string, requested_currency: string, fallback: bool, rates: array<int, array{symbol: string, exchange: string, close: float, volume_quote: float, currency: string}>}
     */
    private function fetchRates(string $currency): array
    {
        $payload = $this->buildPayload($currency);

        try {
            $response = $this->client
                ->withHeaders($this->cookieHeaders())
                ->post('/crypto/scan', $payload);

            if (! $response->successful()) {
                throw new \RuntimeException("TradingView API error: {$response->status()}");
            }

            $data = $response->json();
            $rows = $data['data'] ?? [];

            $rates = $this->extractRates($rows, $currency);

            return [
                'currency' => $currency,
                'requested_currency' => $currency,
                'fallback' => false,
                'rates' => $rates,
            ];
        } catch (\Throwable $e) {
            Log::warning('TradingView rate fetch failed', [
                'currency' => $currency,
                'error' => $e->getMessage(),
            ]);

            return [
                'currency' => $currency,
                'requested_currency' => $currency,
                'fallback' => false,
                'rates' => [],
            ];
        }
    }

    /**
     * @param  array<int, array{s?: string, d?: array<int, mixed>}>  $rows
     * @return array<int, array{symbol: string, exchange: string, close: float, volume_quote: float, currency: string}>
     */
    private function extractRates(array $rows, string $currency): array
    {
        $rates = [];

        foreach ($rows as $row) {
            $ticker = (string) ($row['s'] ?? '');
            $values = $row['d'] ?? [];

            if (! $ticker || ! isset($values[0], $values[1], $values[3])) {
                continue;
            }

            $symbol = Str::after($ticker, ':');
            $exchange = Str::before($ticker, ':');
            $quoteCurrency = strtoupper((string) $values[1]);

            if (! Str::startsWith($symbol, 'BTC') || ! Str::endsWith($symbol, $currency)) {
                continue;
            }

            if ($quoteCurrency !== $currency) {
                continue;
            }

            $rates[] = [
                'symbol' => $symbol,
                'exchange' => $exchange,
                'close' => (float) $values[0],
                'volume_quote' => (float) $values[3],
                'currency' => $quoteCurrency,
            ];
        }

        usort($rates, fn ($a, $b) => $b['volume_quote'] <=> $a['volume_quote']);

        return array_slice($rates, 0, 3);
    }

    /**
     * @param  array{currency: string, requested_currency: string, fallback: bool, rates: array<int, array{symbol: string, exchange: string, close: float, volume_quote: float, currency: string}>}  $result
     */
    private function storeMostLiquidRate(array $result): void
    {
        if ($result['rates'] === []) {
            return;
        }

        $topRate = $result['rates'][0];

        ExchangeRate::updateOrCreate(
            [
                'base_currency' => 'BTC',
                'quote_currency' => $result['currency'],
                'provider' => 'tradingview',
            ],
            [
                'rate' => $topRate['close'],
                'fetched_at' => now(),
                'metadata' => [
                    'exchange' => $topRate['exchange'],
                    'symbol' => $topRate['symbol'],
                    'volume_quote' => $topRate['volume_quote'],
                    'fallback' => $result['fallback'],
                    'requested_currency' => $result['requested_currency'],
                    'top_rates' => $result['rates'],
                ],
            ]
        );
    }

    private function getStoredRate(string $currency): ?ExchangeRate
    {
        return ExchangeRate::query()
            ->where('base_currency', 'BTC')
            ->where('quote_currency', $currency)
            ->orderByRaw("provider = 'tradingview' desc")
            ->orderByDesc('fetched_at')
            ->first();
    }

    /**
     * @return array{currency: string, requested_currency: string, fallback: bool, rates: array<int, array{symbol: string, exchange: string, close: float, volume_quote: float, currency: string}>}
     */
    private function buildResultFromStoredRate(ExchangeRate $stored, string $requestedCurrency, bool $fallback): array
    {
        $topRates = $stored->metadata['top_rates'] ?? [];
        $rates = [];

        foreach ($topRates as $rate) {
            if (! isset($rate['symbol'], $rate['exchange'], $rate['close'], $rate['volume_quote'], $rate['currency'])) {
                continue;
            }

            $rates[] = [
                'symbol' => (string) $rate['symbol'],
                'exchange' => (string) $rate['exchange'],
                'close' => (float) $rate['close'],
                'volume_quote' => (float) $rate['volume_quote'],
                'currency' => (string) $rate['currency'],
            ];
        }

        if ($rates === []) {
            $rates = [[
                'symbol' => $stored->metadata['symbol'] ?? 'BTC'.$stored->quote_currency,
                'exchange' => $stored->metadata['exchange'] ?? 'tradingview',
                'close' => (float) $stored->rate,
                'volume_quote' => (float) ($stored->metadata['volume_quote'] ?? 0),
                'currency' => $stored->quote_currency,
            ]];
        }

        return [
            'currency' => $stored->quote_currency,
            'requested_currency' => $requestedCurrency,
            'fallback' => $fallback,
            'rates' => $rates,
        ];
    }

    /**
     * @return array{currency: string, requested_currency: string, fallback: bool, rates: array<int, array{symbol: string, exchange: string, close: float, volume_quote: float, currency: string}>}
     */
    private function emptyResult(string $requestedCurrency): array
    {
        return [
            'currency' => $requestedCurrency,
            'requested_currency' => $requestedCurrency,
            'fallback' => true,
            'rates' => [],
        ];
    }

    private function buildPayload(string $currency): array
    {
        return [
            'markets' => ['crypto'],
            'symbols' => [
                'query' => ['types' => []],
                'tickers' => [],
            ],
            'options' => ['lang' => 'en'],
            'columns' => ['close', 'currency', 'volume', 'volume_quote'],
            'filter' => [
                ['left' => 'currency', 'operation' => 'equal', 'right' => $currency],
                ['left' => 'type', 'operation' => 'equal', 'right' => 'crypto'],
            ],
            'sort' => ['sortBy' => 'volume_quote', 'sortOrder' => 'desc'],
            'range' => [0, 50],
        ];
    }

    private function cookieHeaders(): array
    {
        $cookies = [];

        if ($this->sessionId) {
            $cookies[] = "sessionid={$this->sessionId}";
        }

        if ($this->sessionIdSign) {
            $cookies[] = "sessionid_sign={$this->sessionIdSign}";
        }

        return $cookies === [] ? [] : ['Cookie' => implode('; ', $cookies)];
    }

    private function cacheStore(): CacheRepository
    {
        if (config('cache.default') === 'redis') {
            return Cache::store('redis');
        }

        return Cache::store();
    }
}
