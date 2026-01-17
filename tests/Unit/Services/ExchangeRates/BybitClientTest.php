<?php

declare(strict_types=1);

use App\Services\ExchangeRates\BybitClient;
use Illuminate\Support\Facades\Cache;
use Illuminate\Support\Facades\Http;

beforeEach(function () {
    config(['services.bybit.base_url' => 'https://api.bybit.com']);
    config(['services.bybit.timeout' => 10]);

    Cache::flush();

    $this->client = new BybitClient;
});

it('fetches BTC to USD rate from Bybit', function () {
    Http::fake([
        'api.bybit.com/v5/market/tickers*' => Http::response([
            'retCode' => 0,
            'retMsg' => 'OK',
            'result' => [
                'list' => [
                    [
                        'symbol' => 'BTCUSDT',
                        'lastPrice' => '45230.50',
                        'bid1Price' => '45230.00',
                        'ask1Price' => '45231.00',
                    ],
                ],
            ],
        ]),
    ]);

    $rate = $this->client->getCryptoToFiatRate('BTC', 'USD');

    expect($rate)->toBe(45230.5);
});

it('fetches BTC to EUR rate from Bybit', function () {
    Http::fake([
        'api.bybit.com/v5/market/tickers*' => Http::response([
            'retCode' => 0,
            'retMsg' => 'OK',
            'result' => [
                'list' => [
                    [
                        'symbol' => 'BTCUSDT',
                        'lastPrice' => '41612.06',
                    ],
                ],
            ],
        ]),
    ]);

    $rate = $this->client->getCryptoToFiatRate('BTC', 'EUR');

    expect($rate)->toBe(41612.06);
});

it('handles API errors gracefully', function () {
    Http::fake([
        'api.bybit.com/*' => Http::response([
            'retCode' => 10001,
            'retMsg' => 'Invalid symbol',
        ], 400),
    ]);

    $this->client->getCryptoToFiatRate('BTC', 'USD');
})->throws(RuntimeException::class, 'Bybit API error');

it('normalizes symbols correctly', function () {
    Http::fake([
        'api.bybit.com/v5/market/tickers*' => Http::response([
            'retCode' => 0,
            'result' => ['list' => [['symbol' => 'BTCUSDT', 'lastPrice' => '45000']]],
        ]),
    ]);

    // All these fiat currencies should map to USDT pairs
    foreach (['USD', 'EUR', 'GBP', 'JPY', 'AUD', 'CAD'] as $fiat) {
        $rate = $this->client->getCryptoToFiatRate('BTC', $fiat);
        expect($rate)->toBeGreaterThan(0);
    }

    Http::assertSentCount(6);
});

it('fetches bulk rates efficiently', function () {
    Http::fake([
        'api.bybit.com/*' => Http::response([
            'retCode' => 0,
            'result' => ['list' => [['symbol' => 'BTCUSDT', 'lastPrice' => '45000']]],
        ]),
    ]);

    $pairs = [
        ['crypto' => 'BTC', 'fiat' => 'USD'],
        ['crypto' => 'BTC', 'fiat' => 'EUR'],
        ['crypto' => 'BTC', 'fiat' => 'GBP'],
    ];

    $rates = $this->client->getBulkRates($pairs);

    expect($rates)->toHaveCount(3)
        ->and($rates['BTC_USD'])->toBe(45000.0)
        ->and($rates['BTC_EUR'])->toBe(45000.0)
        ->and($rates['BTC_GBP'])->toBe(45000.0);
});

it('caches rates for 5 minutes', function () {
    Http::fake([
        'api.bybit.com/*' => Http::response([
            'retCode' => 0,
            'result' => ['list' => [['symbol' => 'BTCUSDT', 'lastPrice' => '45000']]],
        ]),
    ]);

    // First call
    $rate1 = $this->client->getCryptoToFiatRate('BTC', 'USD');

    // Second call (should use cache)
    $rate2 = $this->client->getCryptoToFiatRate('BTC', 'USD');

    expect($rate1)->toBe($rate2);
    Http::assertSentCount(1); // Only one actual API call
});

it('performs health check successfully', function () {
    Http::fake([
        'api.bybit.com/v5/market/time*' => Http::response(['retCode' => 0]),
    ]);

    expect($this->client->healthCheck())->toBeTrue();
});

it('health check fails when API is down', function () {
    Http::fake([
        'api.bybit.com/*' => Http::response([], 500),
    ]);

    expect($this->client->healthCheck())->toBeFalse();
});
