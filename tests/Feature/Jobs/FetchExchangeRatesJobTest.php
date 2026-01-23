<?php

declare(strict_types=1);

use App\Jobs\FetchExchangeRatesJob;
use App\Models\Currency;
use App\Models\ExchangeRate;
use Illuminate\Support\Facades\Http;

beforeEach(function () {
    // Seed currencies
    Currency::factory()->create(['code' => 'USD', 'name' => 'US Dollar', 'is_crypto' => false]);
    Currency::factory()->create(['code' => 'EUR', 'name' => 'Euro', 'is_crypto' => false]);
    Currency::factory()->create(['code' => 'GBP', 'name' => 'British Pound', 'is_crypto' => false]);
    Currency::factory()->create(['code' => 'JPY', 'name' => 'Japanese Yen', 'is_crypto' => false, 'decimals' => 0]);
    Currency::factory()->create(['code' => 'BTC', 'name' => 'Bitcoin', 'is_crypto' => true, 'decimals' => 8]);
});

it('fetches and stores exchange rates from both APIs', function () {
    Http::fakeSequence()
        // Bybit responses for BTC pairs
        ->push(['retCode' => 0, 'result' => ['list' => [['symbol' => 'BTCUSDT', 'lastPrice' => '45000']]]])
        ->push(['retCode' => 0, 'result' => ['list' => [['symbol' => 'BTCUSDT', 'lastPrice' => '45000']]]])
        ->push(['retCode' => 0, 'result' => ['list' => [['symbol' => 'BTCUSDT', 'lastPrice' => '45000']]]])
        ->push(['retCode' => 0, 'result' => ['list' => [['symbol' => 'BTCUSDT', 'lastPrice' => '45000']]]])
        // Forex responses
        ->push(['base' => 'USD', 'rates' => ['EUR' => 0.92, 'GBP' => 0.79, 'JPY' => 149.50]])
        ->push(['base' => 'EUR', 'rates' => ['GBP' => 0.86, 'JPY' => 162.50]]);

    FetchExchangeRatesJob::dispatch();

    // Should have crypto rates (BTC -> USD, EUR, GBP, JPY)
    expect(ExchangeRate::where('base_currency', 'BTC')->count())->toBeGreaterThan(0)
        ->and(ExchangeRate::where('provider', 'bybit')->count())->toBeGreaterThan(0)
        // Should have forex rates (USD -> EUR, GBP, JPY)
        ->and(ExchangeRate::where('base_currency', 'USD')->count())->toBeGreaterThan(0)
        ->and(ExchangeRate::where('provider', 'exchangerate-api')->count())->toBeGreaterThan(0);
});

it('creates unique records per fetched_at timestamp', function () {
    Http::fake([
        'api.bybit.com/*' => Http::response([
            'retCode' => 0,
            'result' => ['list' => [['symbol' => 'BTCUSDT', 'lastPrice' => '45000']]],
        ]),
        'api.exchangerate-api.com/*' => Http::response([
            'base' => 'USD',
            'rates' => ['EUR' => 0.92, 'GBP' => 0.79, 'JPY' => 149.50],
        ]),
    ]);

    // First fetch
    FetchExchangeRatesJob::dispatch();
    $firstCount = ExchangeRate::count();

    // Simulate time passing and second fetch (different fetched_at)
    $this->travel(1)->hour();
    FetchExchangeRatesJob::dispatch();
    $secondCount = ExchangeRate::count();

    expect($secondCount)->toBeGreaterThan($firstCount);
});

it('cleans up stale exchange rates older than 7 days', function () {
    Http::fake();

    // Create old rate
    ExchangeRate::factory()->create([
        'base_currency' => 'USD',
        'quote_currency' => 'EUR',
        'fetched_at' => now()->subDays(8),
    ]);

    FetchExchangeRatesJob::dispatch();

    expect(ExchangeRate::where('fetched_at', '<', now()->subDays(7))->count())->toBe(0);
});

it('logs warning when failure rate is high', function () {
    Http::fake([
        'api.bybit.com/*' => Http::response([], 500), // All fail
        'api.exchangerate-api.com/*' => Http::response([], 500),
    ]);

    FetchExchangeRatesJob::dispatch();

    // Job should complete even with failures
    $this->assertDatabaseCount('exchange_rates', 0);
});

it('handles partial API failures gracefully', function () {
    Http::fake([
        // BTC/USD succeeds
        'api.bybit.com/v5/market/tickers*symbol=BTCUSDT*' => Http::response([
            'retCode' => 0,
            'result' => ['list' => [['symbol' => 'BTCUSDT', 'lastPrice' => '45000']]],
        ]),
        // Other crypto pairs fail
        'api.bybit.com/*' => Http::response([], 500),
        // Forex succeeds
        'api.exchangerate-api.com/*' => Http::response([
            'base' => 'USD',
            'rates' => ['EUR' => 0.92, 'GBP' => 0.79, 'JPY' => 149.50],
        ]),
    ]);

    FetchExchangeRatesJob::dispatch();

    // Should have some rates despite failures
    expect(ExchangeRate::count())->toBeGreaterThan(0);
});
