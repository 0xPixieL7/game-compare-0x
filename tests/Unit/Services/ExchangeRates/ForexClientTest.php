<?php

declare(strict_types=1);

use App\Services\ExchangeRates\ForexClient;
use Illuminate\Support\Facades\Cache;
use Illuminate\Support\Facades\Http;

beforeEach(function () {
    config(['services.forex.base_url' => 'https://api.exchangerate-api.com']);
    config(['services.forex.timeout' => 10]);
    config(['services.forex.api_key' => 'test-key']);

    Cache::flush();

    $this->client = new ForexClient;
});

it('fetches USD to EUR forex rate', function () {
    Http::fake([
        'api.exchangerate-api.com/v4/latest/USD' => Http::response([
            'base' => 'USD',
            'rates' => [
                'EUR' => 0.92,
                'GBP' => 0.79,
                'JPY' => 149.50,
            ],
        ]),
    ]);

    $rate = $this->client->getFiatToFiatRate('USD', 'EUR');

    expect($rate)->toBe(0.92);
});

it('returns 1.0 for same currency conversion', function () {
    $rate = $this->client->getFiatToFiatRate('USD', 'USD');

    expect($rate)->toBe(1.0);
});

it('fetches EUR to GBP cross rate', function () {
    Http::fake([
        'api.exchangerate-api.com/v4/latest/EUR' => Http::response([
            'base' => 'EUR',
            'rates' => [
                'GBP' => 0.86,
                'USD' => 1.09,
            ],
        ]),
    ]);

    $rate = $this->client->getFiatToFiatRate('EUR', 'GBP');

    expect($rate)->toBe(0.86);
});

it('uses fallback rates when API fails', function () {
    Http::fake([
        'api.exchangerate-api.com/*' => Http::response([], 500),
    ]);

    $rate = $this->client->getFiatToFiatRate('USD', 'EUR');

    // Should return approximate fallback rate
    expect($rate)->toBeGreaterThan(0.5)
        ->and($rate)->toBeLessThan(1.5);
});

it('fetches bulk rates efficiently', function () {
    Http::fake([
        'api.exchangerate-api.com/v4/latest/USD' => Http::response([
            'base' => 'USD',
            'rates' => [
                'EUR' => 0.92,
                'GBP' => 0.79,
                'JPY' => 149.50,
            ],
        ]),
        'api.exchangerate-api.com/v4/latest/EUR' => Http::response([
            'base' => 'EUR',
            'rates' => [
                'GBP' => 0.86,
                'JPY' => 162.50,
            ],
        ]),
    ]);

    $pairs = [
        ['base' => 'USD', 'quote' => 'EUR'],
        ['base' => 'USD', 'quote' => 'GBP'],
        ['base' => 'EUR', 'quote' => 'GBP'],
    ];

    $rates = $this->client->getBulkRates($pairs);

    expect($rates)->toHaveCount(3)
        ->and($rates['USD_EUR'])->toBe(0.92)
        ->and($rates['USD_GBP'])->toBe(0.79)
        ->and($rates['EUR_GBP'])->toBe(0.86);

    Http::assertSentCount(2); // Grouped by base currency
});

it('caches forex rates for 15 minutes', function () {
    Http::fake([
        'api.exchangerate-api.com/*' => Http::response([
            'base' => 'USD',
            'rates' => ['EUR' => 0.92],
        ]),
    ]);

    // First call
    $rate1 = $this->client->getFiatToFiatRate('USD', 'EUR');

    // Second call (should use cache)
    $rate2 = $this->client->getFiatToFiatRate('USD', 'EUR');

    expect($rate1)->toBe($rate2);
    Http::assertSentCount(1); // Only one actual API call
});

it('handles inverse fallback rates', function () {
    Http::fake([
        'api.exchangerate-api.com/*' => Http::response([], 500),
    ]);

    // EUR_USD is the inverse of USD_EUR (which has a fallback)
    $rate = $this->client->getFiatToFiatRate('EUR', 'USD');

    expect($rate)->toBeGreaterThan(1.0); // Inverse of ~0.92
});

it('throws exception when no fallback available', function () {
    Http::fake([
        'api.exchangerate-api.com/*' => Http::response([], 500),
    ]);

    // No fallback for exotic pair
    $this->client->getFiatToFiatRate('NGN', 'ZAR');
})->throws(RuntimeException::class, 'No fallback rate available');

it('performs health check successfully', function () {
    Http::fake([
        'api.exchangerate-api.com/v4/latest/USD' => Http::response([
            'base' => 'USD',
            'rates' => ['EUR' => 0.92],
        ]),
    ]);

    expect($this->client->healthCheck())->toBeTrue();
});
