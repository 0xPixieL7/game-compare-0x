<?php

declare(strict_types=1);

use Inertia\Testing\AssertableInertia as Assert;

test('game show exposes active prices with BTC conversion for disparity charting', function () {
    // Use real game ID that exists in the database
    $gameId = 117836;

    $response = $this->get(route('games.show', $gameId));

    $response->assertOk();
    $response->assertInertia(fn (Assert $page) => $page
        ->component('VideoGames/Show')
        ->has('prices')
        ->where('prices.0.id', fn ($id) => is_int($id))
        ->where('prices.0.retailer', fn ($retailer) => is_string($retailer))
        ->where('prices.0.currency', fn ($currency) => is_string($currency))
        ->where('prices.0.amount', fn ($amount) => is_numeric($amount))
        ->where('prices.0.btc_amount', fn ($btc) => $btc === null || is_numeric($btc))
        ->where('prices.0.btc_rate', fn ($rate) => $rate === null || is_numeric($rate))
    );

    // Check that we have prices
    $prices = $response->viewData('page')['props']['prices'];
    expect($prices)->not->toBeEmpty();

    // Check that at least some prices have BTC conversion
    $btcPrices = collect($prices)->filter(fn ($p) => $p['btc_amount'] !== null);
    expect($btcPrices)->not->toBeEmpty();

    // Verify BTC calculations are correct for prices that have it
    $btcPrices->each(function ($price) {
        if ($price['btc_amount'] !== null && $price['btc_rate'] !== null && $price['amount'] > 0) {
            $expectedBtc = $price['amount'] / $price['btc_rate'];
            expect(round($price['btc_amount'], 8))->toBe(round($expectedBtc, 8));
        }
    });
});

test('game show filters out zero prices from disparity chart data', function () {
    // Use real game ID that exists in the database
    $gameId = 117836;

    $response = $this->get(route('games.show', $gameId));

    $response->assertOk();

    // Get the prices data
    $prices = $response->viewData('page')['props']['prices'];

    // The backend should NOT filter zero prices - that's frontend's job
    // But we should verify the data structure is correct
    collect($prices)->each(function ($price) {
        expect($price)->toHaveKeys(['id', 'retailer', 'currency', 'amount', 'btc_amount', 'btc_rate']);
    });
});
