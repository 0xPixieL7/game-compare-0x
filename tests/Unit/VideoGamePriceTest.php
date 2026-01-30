<?php

declare(strict_types=1);

use App\Models\VideoGame;
use App\Models\VideoGamePrice;
use Illuminate\Foundation\Testing\RefreshDatabase;

uses(RefreshDatabase::class);

test('global scope excludes prices with zero amount', function () {
    $game = VideoGame::factory()->create();

    // Create prices with different amounts
    VideoGamePrice::factory()->create([
        'video_game_id' => $game->id,
        'amount_minor' => 0, // Should be excluded
        'is_active' => true,
    ]);

    VideoGamePrice::factory()->create([
        'video_game_id' => $game->id,
        'amount_minor' => 5999, // $59.99
        'is_active' => true,
    ]);

    VideoGamePrice::factory()->create([
        'video_game_id' => $game->id,
        'amount_minor' => 4999, // $49.99
        'is_active' => true,
    ]);

    // Query should only return non-zero prices
    $prices = VideoGamePrice::where('video_game_id', $game->id)->get();

    expect($prices)->toHaveCount(2);
    expect($prices->pluck('amount_minor')->toArray())->not->toContain(0);
});

test('can bypass global scope to get zero prices if needed', function () {
    $game = VideoGame::factory()->create();

    VideoGamePrice::factory()->create([
        'video_game_id' => $game->id,
        'amount_minor' => 0,
        'is_active' => true,
    ]);

    VideoGamePrice::factory()->create([
        'video_game_id' => $game->id,
        'amount_minor' => 5999,
        'is_active' => true,
    ]);

    // Using withoutGlobalScope should return all prices including zero
    $allPrices = VideoGamePrice::withoutGlobalScope('nonZero')
        ->where('video_game_id', $game->id)
        ->get();

    expect($allPrices)->toHaveCount(2);
    expect($allPrices->pluck('amount_minor')->toArray())->toContain(0);
});

test('show page only displays non-zero prices', function () {
    $game = VideoGame::factory()->create();

    // Create mix of zero and non-zero prices
    VideoGamePrice::factory()->create([
        'video_game_id' => $game->id,
        'amount_minor' => 0,
        'is_active' => true,
    ]);

    VideoGamePrice::factory()->create([
        'video_game_id' => $game->id,
        'amount_minor' => 5999,
        'is_active' => true,
        'retailer' => 'Steam',
    ]);

    $response = $this->get("/games/{$game->id}");

    $prices = $response->viewData('page')['props']['prices'];

    expect($prices)->toHaveCount(1);
    expect($prices[0]['amount'])->toBe(59.99);
    expect($prices[0]['retailer'])->toBe('Steam');
});
