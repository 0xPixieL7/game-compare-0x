<?php

declare(strict_types=1);

use App\Models\Product;
use App\Models\VideoGame;
use App\Models\VideoGameSource;
use App\Models\VideoGameTitle;
use App\Models\VideoGameTitleSource;

it('loads video games for a product via video_game_titles (no direct FK)', function () {
    $product = Product::factory()->create();

    $source = VideoGameSource::factory()->create([
        'provider' => 'giantbomb',
    ]);

    $title = VideoGameTitle::factory()->create([
        'product_id' => $product->id,
        'name' => 'Test Game',
        'normalized_title' => 'test-game',
        'slug' => 'test-game',
        'providers' => ['giantbomb'],
    ]);

    VideoGameTitleSource::factory()->create([
        'video_game_title_id' => $title->id,
        'video_game_source_id' => $source->id,
        'provider_item_id' => '123',
    ]);

    $game = VideoGame::factory()->create([
        'video_game_title_id' => $title->id,
        'slug' => 'test-game',
        'platform' => ['PC'],
    ]);

    $reloaded = $product->fresh();

    expect($reloaded->videoGames()->pluck('id')->all())
        ->toContain($game->id);
});
