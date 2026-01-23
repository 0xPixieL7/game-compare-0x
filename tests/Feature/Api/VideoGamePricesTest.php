<?php

declare(strict_types=1);

use App\Models\Currency;
use App\Models\User;
use App\Models\VideoGame;
use App\Models\VideoGamePrice;
use App\Models\VideoGameTitle;
use Illuminate\Foundation\Testing\DatabaseMigrations;
use Illuminate\Support\Facades\Queue;
use Illuminate\Support\Str;

uses(DatabaseMigrations::class);

beforeEach(function () {
    Queue::fake();

    Currency::query()->create([
        'code' => 'USD',
        'name' => 'US Dollar',
        'symbol' => '$',
        'decimals' => 2,
        'is_crypto' => false,
    ]);
});

it('lists prices for a game', function () {
    $title = VideoGameTitle::factory()->create();
    $game = VideoGame::withoutEvents(fn () => VideoGame::query()->create([
        'video_game_title_id' => $title->id,
        'slug' => Str::slug('test-game').'-1',
        'provider' => 'test',
        'external_id' => 1,
        'name' => 'Test Game',
    ]));
    $price = VideoGamePrice::factory()->create([
        'video_game_id' => $game->id,
        'retailer' => 'Steam',
        'country_code' => 'US',
        'currency' => 'USD',
        'amount_minor' => 5999,
    ]);

    $res = $this->getJson("/api/games/{$game->id}/prices");
    $res->assertSuccessful();
    $res->assertJsonFragment(['id' => $price->id, 'retailer' => 'Steam']);
});

it('returns latest price for a game', function () {
    $title = VideoGameTitle::factory()->create();
    $game = VideoGame::withoutEvents(fn () => VideoGame::query()->create([
        'video_game_title_id' => $title->id,
        'slug' => Str::slug('test-game').'-2',
        'provider' => 'test',
        'external_id' => 2,
        'name' => 'Test Game 2',
    ]));

    VideoGamePrice::factory()->create([
        'video_game_id' => $game->id,
        'retailer' => 'GOG',
        'country_code' => 'US',
        'currency' => 'USD',
        'amount_minor' => 6999,
        'recorded_at' => now()->subDay(),
    ]);

    $latest = VideoGamePrice::factory()->create([
        'video_game_id' => $game->id,
        'retailer' => 'Steam',
        'country_code' => 'US',
        'currency' => 'USD',
        'amount_minor' => 5999,
        'recorded_at' => now(),
    ]);

    $res = $this->getJson("/api/games/{$game->id}/prices/latest");
    $res->assertSuccessful();
    $res->assertJsonFragment(['id' => $latest->id, 'amount_minor' => 5999]);
});

it('upserts a price point (auth required)', function () {
    $user = User::factory()->create();
    $title = VideoGameTitle::factory()->create();
    $game = VideoGame::withoutEvents(fn () => VideoGame::query()->create([
        'video_game_title_id' => $title->id,
        'slug' => Str::slug('test-game').'-3',
        'provider' => 'test',
        'external_id' => 3,
        'name' => 'Test Game 3',
    ]));

    $payload = [
        'retailer' => 'Steam',
        'country_code' => 'US',
        'currency' => 'USD',
        'amount_minor' => 5999,
    ];

    $res1 = $this->actingAs($user)->postJson("/api/games/{$game->id}/prices", $payload);
    $res1->assertStatus(201);
    expect(VideoGamePrice::query()->where('video_game_id', $game->id)->count())->toBe(1);

    $res2 = $this->actingAs($user)->postJson(
        "/api/games/{$game->id}/prices",
        array_replace($payload, ['amount_minor' => 4999])
    );
    $res2->assertStatus(201);

    expect(VideoGamePrice::query()->where('video_game_id', $game->id)->count())->toBe(1);
    expect(VideoGamePrice::query()->where('video_game_id', $game->id)->value('amount_minor'))->toBe(4999);
});
