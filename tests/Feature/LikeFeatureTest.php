<?php

declare(strict_types=1);

use App\Models\User;
use App\Models\VideoGame;
use Illuminate\Foundation\Testing\RefreshDatabase;

uses(RefreshDatabase::class);

test('authenticated user can like a game', function () {
    $user = User::factory()->create();
    $game = VideoGame::factory()->create();

    $this->actingAs($user)
        ->post("/games/{$game->id}/like")
        ->assertRedirect();

    expect($user->likes()->where('video_game_id', $game->id)->exists())->toBeTrue();
});

test('authenticated user can unlike a game', function () {
    $user = User::factory()->create();
    $game = VideoGame::factory()->create();

    // Like the game first
    $user->likes()->attach($game->id);

    // Unlike it
    $this->actingAs($user)
        ->post("/games/{$game->id}/like")
        ->assertRedirect();

    expect($user->likes()->where('video_game_id', $game->id)->exists())->toBeFalse();
});

test('liking same game twice toggles it', function () {
    $user = User::factory()->create();
    $game = VideoGame::factory()->create();

    // First like
    $this->actingAs($user)->post("/games/{$game->id}/like");
    expect($user->likes()->where('video_game_id', $game->id)->exists())->toBeTrue();

    // Second like (unlike)
    $this->actingAs($user)->post("/games/{$game->id}/like");
    expect($user->likes()->where('video_game_id', $game->id)->exists())->toBeFalse();

    // Third like (like again)
    $this->actingAs($user)->post("/games/{$game->id}/like");
    expect($user->likes()->where('video_game_id', $game->id)->exists())->toBeTrue();
});

test('guest cannot like a game', function () {
    $game = VideoGame::factory()->create();

    $this->post("/games/{$game->id}/like")
        ->assertRedirect('/login');
});

test('game show page includes isLiked prop for authenticated user', function () {
    $user = User::factory()->create();
    $game = VideoGame::factory()->create();

    // Not liked yet
    $response = $this->actingAs($user)->get("/games/{$game->id}");
    expect($response->viewData('page')['props']['game']['isLiked'])->toBeFalse();

    // Like the game
    $user->likes()->attach($game->id);

    // Now it should be liked
    $response = $this->actingAs($user)->get("/games/{$game->id}");
    expect($response->viewData('page')['props']['game']['isLiked'])->toBeTrue();
});

test('liked games appear in dashboard My Likes carousel', function () {
    $user = User::factory()->create();
    $game1 = VideoGame::factory()->create(['name' => 'Liked Game 1']);
    $game2 = VideoGame::factory()->create(['name' => 'Liked Game 2']);

    // Like both games
    $user->likes()->attach([$game1->id, $game2->id]);

    $response = $this->actingAs($user)->get('/dashboard');

    $carouselRows = $response->viewData('page')['props']['carouselRows'];

    // Find "My Likes" row
    $myLikesRow = collect($carouselRows)->firstWhere('title', 'My Likes');

    expect($myLikesRow)->not->toBeNull();
    expect($myLikesRow['games'])->toHaveCount(2);
    expect(collect($myLikesRow['games'])->pluck('name')->toArray())
        ->toContain('Liked Game 1', 'Liked Game 2');
});

test('My Likes row appears first in dashboard carousel', function () {
    $user = User::factory()->create();
    $game = VideoGame::factory()->create();

    $user->likes()->attach($game->id);

    $response = $this->actingAs($user)->get('/dashboard');

    $carouselRows = $response->viewData('page')['props']['carouselRows'];

    // First row should be "My Likes"
    expect($carouselRows[0]['title'])->toBe('My Likes');
});
