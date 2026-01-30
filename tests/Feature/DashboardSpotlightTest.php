<?php

declare(strict_types=1);

use App\Models\User;
use App\Models\VideoGame;
use Illuminate\Support\Facades\Cache;
use Illuminate\Support\Facades\DB;

beforeEach(function () {
    Cache::flush();
});

it('shows trending games in spotlight for guests', function () {
    $response = $this->get('/dashboard');

    $response->assertSuccessful();
    $response->assertInertia(fn ($page) => $page
        ->component('Dashboard/Index')
        ->has('spotlightGames', 12)
        ->where('spotlightGames.0.id', fn ($id) => $id > 0)
    );
});

it('prepends liked games to spotlight for authenticated users', function () {
    $user = User::factory()->create();

    // Get a game from trending list
    $trendingGame = DB::table('video_games_toplists_mv')
        ->where('list_key', 'trending')
        ->where('rating', '>=', 80)
        ->first();

    if (! $trendingGame) {
        $this->markTestSkipped('No trending games available');
    }

    $game = VideoGame::find($trendingGame->id);
    if (! $game) {
        $this->markTestSkipped('Game not found in video_games table');
    }

    // Like the game
    $user->likes()->attach($game->id);

    $response = $this->actingAs($user)->get('/dashboard');

    $response->assertSuccessful();
    $response->assertInertia(fn ($page) => $page
        ->component('Dashboard/Index')
        ->has('spotlightGames', 12)
        ->where('spotlightGames.0.id', $game->id) // First game should be the liked one
    );
});

it('spotlight games have required spotlight format', function () {
    $response = $this->get('/dashboard');

    $response->assertSuccessful();
    $response->assertInertia(fn ($page) => $page
        ->component('Dashboard/Index')
        ->has('spotlightGames.0', fn ($game) => $game
            ->has('id')
            ->has('name')
            ->has('image')
            ->has('background')
            ->has('spotlight_score', fn ($score) => $score
                ->has('total')
                ->has('grade')
                ->has('verdict')
                ->has('breakdown')
            )
            ->has('spotlight_gallery')
        )
    );
});

it('spotlight pulls from top lists when available', function () {
    // Verify we have games in top lists
    $topListCount = DB::table('video_games_toplists_mv')
        ->whereIn('list_key', ['trending', 'upcoming', 'popular', 'top_rated'])
        ->whereNotNull('cover_url')
        ->where('rating', '>=', 80)
        ->count();

    expect($topListCount)->toBeGreaterThan(0);

    $response = $this->get('/dashboard');

    $response->assertSuccessful();
    $response->assertInertia(fn ($page) => $page
        ->component('Dashboard/Index')
        ->has('spotlightGames', fn ($games) => $games->count() === 12)
    );
});
