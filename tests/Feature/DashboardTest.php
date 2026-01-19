<?php

use App\Models\User;

test('guests are redirected to the login page', function () {
    $this->get(route('dashboard'))->assertRedirect(route('login'));
});

test('authenticated users can visit the dashboard', function () {
    $this->actingAs($user = User::factory()->create());

    $this->get(route('dashboard'))->assertOk();
});

test('guests do not receive chart data on dashboard show', function () {
    $game = \App\Models\VideoGame::factory()->create();

    $response = $this->get(route('dashboard.show', ['gameId' => $game->id]));

    $response->assertOk();
    $response->assertInertia(fn (\Inertia\Testing\AssertableInertia $page) => $page
        ->component('Dashboard/Show')
        ->where('priceData', [])
        ->where('availabilityData', [])
    );
});
