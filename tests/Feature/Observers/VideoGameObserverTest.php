<?php

declare(strict_types=1);

use App\Jobs\Enrichment\EnrichVideoGameJob;
use App\Models\VideoGame;
use Illuminate\Support\Facades\Http;
use Illuminate\Support\Facades\Queue;

test('VideoGame observer dispatches enrichment job on creation', function () {
    Queue::fake();
    Http::fake();

    $videoGame = VideoGame::factory()->create([
        'name' => 'Test Game',
        'provider' => 'igdb',
        'external_id' => '12345',
    ]);

    Queue::assertPushed(EnrichVideoGameJob::class, function ($job) use ($videoGame) {
        return $job->videoGameId === $videoGame->id;
    });
});

test('VideoGame observer dispatches enrichment job when identity fields change', function () {
    Http::fake();
    Queue::fake(); // Fake queue after HTTP to prevent propagation jobs

    // FIX: Use createQuietly to avoid initial observer dispatch
    $videoGame = VideoGame::factory()->createQuietly([
        'name' => 'Original Name',
        'provider' => 'igdb',
        'external_id' => '12345',
    ]);

    // Update identity field
    $videoGame->update(['name' => 'Updated Name']);

    // Should have 1 job: from update only
    Queue::assertPushed(EnrichVideoGameJob::class, function ($job) use ($videoGame) {
        return $job->videoGameId === $videoGame->id;
    });
    
    expect(Queue::pushed(EnrichVideoGameJob::class)->count())->toBe(1);
});

test('VideoGame observer does not dispatch enrichment job when non-identity fields change', function () {
    Queue::fake();
    Http::fake();

    $videoGame = VideoGame::factory()->createQuietly([
        'name' => 'Test Game',
        'provider' => 'igdb',
        'external_id' => '12345',
    ]);

    $videoGame->update(['rating' => 85.5]);

    Queue::assertNotPushed(EnrichVideoGameJob::class);
});

test('Observer logic does not perform database queries', function () {
    Http::fake();
    Queue::fake();

    // Create game first to get past factory setup
    $videoGame = VideoGame::factory()->createQuietly([
        'name' => 'Test Game',
        'provider' => 'igdb',
        'external_id' => '12345',
    ]);

    // Now enable query log and test observer on update
    DB::enableQueryLog();

    $videoGame->update(['name' => 'Updated Name']);

    $queries = DB::getQueryLog();

    // Filter out the UPDATE query itself (expected)
    $observerQueries = array_filter($queries, function ($query) {
        // Allow the UPDATE query that triggered the observer
        // Disallow any SELECT queries the observer might make
        return stripos($query['query'], 'SELECT') !== false;
    });

    expect($observerQueries)->toBeEmpty();
});
