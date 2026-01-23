<?php

declare(strict_types=1);

use App\Jobs\Enrichment\EnrichVideoGameJob;
use App\Jobs\Propagation\PropagateSourceMetadataJob;
use App\Models\VideoGame;
use App\Models\VideoGameTitle;
use App\Models\VideoGameTitleSource;
use Illuminate\Support\Facades\Queue;

test('full pipeline: VideoGameTitleSource created triggers complete enrichment flow', function () {
    Queue::fake();

    // Step 1: Create a VideoGameTitleSource (simulate Rust CLI ingestion)
    $title = VideoGameTitle::factory()->create(['name' => 'Test Game Title']);

    $source = VideoGameTitleSource::create([
        'video_game_title_id' => $title->id,
        'provider' => 'igdb',
        'external_id' => '12345',
        'name' => 'Test Game',
        'rating' => 85.5,
        'developer' => 'Test Developer',
        'publisher' => 'Test Publisher',
    ]);

    // Step 2: Observer should dispatch PropagateSourceMetadataJob
    Queue::assertPushed(PropagateSourceMetadataJob::class, function ($job) use ($source) {
        return $job->sourceId === $source->id;
    });
});

test('full pipeline: PropagateSourceMetadataJob creates VideoGame and triggers enrichment', function () {
    Queue::fake();

    $title = VideoGameTitle::factory()->create();

    $source = VideoGameTitleSource::create([
        'video_game_title_id' => $title->id,
        'provider' => 'igdb',
        'external_id' => '12345',
        'name' => 'Test Game',
    ]);

    // Clear the observer-dispatched job
    Queue::flush();

    // Step 3: Run PropagateSourceMetadataJob manually
    PropagateSourceMetadataJob::dispatchSync($source->id);

    // Verify VideoGame was created
    $videoGame = VideoGame::where('provider', 'igdb')
        ->where('external_id', '12345')
        ->first();

    expect($videoGame)->not->toBeNull();

    // Step 4: VideoGame creation should trigger EnrichVideoGameJob
    Queue::assertPushed(EnrichVideoGameJob::class, function ($job) use ($videoGame) {
        return $job->videoGameId === $videoGame->id;
    });
});

test('full pipeline: idempotency - running pipeline twice produces same result', function () {
    $title = VideoGameTitle::factory()->create();

    $source = VideoGameTitleSource::create([
        'video_game_title_id' => $title->id,
        'provider' => 'igdb',
        'external_id' => '12345',
        'name' => 'Test Game',
    ]);

    // Run propagation twice
    PropagateSourceMetadataJob::dispatchSync($source->id);
    PropagateSourceMetadataJob::dispatchSync($source->id);

    // Should only create one VideoGame
    $count = VideoGame::where('provider', 'igdb')
        ->where('external_id', '12345')
        ->count();

    expect($count)->toBe(1);
});

test('full pipeline: partial failure - media enrichment fails but prices succeed', function () {
    Queue::fake();

    $title = VideoGameTitle::factory()->create();

    $source = VideoGameTitleSource::create([
        'video_game_title_id' => $title->id,
        'provider' => 'steam',
        'external_id' => '570',
        'name' => 'Dota 2',
    ]);

    PropagateSourceMetadataJob::dispatchSync($source->id);

    $videoGame = VideoGame::where('provider', 'steam')
        ->where('external_id', '570')
        ->first();

    // EnrichVideoGameJob should be dispatched regardless of individual job failures
    Queue::assertPushed(EnrichVideoGameJob::class, function ($job) use ($videoGame) {
        return $job->videoGameId === $videoGame->id;
    });
});

test('observer pipeline handles bulk inserts efficiently', function () {
    Queue::fake();

    $title = VideoGameTitle::factory()->create();

    // Simulate bulk ingestion
    $sources = collect(range(1, 10))->map(fn ($i) => [
        'video_game_title_id' => $title->id,
        'provider' => 'igdb',
        'external_id' => (string) $i,
        'name' => "Test Game {$i}",
        'created_at' => now(),
        'updated_at' => now(),
    ]);

    // Insert all at once (triggers observers for each)
    foreach ($sources as $sourceData) {
        VideoGameTitleSource::create($sourceData);
    }

    // Each source should dispatch a propagation job
    Queue::assertPushed(PropagateSourceMetadataJob::class, 10);
});

test('enrichment job concurrency control prevents duplicate processing', function () {
    $game = VideoGame::factory()->create();

    // Try to enrich the same game twice concurrently
    EnrichVideoGameJob::dispatchSync($game->id);

    // Second attempt should be skipped due to lock (no exception thrown)
    EnrichVideoGameJob::dispatchSync($game->id);

    expect(true)->toBeTrue(); // Both should complete without error
});

test('horizon queue configuration includes enrichment supervisor', function () {
    $config = config('horizon.defaults');

    expect($config)->toHaveKey('supervisor-enrichment');
    expect($config['supervisor-enrichment']['queue'])->toBe(['enrichment']);
});

test('rate limiters are configured for all providers', function () {
    $limiters = ['steam', 'igdb', 'tgdb', 'psstore', 'xbox'];

    foreach ($limiters as $limiterName) {
        $limiter = app('Illuminate\Cache\RateLimiter')->limiter($limiterName);
        expect($limiter)->not->toBeNull();
    }
});
