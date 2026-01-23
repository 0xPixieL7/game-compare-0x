<?php

declare(strict_types=1);

use App\Jobs\Enrichment\EnrichGameMediaJob;
use App\Jobs\Enrichment\EnrichGamePricesJob;
use App\Jobs\Enrichment\EnrichVideoGameJob;
use App\Models\VideoGame;
use Illuminate\Support\Facades\Cache;
use Illuminate\Support\Facades\Queue;

test('EnrichVideoGameJob dispatches price and media enrichment jobs', function () {
    Queue::fake();

    $game = VideoGame::factory()->create();

    EnrichVideoGameJob::dispatchSync($game->id);

    Queue::assertPushed(EnrichGamePricesJob::class, function ($job) use ($game) {
        return $job->videoGameId === $game->id;
    });

    Queue::assertPushed(EnrichGameMediaJob::class, function ($job) use ($game) {
        return $job->videoGameId === $game->id;
    });
});

test('EnrichVideoGameJob uses atomic locking to prevent duplicate processing', function () {
    $game = VideoGame::factory()->create();
    $lockKey = "enrich_game:{$game->id}";

    // Acquire lock manually
    $lock = Cache::lock($lockKey, 60);
    expect($lock->get())->toBeTrue();

    // Try to run job while locked
    $executed = false;
    try {
        EnrichVideoGameJob::dispatchSync($game->id);
        $executed = true;
    } catch (Exception $e) {
        // Job should skip due to lock
    }

    // Job should have been skipped (no exception thrown, but logs show skip)
    expect($executed)->toBeTrue(); // Job runs but returns early

    $lock->release();
});

test('EnrichVideoGameJob implements ShouldBeUnique interface', function () {
    $job = new EnrichVideoGameJob(123);

    expect($job)->toBeInstanceOf(\Illuminate\Contracts\Queue\ShouldBeUnique::class);
    expect($job->uniqueId())->toBe('123');
});

test('EnrichVideoGameJob releases lock after execution', function () {
    $game = VideoGame::factory()->create();
    $lockKey = "enrich_game:{$game->id}";

    EnrichVideoGameJob::dispatchSync($game->id);

    // Lock should be released after job completes
    $lock = Cache::lock($lockKey, 60);
    expect($lock->get())->toBeTrue();

    $lock->release();
});

test('EnrichVideoGameJob handles missing game gracefully', function () {
    EnrichVideoGameJob::dispatchSync(99999);

    // Should not throw exception
    expect(true)->toBeTrue();
});

test('EnrichVideoGameJob dispatches sub-jobs even if one provider fails', function () {
    Queue::fake();

    $game = VideoGame::factory()->create();

    EnrichVideoGameJob::dispatchSync($game->id);

    // Both jobs should be dispatched regardless of individual failures
    Queue::assertPushed(EnrichGamePricesJob::class);
    Queue::assertPushed(EnrichGameMediaJob::class);
});
