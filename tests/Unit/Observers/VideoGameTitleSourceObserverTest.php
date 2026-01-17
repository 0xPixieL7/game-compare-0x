<?php

declare(strict_types=1);

use App\Jobs\Propagation\PropagateSourceMetadataJob;
use App\Models\VideoGameTitle;
use App\Models\VideoGameTitleSource;
use Illuminate\Foundation\Testing\RefreshDatabase;
use Illuminate\Support\Facades\Queue;

uses(RefreshDatabase::class);

test('VideoGameTitleSource observer dispatches propagation job on creation', function () {
    Queue::fake();

    $title = VideoGameTitle::factory()->create();

    $source = VideoGameTitleSource::create([
        'video_game_title_id' => $title->id,
        'provider' => 'igdb',
        'external_id' => '12345',
        'name' => 'Test Game',
    ]);

    Queue::assertPushed(PropagateSourceMetadataJob::class, function ($job) use ($source) {
        return $job->sourceId === $source->id;
    });
});

test('VideoGameTitleSource observer dispatches propagation job when watched fields change', function () {
    Queue::fake();

    $title = VideoGameTitle::factory()->create();

    $source = VideoGameTitleSource::create([
        'video_game_title_id' => $title->id,
        'provider' => 'igdb',
        'external_id' => '12345',
        'name' => 'Test Game',
    ]);

    Queue::assertPushed(PropagateSourceMetadataJob::class, 1); // From creation

    // Update watched field - SHOULD dispatch
    $source->update(['name' => 'Updated Game Name']);
    Queue::assertPushed(PropagateSourceMetadataJob::class, 2);

    // Update another watched field - SHOULD dispatch
    $source->update(['rating' => 85.0]);
    Queue::assertPushed(PropagateSourceMetadataJob::class, 3);
});

test('VideoGameTitleSource observer does not dispatch propagation job when non-watched fields change', function () {
    Queue::fake();

    $title = VideoGameTitle::factory()->create();

    $source = VideoGameTitleSource::create([
        'video_game_title_id' => $title->id,
        'provider' => 'igdb',
        'external_id' => '12345',
        'name' => 'Test Game',
    ]);

    Queue::assertPushed(PropagateSourceMetadataJob::class, 1); // From creation

    // Update non-watched field (external_id is not in the watched list)
    $source->update(['external_id' => '67890']);

    // Should still be 1 (only from creation)
    Queue::assertPushed(PropagateSourceMetadataJob::class, 1);
});

test('observer dispatches job after database transaction commits', function () {
    Queue::fake();

    $title = VideoGameTitle::factory()->create();

    DB::transaction(function () use ($title) {
        VideoGameTitleSource::create([
            'video_game_title_id' => $title->id,
            'provider' => 'igdb',
            'external_id' => '12345',
            'name' => 'Test Game',
        ]);

        // Job should not be pushed yet (transaction not committed)
        Queue::assertNothingPushed();
    });

    // After transaction commits, job should be pushed
    Queue::assertPushed(PropagateSourceMetadataJob::class);
});
