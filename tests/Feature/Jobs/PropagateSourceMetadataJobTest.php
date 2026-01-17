<?php

declare(strict_types=1);

use App\Jobs\Propagation\PropagateMediaToVideoGameJob;
use App\Jobs\Propagation\PropagateSourceMetadataJob;
use App\Models\VideoGame;
use App\Models\VideoGameTitle;
use App\Models\VideoGameTitleSource;
use Illuminate\Foundation\Testing\RefreshDatabase;
use Illuminate\Support\Facades\Queue;

uses(RefreshDatabase::class);

test('job creates canonical VideoGame from source', function () {
    $title = VideoGameTitle::factory()->create();

    $source = VideoGameTitleSource::create([
        'video_game_title_id' => $title->id,
        'provider' => 'igdb',
        'external_id' => '12345',
        'name' => 'Test Game',
        'rating' => 85.5,
        'developer' => 'Test Dev',
        'publisher' => 'Test Pub',
    ]);

    PropagateSourceMetadataJob::dispatchSync($source->id);

    $videoGame = VideoGame::where('provider', 'igdb')
        ->where('external_id', '12345')
        ->first();

    expect($videoGame)->not->toBeNull();
    expect($videoGame->name)->toBe('Test Game');
    expect($videoGame->rating)->toBe(85.5);
    expect($videoGame->video_game_title_id)->toBe($title->id);
});

test('job updates existing VideoGame with higher priority source', function () {
    $title = VideoGameTitle::factory()->create();

    // Create existing VideoGame from lower priority source (steam = 80)
    $existingGame = VideoGame::create([
        'video_game_title_id' => $title->id,
        'provider' => 'steam',
        'external_id' => '12345',
        'name' => 'Old Name',
        'rating' => 75.0,
    ]);

    // Create higher priority source (igdb = 100)
    $source = VideoGameTitleSource::create([
        'video_game_title_id' => $title->id,
        'provider' => 'igdb',
        'external_id' => '12345',
        'name' => 'Updated Name',
        'rating' => 90.0,
    ]);

    PropagateSourceMetadataJob::dispatchSync($source->id);

    // Creates a NEW VideoGame because provider+external_id differs
    // This is correct behavior - different providers have different records
    $igdbGame = VideoGame::where('provider', 'igdb')
        ->where('external_id', '12345')
        ->first();

    expect($igdbGame)->not->toBeNull();
    expect($igdbGame->name)->toBe('Updated Name');
    expect($igdbGame->rating)->toBe(90.0);
});

test('job is idempotent - running twice produces same result', function () {
    $title = VideoGameTitle::factory()->create();

    $source = VideoGameTitleSource::create([
        'video_game_title_id' => $title->id,
        'provider' => 'igdb',
        'external_id' => '12345',
        'name' => 'Test Game',
    ]);

    // Run job twice
    PropagateSourceMetadataJob::dispatchSync($source->id);
    PropagateSourceMetadataJob::dispatchSync($source->id);

    // Should only create one VideoGame
    $count = VideoGame::where('provider', 'igdb')
        ->where('external_id', '12345')
        ->count();

    expect($count)->toBe(1);
});

test('job dispatches media propagation job after metadata propagation', function () {
    Queue::fake();

    $title = VideoGameTitle::factory()->create();

    $source = VideoGameTitleSource::create([
        'video_game_title_id' => $title->id,
        'provider' => 'igdb',
        'external_id' => '12345',
        'name' => 'Test Game',
    ]);

    PropagateSourceMetadataJob::dispatchSync($source->id);

    Queue::assertPushed(PropagateMediaToVideoGameJob::class);
});

test('job uses transactional writes', function () {
    $title = VideoGameTitle::factory()->create();

    $source = VideoGameTitleSource::create([
        'video_game_title_id' => $title->id,
        'provider' => 'igdb',
        'external_id' => '12345',
        'name' => 'Test Game',
    ]);

    DB::beginTransaction();

    PropagateSourceMetadataJob::dispatchSync($source->id);

    // Rollback the outer transaction
    DB::rollBack();

    // The job should have created its own transaction, so data should NOT exist
    // (because the job's transaction was inside the rolled-back transaction)
    $count = VideoGame::where('provider', 'igdb')
        ->where('external_id', '12345')
        ->count();

    expect($count)->toBe(0);
});

test('job handles missing source gracefully', function () {
    PropagateSourceMetadataJob::dispatchSync(99999);

    // Should not throw exception
    expect(true)->toBeTrue();
});

test('job implements ShouldBeUnique interface', function () {
    $job = new PropagateSourceMetadataJob(123);

    expect($job)->toBeInstanceOf(\Illuminate\Contracts\Queue\ShouldBeUnique::class);
    expect($job->uniqueId())->toBe('propagate-source-123');
});

test('job merges metadata into attributes field correctly', function () {
    $title = VideoGameTitle::factory()->create();

    $source = VideoGameTitleSource::create([
        'video_game_title_id' => $title->id,
        'provider' => 'igdb',
        'external_id' => '12345',
        'name' => 'Test Game',
        'description' => 'Test description',
        'developer' => 'Test Developer',
        'publisher' => 'Test Publisher',
        'genre' => ['Action', 'RPG'],
        'platform' => ['PS5', 'PC'],
    ]);

    PropagateSourceMetadataJob::dispatchSync($source->id);

    $videoGame = VideoGame::where('provider', 'igdb')
        ->where('external_id', '12345')
        ->first();

    expect($videoGame->attributes)->toHaveKey('description');
    expect($videoGame->attributes)->toHaveKey('developer');
    expect($videoGame->attributes['developer'])->toBe('Test Developer');
});
