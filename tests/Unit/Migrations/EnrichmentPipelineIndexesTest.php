<?php

declare(strict_types=1);

use Illuminate\Support\Facades\Schema;

test('enrichment pipeline indexes exist on video_game_prices table', function () {
    $indexes = Schema::getIndexes('video_game_prices');
    $indexNames = collect($indexes)->pluck('name')->toArray();

    expect($indexNames)->toContain('idx_prices_enrichment_lookup');
    expect($indexNames)->toContain('idx_prices_retailer_active');
});

test('enrichment pipeline indexes exist on images table', function () {
    $indexes = Schema::getIndexes('images');
    $indexNames = collect($indexes)->pluck('name')->toArray();

    expect($indexNames)->toContain('idx_images_enrichment_dedup');
    expect($indexNames)->toContain('idx_images_provider_external');
});

test('enrichment pipeline indexes exist on videos table', function () {
    $indexes = Schema::getIndexes('videos');
    $indexNames = collect($indexes)->pluck('name')->toArray();

    expect($indexNames)->toContain('idx_videos_enrichment_dedup');
    expect($indexNames)->toContain('idx_videos_provider_external');
});

test('last_enriched_at column and index exist on video_games table', function () {
    expect(Schema::hasColumn('video_games', 'last_enriched_at'))->toBeTrue();

    $indexes = Schema::getIndexes('video_games');
    $indexNames = collect($indexes)->pluck('name')->toArray();

    expect($indexNames)->toContain('idx_games_last_enriched');
});
