<?php

declare(strict_types=1);

use Illuminate\Support\Facades\Schema;
use Tests\TestCase;

uses(TestCase::class);

it('enforces products -> video_game_titles -> video_games traversal by schema shape', function () {
    // The ONLY table that should have product_id in the game domain is video_game_titles.
    expect(Schema::hasColumn('video_game_titles', 'product_id'))->toBeTrue();

    expect(Schema::hasColumn('video_games', 'product_id'))->toBeFalse();
    expect(Schema::hasColumn('video_game_sources', 'product_id'))->toBeFalse();
    expect(Schema::hasColumn('video_game_title_sources', 'product_id'))->toBeFalse();

    // video_games must point to video_game_titles.
    expect(Schema::hasColumn('video_games', 'video_game_title_id'))->toBeTrue();

    // video_game_title_sources is the only place provider item IDs/payloads live.
    expect(Schema::hasColumn('video_game_title_sources', 'provider_item_id'))->toBeTrue();
    expect(Schema::hasColumn('video_game_title_sources', 'raw_payload'))->toBeTrue();
});
