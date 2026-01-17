<?php

declare(strict_types=1);

use Illuminate\Database\Migrations\Migration;

return new class extends Migration
{
    public function up(): void
    {
        // NOTE:
        // This migration was originally created before the `video_game_sources` and
        // `video_game_titles` tables existed.
        //
        // The canonical `video_games` table definition now lives in
        // `2026_01_12_020150_create_video_games_table.php`, which runs AFTER those
        // dependencies and can safely create foreign keys.
    }

    public function down(): void
    {
        // no-op
    }
};
