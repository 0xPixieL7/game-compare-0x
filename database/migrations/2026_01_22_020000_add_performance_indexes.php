<?php

declare(strict_types=1);

use Illuminate\Database\Migrations\Migration;
use Illuminate\Support\Facades\DB;

return new class extends Migration
{
    /**
     * Run the migrations - Add performance indexes for premium filtering and ranking
     */
    public function up(): void
    {
        // Indexes for video_games table - Optimized for premium filtering
        DB::statement('CREATE INDEX IF NOT EXISTS idx_vg_rating_count ON video_games(rating DESC, rating_count DESC) WHERE rating >= 60 AND rating_count >= 5');
        DB::statement('CREATE INDEX IF NOT EXISTS idx_vg_release_date ON video_games(release_date DESC) WHERE release_date IS NOT NULL');
        DB::statement('CREATE INDEX IF NOT EXISTS idx_vg_popularity ON video_games(popularity_score DESC NULLS LAST) WHERE popularity_score IS NOT NULL');

        // Indexes for video_game_title_sources (Dashboard queries)
        DB::statement('CREATE INDEX IF NOT EXISTS idx_vgts_rating_premium ON video_game_title_sources(rating DESC, rating_count DESC) WHERE rating >= 60 AND rating_count >= 5 AND provider = \'igdb\'');

        // Composite index for pricing queries
        DB::statement('CREATE INDEX IF NOT EXISTS idx_vgp_game_recorded ON video_game_prices(video_game_id, recorded_at DESC)');

        // Indexes for media queries
        DB::statement('CREATE INDEX IF NOT EXISTS idx_images_game ON images(imageable_id, imageable_type) WHERE imageable_type = \'App\\\\Models\\\\VideoGame\'');
        DB::statement('CREATE INDEX IF NOT EXISTS idx_videos_game ON videos(video_game_id) WHERE video_game_id IS NOT NULL');
    }

    /**
     * Reverse the migrations.
     */
    public function down(): void
    {
        DB::statement('DROP INDEX CONCURRENTLY IF EXISTS idx_vg_rating_count');
        DB::statement('DROP INDEX CONCURRENTLY IF EXISTS idx_vg_release_date');
        DB::statement('DROP INDEX CONCURRENTLY IF EXISTS idx_vg_popularity');
        DB::statement('DROP INDEX CONCURRENTLY IF EXISTS idx_vgts_rating_premium');
        DB::statement('DROP INDEX CONCURRENTLY IF EXISTS idx_vgts_genre');
        DB::statement('DROP INDEX CONCURRENTLY IF EXISTS idx_vgp_game_recorded');
        DB::statement('DROP INDEX CONCURRENTLY IF EXISTS idx_images_game');
        DB::statement('DROP INDEX CONCURRENTLY IF EXISTS idx_videos_game');
    }
};
