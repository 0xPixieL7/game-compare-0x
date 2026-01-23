<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Support\Facades\DB;

return new class extends Migration
{
    /**
     * Run the migrations.
     */
    public function up(): void
    {
        if (DB::getDriverName() !== 'pgsql') {
            return;
        }

        // 1. Upcoming Games Materialized View
        // Focuses on games with future release dates, ordered by popularity score.
        DB::statement('
            CREATE MATERIALIZED VIEW public.video_games_upcoming_mv AS
            SELECT 
                id, name, slug, rating, release_date, platform, 
                popularity_score, canonical_name, rating_count, genre, media, 
                image_url, image_urls, review_score
            FROM public.video_games_ranked_mv
            WHERE release_date > CURRENT_DATE
            ORDER BY popularity_score DESC NULLS LAST, release_date ASC NULLS LAST
            WITH DATA
        ');

        DB::statement('CREATE INDEX idx_vg_upcoming_pop ON public.video_games_upcoming_mv (popularity_score DESC NULLS LAST)');
        DB::statement('CREATE INDEX idx_vg_upcoming_date ON public.video_games_upcoming_mv (release_date ASC)');

        // 2. Genre-Based Ranking Materialized View
        // Unnests the genre array to allow for extremely fast per-genre top lists.
        DB::statement("
            CREATE MATERIALIZED VIEW public.video_games_genre_ranked_mv AS
            SELECT 
                id, name, slug, rating, release_date, platform, 
                popularity_score, canonical_name, rating_count,
                jsonb_array_elements_text(CASE 
                    WHEN jsonb_typeof(genre::jsonb) = 'array' THEN genre::jsonb 
                    ELSE '[]'::jsonb 
                END) as genre_name,
                media, image_url, image_urls, review_score
            FROM public.video_games_ranked_mv
            ORDER BY rating DESC NULLS LAST, popularity_score DESC NULLS LAST
            WITH DATA
        ");

        DB::statement('CREATE INDEX idx_vg_genre_ranked_name ON public.video_games_genre_ranked_mv (genre_name)');
        DB::statement('CREATE INDEX idx_vg_genre_ranked_rating ON public.video_games_genre_ranked_mv (rating DESC NULLS LAST)');
    }

    /**
     * Reverse the migrations.
     */
    public function down(): void
    {
        if (DB::getDriverName() !== 'pgsql') {
            return;
        }

        DB::statement('DROP MATERIALIZED VIEW IF EXISTS public.video_games_upcoming_mv');
        DB::statement('DROP MATERIALIZED VIEW IF EXISTS public.video_games_genre_ranked_mv');
    }
};
