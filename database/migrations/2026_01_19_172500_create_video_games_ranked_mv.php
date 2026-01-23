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

        // This materialized view organizes games by rating and release date (descending).
        // It is optimized for the landing page 'Top Rated' and 'New Releases' sections.
        DB::statement('
            CREATE MATERIALIZED VIEW public.video_games_ranked_mv AS
            WITH primary_images AS (
                SELECT DISTINCT ON (img.video_game_id)
                    img.video_game_id,
                    img.url as image_url,
                    img.urls as image_urls
                FROM public.images img
                ORDER BY img.video_game_id, img.is_thumbnail DESC, img.order_column NULLS LAST, img.created_at DESC
            ),
            primary_sources AS (
                SELECT DISTINCT ON (vgts.video_game_title_id)
                    vgts.video_game_title_id,
                    vgts.rating_count,
                    vgts.genre,
                    vgts.raw_payload as media
                FROM public.video_game_title_sources vgts
                ORDER BY vgts.video_game_title_id, vgts.rating DESC NULLS LAST, vgts.id
            )
            SELECT 
                vg.id,
                vg.name,
                vg.slug,
                vg.rating,
                vg.release_date,
                vg.platform,
                vg.developer,
                vg.hypes,
                vg.follows,
                vg.popularity_score,
                vgt.name as canonical_name,
                s.rating_count,
                s.genre,
                s.media,
                pi.image_url,
                pi.image_urls,
                (
                    (COALESCE(vg.rating, 0) * 0.6) + 
                    (LEAST(COALESCE(vg.rating_count, 0), 1000) * 0.02) + 
                    (log(COALESCE(vg.popularity_score, 0) + 1) * 5)
                ) as review_score
            FROM public.video_games vg
            JOIN public.video_game_titles vgt ON vgt.id = vg.video_game_title_id
            LEFT JOIN primary_sources s ON s.video_game_title_id = vgt.id
            LEFT JOIN primary_images pi ON pi.video_game_id = vg.id
            ORDER BY review_score DESC NULLS LAST, vg.release_date DESC NULLS LAST
            WITH DATA
        ');

        // Indexes for fast sorting and lookups
        DB::statement('CREATE INDEX idx_vgr_mv_rating_date ON public.video_games_ranked_mv (rating DESC NULLS LAST, release_date DESC NULLS LAST)');
        DB::statement('CREATE INDEX idx_vgr_mv_date_rating ON public.video_games_ranked_mv (release_date DESC NULLS LAST, rating DESC NULLS LAST)');
        DB::statement('CREATE INDEX idx_vgr_mv_id ON public.video_games_ranked_mv (id)');

        // Index on title for searchability
        DB::statement('CREATE INDEX idx_vgr_mv_name ON public.video_games_ranked_mv USING gin (name extensions.gin_trgm_ops)');
    }

    /**
     * Reverse the migrations.
     */
    public function down(): void
    {
        if (DB::getDriverName() !== 'pgsql') {
            return;
        }

        DB::statement('DROP MATERIALIZED VIEW IF EXISTS public.video_games_ranked_mv');
    }
};
