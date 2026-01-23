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

        DB::statement('DROP MATERIALIZED VIEW IF EXISTS public.video_games_toplists_mv');

        DB::statement("
            CREATE MATERIALIZED VIEW public.video_games_toplists_mv AS
            SELECT 
                list_key,
                list_type,
                list_name,
                provider_key,
                rank,
                id,
                name,
                slug,
                rating,
                release_date,
                platform,
                popularity_score,
                canonical_name,
                rating_count,
                genre,
                media,
                image_url,
                image_urls,
                review_score,
                snapshot_at
            FROM (
                SELECT DISTINCT ON (pt.list_key, vgr.id)
                    pt.list_key,
                    pt.list_type,
                    CASE 
                        WHEN pt.list_key = 'trending' THEN 'Trending Now'
                        WHEN pt.list_key = 'upcoming' THEN 'Most Anticipated'
                        WHEN pt.list_key = 'popular' THEN 'All Time Popular'
                        WHEN pt.list_key = 'top_rated' THEN 'Highest Rated'
                        ELSE INITCAP(REPLACE(pt.list_key, '-', ' '))
                    END as list_name,
                    pt.provider_key,
                    pti.rank,
                    vgr.id,
                    vgr.name,
                    vgr.slug,
                    vgr.rating,
                    vgr.release_date,
                    vgr.platform,
                    vgr.popularity_score,
                    vgr.canonical_name,
                    vgr.rating_count,
                    vgr.genre,
                    vgr.media,
                    vgr.image_url,
                    vgr.image_urls,
                    vgr.review_score,
                    pt.snapshot_at
                FROM public.provider_toplists pt
                JOIN public.provider_toplist_items pti ON pti.provider_toplist_id = pt.id
                JOIN public.video_games_ranked_mv vgr ON vgr.id = pti.video_game_id
                ORDER BY pt.list_key, vgr.id, pti.rank ASC
            ) sub
            ORDER BY list_key, release_date DESC NULLS LAST
            WITH DATA
        ");

        DB::statement('CREATE INDEX idx_vg_toplists_key ON public.video_games_toplists_mv (list_key)');
        DB::statement('CREATE INDEX idx_vg_toplists_date ON public.video_games_toplists_mv (release_date DESC NULLS LAST)');
    }

    /**
     * Reverse the migrations.
     */
    public function down(): void
    {
        if (DB::getDriverName() !== 'pgsql') {
            return;
        }

        DB::statement('DROP MATERIALIZED VIEW IF EXISTS public.video_games_toplists_mv');
    }
};
