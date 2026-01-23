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

        DB::statement('
            CREATE MATERIALIZED VIEW public.video_games_toplists_mv AS
            SELECT 
                pt.provider_key,
                pt.list_key,
                pt.list_type,
                pt.name as list_name,
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
            ORDER BY pt.provider_key, pt.list_key, vgr.release_date DESC NULLS LAST, pti.rank ASC
            WITH DATA
        ');

        DB::statement('CREATE INDEX idx_vg_toplists_key ON public.video_games_toplists_mv (list_key)');
        DB::statement('CREATE INDEX idx_vg_toplists_provider ON public.video_games_toplists_mv (provider_key)');
        DB::statement('CREATE INDEX idx_vg_toplists_rank ON public.video_games_toplists_mv (rank)');
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
