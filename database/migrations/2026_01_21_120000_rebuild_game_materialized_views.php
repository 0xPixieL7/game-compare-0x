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

        if (DB::getDriverName() !== 'pgsql') {
            return;
        }

        // Increase timeout for large view recreation
        DB::statement("SET statement_timeout = '20min'");

        // Drop ALL dependent views first to avoid dependency errors
        DB::statement('DROP MATERIALIZED VIEW IF EXISTS public.video_games_toplists_mv');
        DB::statement('DROP MATERIALIZED VIEW IF EXISTS public.video_games_genre_ranked_mv');
        DB::statement('DROP MATERIALIZED VIEW IF EXISTS public.video_games_upcoming_mv');
        // CASCADE makes this idempotent even if a dependent MV was left behind.
        DB::statement('DROP MATERIALIZED VIEW IF EXISTS public.video_games_ranked_mv CASCADE');

        // 1. Recreate video_games_ranked_mv with enhanced media and ranking primitives
        // Ordered by latest to oldest as primarily requested
        DB::statement("
            CREATE MATERIALIZED VIEW public.video_games_ranked_mv AS
            WITH primary_images AS (
                SELECT DISTINCT ON (img.video_game_id)
                    img.video_game_id,
                    img.url as image_url,
                    img.urls as image_urls
                FROM public.images img
                WHERE img.primary_collection NOT IN ('screenshot', 'screenshots', 'background', 'wallpaper', 'background_image', 'background_images', 'hero_images')
                ORDER BY img.video_game_id, img.is_thumbnail DESC, img.order_column NULLS LAST, img.created_at DESC
            ),
            collection_images AS (
                SELECT 
                    video_game_id,
                    MAX(url) FILTER (WHERE primary_collection IN ('cover', 'front-cover', 'box-art', 'cover_front', 'cover_images')) as cover_url,
                    MAX(url) FILTER (WHERE primary_collection IN ('background', 'screenshot', 'wallpaper', 'background_image', 'screenshots', 'background_images', 'hero_images')) as background_url,
                    MAX(url) FILTER (WHERE primary_collection IN ('artwork', 'fanart', 'clear_art', 'artworks')) as artwork_url
                FROM public.images
                GROUP BY video_game_id
            ),
            primary_videos AS (
                SELECT DISTINCT ON (vid.video_game_id)
                    vid.video_game_id,
                    vid.video_id,
                    vid.title as video_name
                FROM public.videos vid
                ORDER BY vid.video_game_id, 
                    CASE vid.primary_collection
                        WHEN 'trailers' THEN 1
                        WHEN 'launch_trailers' THEN 2
                        WHEN 'cinematic_trailers' THEN 3
                        WHEN 'gameplay' THEN 4
                        WHEN 'promotional' THEN 5
                        ELSE 6
                    END ASC,
                    vid.created_at DESC
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
                COALESCE(vg.rating_count, s.rating_count, 0) as rating_count,
                COALESCE(vg.genre, s.genre) as genre,
                s.media,
                pi.image_url,
                pi.image_urls,
                ci.cover_url,
                ci.background_url,
                ci.artwork_url,
                pv.video_id as primary_video_id,
                pv.video_name as primary_video_name,
                (
                    (COALESCE(vg.rating, 0) * 0.45) + 
                    (LEAST(COALESCE(vg.rating_count, s.rating_count, 0), 1000) * 0.01) + 
                    (log(COALESCE(vg.popularity_score, 0) + 1) * 3) +
                    (CASE 
                        WHEN vg.release_date > CURRENT_DATE - INTERVAL '6 months' THEN 40
                        WHEN vg.release_date > CURRENT_DATE - INTERVAL '1 year' THEN 25
                        WHEN vg.release_date > CURRENT_DATE - INTERVAL '2 years' THEN 15
                        WHEN vg.release_date > CURRENT_DATE THEN 50 -- Future high anticipation
                        ELSE 0 
                    END)
                ) as review_score
            FROM public.video_games vg
            JOIN public.video_game_titles vgt ON vgt.id = vg.video_game_title_id
            LEFT JOIN primary_sources s ON s.video_game_title_id = vgt.id
            LEFT JOIN primary_images pi ON pi.video_game_id = vg.id
            LEFT JOIN collection_images ci ON ci.video_game_id = vg.id
            LEFT JOIN primary_videos pv ON pv.video_game_id = vg.id
            ORDER BY vg.release_date DESC NULLS LAST, review_score DESC NULLS LAST
            WITH DATA
        ");

        // Required for REFRESH MATERIALIZED VIEW CONCURRENTLY.
        DB::statement('CREATE UNIQUE INDEX IF NOT EXISTS ux_vgr_mv_id ON public.video_games_ranked_mv (id)');

        // 2. Recreate video_games_genre_ranked_mv (Flattening genre array)
        DB::statement("
            CREATE MATERIALIZED VIEW public.video_games_genre_ranked_mv AS
            SELECT 
                mv.*,
                genre_name
            FROM public.video_games_ranked_mv mv
            CROSS JOIN LATERAL jsonb_array_elements_text(
                CASE 
                    WHEN jsonb_typeof(mv.genre::jsonb) = 'array' THEN mv.genre::jsonb 
                    ELSE '[]'::jsonb 
                END
            ) as genre_name
            WITH DATA
        ");

        // Required for REFRESH MATERIALIZED VIEW CONCURRENTLY.
        DB::statement('CREATE UNIQUE INDEX IF NOT EXISTS ux_vg_genre_ranked_mv_id_genre ON public.video_games_genre_ranked_mv (id, genre_name)');

        // 3. Recreate video_games_upcoming_mv
        DB::statement('
            CREATE MATERIALIZED VIEW public.video_games_upcoming_mv AS
            SELECT * FROM public.video_games_ranked_mv
            WHERE release_date > CURRENT_DATE
            ORDER BY release_date ASC, review_score DESC
            WITH DATA
        ');

        // Required for REFRESH MATERIALIZED VIEW CONCURRENTLY.
        DB::statement('CREATE UNIQUE INDEX IF NOT EXISTS ux_vg_upcoming_mv_id ON public.video_games_upcoming_mv (id)');

        // 4. Recreate video_games_toplists_mv with Latest to Oldest priority
        DB::statement("
            CREATE MATERIALIZED VIEW public.video_games_toplists_mv AS
            WITH game_prices_agg AS (
                SELECT 
                    video_game_id, 
                    jsonb_object_agg(currency, amount_minor) as prices
                FROM public.video_game_prices
                WHERE is_active = true
                GROUP BY video_game_id
            )
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
                cover_url,
                background_url,
                artwork_url,
                primary_video_id,
                primary_video_name,
                review_score,
                prices,
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
                    vgr.cover_url,
                    vgr.background_url,
                    vgr.artwork_url,
                    vgr.primary_video_id,
                    vgr.primary_video_name,
                    vgr.review_score,
                    gpa.prices,
                    pt.snapshot_at
                FROM public.provider_toplists pt
                JOIN public.provider_toplist_items pti ON pti.provider_toplist_id = pt.id
                JOIN public.video_games_ranked_mv vgr ON vgr.id = pti.video_game_id
                LEFT JOIN game_prices_agg gpa ON gpa.video_game_id = vgr.id
                ORDER BY pt.list_key, vgr.id, pti.rank ASC
            ) sub
            ORDER BY list_key, release_date DESC NULLS LAST, popularity_score DESC NULLS LAST
            WITH DATA
        ");

        // Required for REFRESH MATERIALIZED VIEW CONCURRENTLY.
        DB::statement('CREATE UNIQUE INDEX IF NOT EXISTS ux_vg_toplists_mv_list_key_id ON public.video_games_toplists_mv (list_key, id)');

        // Indexes
        DB::statement('CREATE INDEX idx_vgr_mv_id ON public.video_games_ranked_mv (id)');
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
        DB::statement('DROP MATERIALIZED VIEW IF EXISTS public.video_games_genre_ranked_mv');
        DB::statement('DROP MATERIALIZED VIEW IF EXISTS public.video_games_upcoming_mv');
        DB::statement('DROP MATERIALIZED VIEW IF EXISTS public.video_games_ranked_mv CASCADE');
    }
};
