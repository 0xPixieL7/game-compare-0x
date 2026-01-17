<?php

declare(strict_types=1);

use Illuminate\Database\Migrations\Migration;
use Illuminate\Support\Facades\DB;

return new class extends Migration
{
    public function withinTransaction(): bool
    {
        return false;
    }

    public function up(): void
    {
        // Skip this migration for non-PostgreSQL databases
        if (DB::connection()->getDriverName() !== 'pgsql') {
            return;
        }

        DB::statement('CREATE EXTENSION IF NOT EXISTS bloom WITH SCHEMA extensions');

        DB::statement(<<<'SQL'
CREATE MATERIALIZED VIEW IF NOT EXISTS public.video_game_title_sources_mv AS
WITH latest_prices AS (
    SELECT DISTINCT ON (vgp.video_game_id, vgp.country_code, vgp.retailer)
        vgp.video_game_id,
        vgp.country_code,
        vgp.retailer,
        vgp.amount_minor,
        vgp.currency,
        vgp.recorded_at,
        vgp.bucket
    FROM public.video_game_prices vgp
    WHERE vgp.bucket = 'snapshot'
      AND vgp.is_active = true
    ORDER BY
        vgp.video_game_id,
        vgp.country_code,
        vgp.retailer,
        vgp.recorded_at DESC,
        vgp.id DESC
),
primary_images AS (
    SELECT DISTINCT ON (img.video_game_id)
        img.video_game_id,
        img.url AS primary_image_url
    FROM public.images img
    WHERE img.is_thumbnail = true
    ORDER BY img.video_game_id, img.order_column NULLS LAST, img.created_at DESC
),
image_rollup AS (
    SELECT
        img.video_game_id,
        jsonb_agg(img.url ORDER BY img.order_column NULLS LAST, img.created_at DESC) AS image_urls
    FROM public.images img
    WHERE img.is_thumbnail = false
    GROUP BY img.video_game_id
),
primary_videos AS (
    SELECT DISTINCT ON (vid.video_game_id)
        vid.video_game_id,
        vid.url AS primary_video_url
    FROM public.videos vid
    ORDER BY vid.video_game_id, vid.order_column NULLS LAST, vid.created_at DESC
)
SELECT
    vgts.id AS video_game_title_source_id,
    vgts.video_game_title_id,
    vgts.video_game_source_id,
    vgts.provider,
    vgts.provider_item_id,
    vgts.external_id,
    vgts.slug,
    vgts.name,
    vgts.description,
    vgts.release_date,
    vgts.platform,
    vgts.rating,
    vgts.rating_count,
    vgts.developer,
    vgts.publisher,
    vgts.genre,
    vgts.raw_payload,
    vgt.name AS title_name,
    vgt.slug AS title_slug,
    vgt.normalized_title,
    vg.id AS video_game_id,
    lp.country_code,
    lp.retailer,
    lp.amount_minor,
    lp.currency,
    lp.recorded_at,
    lp.bucket,
    pi.primary_image_url,
    ir.image_urls,
    pv.primary_video_url,
    vg.created_at,
    vg.updated_at
FROM public.video_game_title_sources vgts
JOIN public.video_game_titles vgt
  ON vgt.id = vgts.video_game_title_id
JOIN public.video_games vg
  ON vg.video_game_title_id = vgt.id
LEFT JOIN latest_prices lp
  ON lp.video_game_id = vg.id
LEFT JOIN primary_images pi
  ON pi.video_game_id = vg.id
LEFT JOIN image_rollup ir
  ON ir.video_game_id = vg.id
LEFT JOIN primary_videos pv
  ON pv.video_game_id = vg.id
WITH NO DATA
SQL
        );

        DB::statement('CREATE INDEX IF NOT EXISTS vgts_mv_title_id_idx ON public.video_game_title_sources_mv (video_game_title_id)');
        DB::statement('CREATE INDEX IF NOT EXISTS vgts_mv_provider_idx ON public.video_game_title_sources_mv (provider)');
        DB::statement('CREATE INDEX IF NOT EXISTS vgts_mv_country_code_idx ON public.video_game_title_sources_mv (country_code)');
        DB::statement('CREATE INDEX IF NOT EXISTS vgts_mv_retailer_idx ON public.video_game_title_sources_mv (retailer)');
        DB::statement('CREATE INDEX IF NOT EXISTS vgts_mv_amount_minor_idx ON public.video_game_title_sources_mv (amount_minor)');
        DB::statement('CREATE INDEX IF NOT EXISTS vgts_mv_provider_country_retailer_amount_idx ON public.video_game_title_sources_mv (provider, country_code, retailer, amount_minor)');
        DB::statement('CREATE INDEX IF NOT EXISTS vgts_mv_bloom_filters_idx ON public.video_game_title_sources_mv USING bloom (provider, (country_code::text), retailer, (currency::text))');
    }

    public function down(): void
    {
        DB::statement('DROP INDEX IF EXISTS vgts_mv_bloom_filters_idx');
        DB::statement('DROP INDEX IF EXISTS vgts_mv_provider_country_retailer_amount_idx');
        DB::statement('DROP INDEX IF EXISTS vgts_mv_amount_minor_idx');
        DB::statement('DROP INDEX IF EXISTS vgts_mv_retailer_idx');
        DB::statement('DROP INDEX IF EXISTS vgts_mv_country_code_idx');
        DB::statement('DROP INDEX IF EXISTS vgts_mv_provider_idx');
        DB::statement('DROP INDEX IF EXISTS vgts_mv_title_id_idx');
        DB::statement('DROP MATERIALIZED VIEW IF EXISTS public.video_game_title_sources_mv');
    }
};
