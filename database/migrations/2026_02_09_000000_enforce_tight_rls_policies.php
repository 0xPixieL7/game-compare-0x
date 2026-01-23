<?php

declare(strict_types=1);

use Illuminate\Database\Migrations\Migration;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    /**
     * Allow RLS policy operations outside of transaction to prevent aborted transaction errors.
     */
    public $withinTransaction = false;

    /**
     * Enforce Row-Level Security policies
     *
     * SECURITY MODEL:
     * - Public catalogue data: READ-ONLY for anon & authenticated users
     * - System tables: RLS enabled (no public access)
     * - Service role (backend): FULL ACCESS via role grants (not policies)
     */
    public function up(): void
    {
        if (DB::getDriverName() !== 'pgsql') {
            return;
        }

        $this->info('🔐 Enabling Row-Level Security...');

        // ==========================================
        // PUBLIC READ-ONLY CATALOGUE DATA
        // ==========================================
        $publicTables = [
            'products',
            'video_game_titles',
            'video_games',
            'videos',
            'images',
            'video_game_sources',
            'video_game_title_sources',
            'video_game_prices',
            'provider_toplists',
            'provider_toplist_items',
            'media',
            'currencies',
            'exchange_rates',
            'tax_profiles',
            'countries',
            'retailers',
        ];

        foreach ($publicTables as $table) {
            if (! Schema::hasTable($table)) {
                continue;
            }

            try {
                DB::statement("ALTER TABLE public.{$table} ENABLE ROW LEVEL SECURITY");
                DB::statement("GRANT SELECT ON public.{$table} TO anon, authenticated");
                DB::statement("CREATE POLICY read_all_{$table} ON public.{$table} FOR SELECT TO anon, authenticated USING (true)");

                $this->info("  ✓ {$table}: Public read-only");
            } catch (\Throwable $e) {
                $this->warn("  ⚠ {$table}: {$e->getMessage()}");
            }
        }

        // ==========================================
        // MATERIALIZED VIEWS (NO RLS; GRANT SELECT ONLY)
        // ==========================================
        // Postgres does not support RLS on materialized views.
        // We still want anon/auth to be able to SELECT them.
        $publicMaterializedViews = [
            'video_games_ranked_mv',
            'video_games_genre_ranked_mv',
            'video_games_upcoming_mv',
            'video_games_toplists_mv',
        ];

        foreach ($publicMaterializedViews as $matView) {
            $exists = DB::selectOne(
                "select 1 as one from pg_matviews where schemaname = 'public' and matviewname = ? limit 1",
                [$matView],
            );

            if ($exists === null) {
                continue;
            }

            try {
                DB::statement("GRANT SELECT ON public.{$matView} TO anon, authenticated");
                $this->info("  ✓ {$matView}: Materialized view (public SELECT)");
            } catch (\Throwable $e) {
                $this->warn("  ⚠ {$matView}: {$e->getMessage()}");
            }
        }

        // ==========================================
        // MATERIALIZED VIEW REFRESH (SERVICE ROLE ONLY)
        // ==========================================
        // anon/authenticated MUST NOT be able to refresh.
        // Use a SECURITY DEFINER function and grant EXECUTE only to service_role.
        try {
            DB::statement(<<<'SQL'
CREATE OR REPLACE FUNCTION public.refresh_game_materialized_views()
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_matviews WHERE schemaname = 'public' AND matviewname = 'video_games_ranked_mv') THEN
    BEGIN
      REFRESH MATERIALIZED VIEW CONCURRENTLY public.video_games_ranked_mv;
    EXCEPTION WHEN OTHERS THEN
      REFRESH MATERIALIZED VIEW public.video_games_ranked_mv;
    END;
  END IF;

  IF EXISTS (SELECT 1 FROM pg_matviews WHERE schemaname = 'public' AND matviewname = 'video_games_genre_ranked_mv') THEN
    BEGIN
      REFRESH MATERIALIZED VIEW CONCURRENTLY public.video_games_genre_ranked_mv;
    EXCEPTION WHEN OTHERS THEN
      REFRESH MATERIALIZED VIEW public.video_games_genre_ranked_mv;
    END;
  END IF;

  IF EXISTS (SELECT 1 FROM pg_matviews WHERE schemaname = 'public' AND matviewname = 'video_games_upcoming_mv') THEN
    BEGIN
      REFRESH MATERIALIZED VIEW CONCURRENTLY public.video_games_upcoming_mv;
    EXCEPTION WHEN OTHERS THEN
      REFRESH MATERIALIZED VIEW public.video_games_upcoming_mv;
    END;
  END IF;

  IF EXISTS (SELECT 1 FROM pg_matviews WHERE schemaname = 'public' AND matviewname = 'video_games_toplists_mv') THEN
    BEGIN
      REFRESH MATERIALIZED VIEW CONCURRENTLY public.video_games_toplists_mv;
    EXCEPTION WHEN OTHERS THEN
      REFRESH MATERIALIZED VIEW public.video_games_toplists_mv;
    END;
  END IF;
END;
$$;
SQL);

            // Remove default EXECUTE for PUBLIC.
            DB::statement('REVOKE ALL ON FUNCTION public.refresh_game_materialized_views() FROM PUBLIC');

            // Supabase role (may not exist in non-Supabase PG).
            DB::statement('GRANT EXECUTE ON FUNCTION public.refresh_game_materialized_views() TO service_role');

            $this->info('  ✓ refresh_game_materialized_views(): service_role-only');
        } catch (\Throwable $e) {
            $this->warn('  ⚠ refresh_game_materialized_views(): '.$e->getMessage());
        }

        // ==========================================
        // SYSTEM TABLES (RLS enabled, no public access)
        // ==========================================
        $systemTables = [
            'users',
            'jobs',
            'job_batches',
            'sessions',
            'failed_jobs',
            'cache',
            'cache_locks',
            'migrations',
            'password_reset_tokens',
            'personal_access_tokens',
            'price_charting_igdb_mappings',
            'webhook_events',
            'telescope_entries',
            'telescope_entries_tags',
            'telescope_monitoring',
        ];

        foreach ($systemTables as $table) {
            if (! Schema::hasTable($table)) {
                continue;
            }

            try {
                DB::statement("ALTER TABLE public.{$table} ENABLE ROW LEVEL SECURITY");
                $this->info("  ✓ {$table}: System table (RLS enabled)");
            } catch (\Throwable $e) {
                $this->warn("  ⚠ {$table}: {$e->getMessage()}");
            }
        }

        $this->info('');
        $this->info('✅ Row-Level Security enabled!');
        $this->info('');
        $this->info('SECURITY SUMMARY:');
        $this->info('  📖 PUBLIC READ-ONLY: Catalogue data (anon/auth can SELECT)');
        $this->info('  🔒 SYSTEM TABLES: RLS enabled (service role access only)');
    }

    public function down(): void
    {
        if (DB::getDriverName() !== 'pgsql') {
            return;
        }

        $this->info('⏮️  Rolling back RLS policies...');

        $allTables = [
            // Public tables
            'products',
            'video_game_titles',
            'video_games',
            'videos',
            'images',
            'video_game_sources',
            'video_game_title_sources',
            'video_game_prices',
            'media',
            'currencies',
            'exchange_rates',
            'tax_profiles',
            'countries',
            'retailers',
            // System tables
            'users',
            'jobs',
            'job_batches',
            'sessions',
            'failed_jobs',
            'cache',
            'cache_locks',
            'migrations',
            'password_reset_tokens',
            'personal_access_tokens',
            'telescope_entries',
            'telescope_entries_tags',
            'telescope_monitoring',
        ];

        foreach ($allTables as $table) {
            if (! Schema::hasTable($table)) {
                continue;
            }

            try {
                // Drop all policies
                $policies = DB::select('SELECT policyname FROM pg_policies WHERE tablename = ?', [$table]);
                foreach ($policies as $policy) {
                    DB::statement("DROP POLICY IF EXISTS \"{$policy->policyname}\" ON public.{$table}");
                }

                // Revoke grants
                DB::statement("REVOKE SELECT ON public.{$table} FROM anon, authenticated");

                // Disable RLS
                DB::statement("ALTER TABLE public.{$table} DISABLE ROW LEVEL SECURITY");

                $this->info("  ↪ Reverted {$table}");
            } catch (\Throwable $e) {
                $this->warn("  ⚠ Could not revert {$table}: {$e->getMessage()}");
            }
        }

        // Materialized views
        $publicMaterializedViews = [
            'video_games_ranked_mv',
            'video_games_genre_ranked_mv',
            'video_games_upcoming_mv',
            'video_games_toplists_mv',
        ];

        foreach ($publicMaterializedViews as $matView) {
            try {
                DB::statement("REVOKE SELECT ON public.{$matView} FROM anon, authenticated");
            } catch (\Throwable $e) {
                // Ignore on rollback
            }
        }

        try {
            DB::statement('DROP FUNCTION IF EXISTS public.refresh_game_materialized_views()');
        } catch (\Throwable $e) {
            // Ignore on rollback
        }

        $this->info('✅ RLS policies reverted');
    }

    /**
     * Console output helpers
     */
    protected function info(string $message): void
    {
        $this->writeLine($message);
    }

    protected function warn(string $message): void
    {
        $this->writeLine($message);
    }

    protected function writeLine(string $message): void
    {
        if (app()->runningInConsole()) {
            fwrite(STDOUT, $message.PHP_EOL);
        }
    }
};
