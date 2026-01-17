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
            'media',
            'currencies',
            'exchange_rates',
            'tax_profiles',
            'countries',
            'retailers',
            'video_game_profiles',
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
            'video_game_profiles',
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
