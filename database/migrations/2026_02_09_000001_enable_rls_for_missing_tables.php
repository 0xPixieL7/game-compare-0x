<?php

declare(strict_types=1);

use Illuminate\Database\Migrations\Migration;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    public $withinTransaction = false;

    public function up(): void
    {
        if (DB::getDriverName() !== 'pgsql') {
            return;
        }

        // Fail fast instead of hanging on locks (Supabase dashboard runs background queries).
        DB::statement("SET lock_timeout = '5s'");
        DB::statement("SET statement_timeout = '30s'");

        // Public read-only
        $publicTables = [
            'provider_toplists',
            'provider_toplist_items',
        ];

        foreach ($publicTables as $table) {
            if (! Schema::hasTable($table)) {
                continue;
            }

            try {
                DB::statement("ALTER TABLE public.{$table} ENABLE ROW LEVEL SECURITY");
                DB::statement("GRANT SELECT ON public.{$table} TO anon, authenticated");
                DB::statement("CREATE POLICY read_all_{$table} ON public.{$table} FOR SELECT TO anon, authenticated USING (true)");
            } catch (Throwable $e) {
                // best-effort / idempotent
            }
        }

        // System tables (RLS on, no public grants/policies)
        $systemTables = [
            'price_charting_igdb_mappings',
            'webhook_events',
        ];

        foreach ($systemTables as $table) {
            if (! Schema::hasTable($table)) {
                continue;
            }

            try {
                DB::statement("ALTER TABLE public.{$table} ENABLE ROW LEVEL SECURITY");
                DB::statement("REVOKE ALL ON public.{$table} FROM anon, authenticated");
            } catch (Throwable $e) {
                // best-effort / idempotent
            }
        }
    }

    public function down(): void
    {
        if (DB::getDriverName() !== 'pgsql') {
            return;
        }

        DB::statement("SET lock_timeout = '5s'");
        DB::statement("SET statement_timeout = '30s'");

        // Rollback is best-effort; we only remove what we added.
        foreach (['provider_toplists', 'provider_toplist_items'] as $table) {
            if (! Schema::hasTable($table)) {
                continue;
            }

            try {
                DB::statement("DROP POLICY IF EXISTS read_all_{$table} ON public.{$table}");
                DB::statement("REVOKE SELECT ON public.{$table} FROM anon, authenticated");
            } catch (Throwable $e) {
                // ignore
            }
        }

        foreach (['price_charting_igdb_mappings', 'webhook_events'] as $table) {
            if (! Schema::hasTable($table)) {
                continue;
            }

            try {
                DB::statement("ALTER TABLE public.{$table} DISABLE ROW LEVEL SECURITY");
            } catch (Throwable $e) {
                // ignore
            }
        }
    }
};
