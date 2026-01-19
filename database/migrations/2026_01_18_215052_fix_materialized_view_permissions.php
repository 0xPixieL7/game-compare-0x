<?php

declare(strict_types=1);

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

        // Supabase/PostgreSQL does not support RLS on Materialized Views.
        // Access must be granted explicitly via standard SQL GRANTS.
        $materializedViews = [
            'video_game_title_sources_mv',
        ];

        foreach ($materializedViews as $view) {
            try {
                // Grant SELECT access to anon and authenticated roles
                DB::statement("GRANT SELECT ON public.{$view} TO anon, authenticated");

                if (app()->runningInConsole()) {
                    fwrite(STDOUT, "  ✓ {$view}: Explicit SELECT granted to anon/authenticated".PHP_EOL);
                }
            } catch (\Throwable $e) {
                if (app()->runningInConsole()) {
                    fwrite(STDOUT, "  ⚠ {$view}: ".$e->getMessage().PHP_EOL);
                }
            }
        }
    }

    /**
     * Reverse the migrations.
     */
    public function down(): void
    {
        if (DB::getDriverName() !== 'pgsql') {
            return;
        }

        $materializedViews = [
            'video_game_title_sources_mv',
        ];

        foreach ($materializedViews as $view) {
            try {
                DB::statement("REVOKE SELECT ON public.{$view} FROM anon, authenticated");
            } catch (\Throwable $e) {
                // Ignore errors on rollback
            }
        }
    }
};
