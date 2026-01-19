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
     * Run the migrations.
     */
    public function up(): void
    {
        if (DB::getDriverName() !== 'pgsql') {
            return;
        }

        $auxiliaryTables = [
            'video_game_websites',
            'video_game_external_links',
            'video_game_alternative_names',
            'video_game_platforms',
            'video_game_platform_families',
        ];

        foreach ($auxiliaryTables as $table) {
            if (! Schema::hasTable($table)) {
                continue;
            }

            try {
                // Enable RLS
                DB::statement("ALTER TABLE public.{$table} ENABLE ROW LEVEL SECURITY");

                // Grant SELECT to anon and authenticated roles (Supabase standard)
                DB::statement("GRANT SELECT ON public.{$table} TO anon, authenticated");

                // Create the policy for public read access
                DB::statement("CREATE POLICY read_all_{$table} ON public.{$table} FOR SELECT TO anon, authenticated USING (true)");

                if (app()->runningInConsole()) {
                    fwrite(STDOUT, "  ✓ {$table}: RLS enabled with public read policy".PHP_EOL);
                }
            } catch (\Throwable $e) {
                if (app()->runningInConsole()) {
                    fwrite(STDOUT, "  ⚠ {$table}: ".$e->getMessage().PHP_EOL);
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

        $auxiliaryTables = [
            'video_game_websites',
            'video_game_external_links',
            'video_game_alternative_names',
            'video_game_platforms',
            'video_game_platform_families',
        ];

        foreach ($auxiliaryTables as $table) {
            if (! Schema::hasTable($table)) {
                continue;
            }

            try {
                DB::statement("DROP POLICY IF EXISTS read_all_{$table} ON public.{$table}");
                DB::statement("REVOKE SELECT ON public.{$table} FROM anon, authenticated");
                DB::statement("ALTER TABLE public.{$table} DISABLE ROW LEVEL SECURITY");
            } catch (\Throwable $e) {
                // Ignore errors on rollback
            }
        }
    }
};
