<?php

declare(strict_types=1);

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    public function withinTransaction(): bool
    {
        // Required for CREATE INDEX CONCURRENTLY on Postgres.
        return false;
    }

    public function up(): void
    {
        $driver = DB::connection()->getDriverName();

        if ($driver === 'pgsql') {
            DB::statement(
                'CREATE INDEX CONCURRENTLY IF NOT EXISTS vgts_provider_external_id_idx '
                .'ON public.video_game_title_sources (provider, external_id)'
            );

            DB::statement(
                'CREATE INDEX CONCURRENTLY IF NOT EXISTS vgts_provider_provider_item_id_idx '
                .'ON public.video_game_title_sources (provider, provider_item_id)'
            );

            DB::statement(
                'CREATE INDEX CONCURRENTLY IF NOT EXISTS vgts_title_provider_idx '
                .'ON public.video_game_title_sources (video_game_title_id, provider)'
            );

            return;
        }

        // SQLite/local fallback (no CONCURRENTLY).
        Schema::table('video_game_title_sources', function (Blueprint $table) {
            $table->index(['provider', 'external_id'], 'vgts_provider_external_id_idx');
            $table->index(['provider', 'provider_item_id'], 'vgts_provider_provider_item_id_idx');
            $table->index(['video_game_title_id', 'provider'], 'vgts_title_provider_idx');
        });
    }

    public function down(): void
    {
        $driver = DB::connection()->getDriverName();

        if ($driver === 'pgsql') {
            DB::statement('DROP INDEX CONCURRENTLY IF EXISTS public.vgts_provider_external_id_idx');
            DB::statement('DROP INDEX CONCURRENTLY IF EXISTS public.vgts_provider_provider_item_id_idx');
            DB::statement('DROP INDEX CONCURRENTLY IF EXISTS public.vgts_title_provider_idx');

            return;
        }

        Schema::table('video_game_title_sources', function (Blueprint $table) {
            $table->dropIndex('vgts_provider_external_id_idx');
            $table->dropIndex('vgts_provider_provider_item_id_idx');
            $table->dropIndex('vgts_title_provider_idx');
        });
    }
};
