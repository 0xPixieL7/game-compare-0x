<?php

declare(strict_types=1);

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    // Disable transactions for CONCURRENTLY index creation
    public $withinTransaction = false;

    public function up(): void
    {
        // Add optimized indexes for compare page performance
        Schema::table('video_game_title_sources', function (Blueprint $table) {
            // Composite index for join performance
            $table->index(['video_game_source_id', 'video_game_title_id'], 'vgts_source_title_idx');

            // Text search index for name lookups
            $table->index('name', 'vgts_name_idx');

            // Rating sorting index
            $table->index('rating', 'vgts_rating_idx');

            // Provider filtering index
            $table->index('provider', 'vgts_provider_idx');
        });

        Schema::table('video_game_titles', function (Blueprint $table) {
            // Text search index for name
            $table->index('name', 'vgt_name_idx');
        });

        // Add PostgreSQL-specific optimizations if using PostgreSQL
        if (DB::getDriverName() === 'pgsql') {
            // GIN index for better text search performance
            DB::statement('CREATE INDEX CONCURRENTLY IF NOT EXISTS vgts_name_gin_idx ON video_game_title_sources USING GIN (to_tsvector(\'english\', name))');
            DB::statement('CREATE INDEX CONCURRENTLY IF NOT EXISTS vgt_name_gin_idx ON video_game_titles USING GIN (to_tsvector(\'english\', name))');
        }
    }

    public function down(): void
    {
        Schema::table('video_game_title_sources', function (Blueprint $table) {
            $table->dropIndex('vgts_source_title_idx');
            $table->dropIndex('vgts_name_idx');
            $table->dropIndex('vgts_rating_idx');
            $table->dropIndex('vgts_provider_idx');
        });

        Schema::table('video_game_titles', function (Blueprint $table) {
            $table->dropIndex('vgt_name_idx');
        });

        if (DB::getDriverName() === 'pgsql') {
            DB::statement('DROP INDEX IF EXISTS vgts_name_gin_idx');
            DB::statement('DROP INDEX IF EXISTS vgt_name_gin_idx');
        }
    }
};
