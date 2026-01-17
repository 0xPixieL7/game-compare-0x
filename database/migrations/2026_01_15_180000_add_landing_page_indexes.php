<?php

declare(strict_types=1);

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    public function up(): void
    {
        // Add optimized indexes for landing page queries
        Schema::table('video_games', function (Blueprint $table) {
            // Rating ordering index (top rated section)
            $table->index('rating', 'vg_rating_idx');

            // Release date ordering index (new releases section)
            $table->index('release_date', 'vg_release_date_idx');
        });

        Schema::table('video_game_title_sources', function (Blueprint $table) {
            // Composite index for genre + rating sorts (both exist and are indexable)
            $table->index(['video_game_title_id', 'rating'], 'vgts_title_rating_idx');
        });

        Schema::table('video_game_prices', function (Blueprint $table) {
            // Composite index for latest price queries
            $table->index(['video_game_id', 'recorded_at'], 'vgp_game_recorded_idx');

            // Currency filtering index
            $table->index('currency', 'vgp_currency_idx');

            // Composite index for price sorting
            $table->index(['video_game_id', 'amount_minor'], 'vgp_game_amount_idx');
        });

        Schema::table('images', function (Blueprint $table) {
            // Direct video_game_id lookup (optimized join)
            if (! $this->indexExists('images', 'img_video_game_id_idx')) {
                $table->index('video_game_id', 'img_video_game_id_idx');
            }
        });
    }

    public function down(): void
    {
        Schema::table('video_games', function (Blueprint $table) {
            $table->dropIndex('vg_rating_idx');
            $table->dropIndex('vg_release_date_idx');
        });

        Schema::table('video_game_title_sources', function (Blueprint $table) {
            $table->dropIndex('vgts_title_rating_idx');
        });

        Schema::table('video_game_prices', function (Blueprint $table) {
            $table->dropIndex('vgp_game_recorded_idx');
            $table->dropIndex('vgp_currency_idx');
            $table->dropIndex('vgp_game_amount_idx');
        });

        Schema::table('images', function (Blueprint $table) {
            if ($this->indexExists('images', 'img_video_game_id_idx')) {
                $table->dropIndex('img_video_game_id_idx');
            }
        });
    }

    private function indexExists(string $table, string $index): bool
    {
        $driver = DB::getDriverName();

        if ($driver === 'sqlite') {
            $result = DB::select(
                "SELECT 1 FROM sqlite_master WHERE type = 'index' AND tbl_name = ? AND name = ?",
                [$table, $index]
            );
        } else {
            // PostgreSQL
            $result = DB::select(
                'SELECT 1 FROM pg_indexes WHERE tablename = ? AND indexname = ?',
                [$table, $index]
            );
        }

        return count($result) > 0;
    }
};
