<?php

declare(strict_types=1);

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    /**
     * Run the migrations.
     *
     * Adds IGDB-specific engagement metrics and attributes to video_games table.
     */
    public function up(): void
    {
        Schema::table('video_games', function (Blueprint $table) {
            // IGDB hype count: tracks user excitement/anticipation for upcoming games
            $table->unsignedInteger('hypes')
                ->nullable()
                ->after('rating_count')
                ->comment('IGDB hype count - user excitement metric for upcoming games');

            // IGDB follows: number of users following/tracking this game
            $table->unsignedInteger('follows')
                ->nullable()
                ->after('hypes')
                ->comment('IGDB follows - number of users tracking this game');

            // Note: 'attributes' column already exists in production (JSON type)
            // Skipping creation to avoid duplicate column error
            // If needed, a separate migration can convert JSON -> JSONB

            // Indexes for sorting/filtering by engagement metrics
            $table->index('hypes', 'video_games_hypes_idx');
            $table->index('follows', 'video_games_follows_idx');
        });
    }

    /**
     * Reverse the migrations.
     */
    public function down(): void
    {
        Schema::table('video_games', function (Blueprint $table) {
            // Drop indexes first
            $table->dropIndex('video_games_hypes_idx');
            $table->dropIndex('video_games_follows_idx');

            // Only drop columns added by this migration (not 'attributes')
            $table->dropColumn(['hypes', 'follows']);
        });
    }
};
