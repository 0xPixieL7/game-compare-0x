<?php

declare(strict_types=1);

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    /**
     * Run the migrations for enrichment pipeline optimization.
     */
    public function up(): void
    {
        // video_game_prices compound index for price lookups
        Schema::table('video_game_prices', function (Blueprint $table) {
            // Compound index for efficient price fetching during enrichment
            // Supports: WHERE video_game_id = X AND retailer = Y AND country_code = Z ORDER BY created_at DESC
            $table->index(
                ['video_game_id', 'retailer', 'country_code', 'created_at'],
                'idx_prices_enrichment_lookup'
            );

            // Retailer-specific lookup for provider-based enrichment
            $table->index(['retailer', 'is_active'], 'idx_prices_retailer_active');
        });

        // images compound indexes for media enrichment
        Schema::table('images', function (Blueprint $table) {
            // Provider + video_game_id for lookup (url is TEXT, use existing unique index)
            $table->index(
                ['video_game_id', 'provider'],
                'idx_images_enrichment_lookup'
            );

            // Provider + external_id for IGDB/TGDB media matching
            $table->index(['provider', 'external_id'], 'idx_images_provider_external');
        });

        // videos compound indexes for media enrichment
        Schema::table('videos', function (Blueprint $table) {
            // Provider + video_game_id for lookup (url is TEXT, use existing checks)
            $table->index(
                ['video_game_id', 'provider'],
                'idx_videos_enrichment_lookup'
            );

            // Provider + external_id for IGDB/TGDB media matching
            $table->index(['provider', 'external_id'], 'idx_videos_provider_external');
        });

        // video_games last_enriched_at for stale detection (future enhancement)
        Schema::table('video_games', function (Blueprint $table) {
            // Add last_enriched_at timestamp for tracking enrichment freshness
            $table->timestamp('last_enriched_at')->nullable()->after('updated_at');

            // Index for finding stale games that need re-enrichment
            $table->index('last_enriched_at', 'idx_games_last_enriched');
        });
    }

    /**
     * Reverse the migrations.
     */
    public function down(): void
    {
        Schema::table('video_game_prices', function (Blueprint $table) {
            $table->dropIndex('idx_prices_enrichment_lookup');
            $table->dropIndex('idx_prices_retailer_active');
        });

        Schema::table('images', function (Blueprint $table) {
            $table->dropIndex('idx_images_enrichment_lookup');
            $table->dropIndex('idx_images_provider_external');
        });

        Schema::table('videos', function (Blueprint $table) {
            $table->dropIndex('idx_videos_enrichment_lookup');
            $table->dropIndex('idx_videos_provider_external');
        });

        Schema::table('video_games', function (Blueprint $table) {
            $table->dropIndex('idx_games_last_enriched');
            $table->dropColumn('last_enriched_at');
        });
    }
};
