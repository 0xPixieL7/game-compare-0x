<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    /**
     * Run the migrations.
     */
    public function up(): void
    {
        Schema::table('video_game_prices', function (Blueprint $table) {
            // PERFORMANCE: Composite index for show page query (video_game_id + is_active + amount_minor)
            // Optimizes: WHERE video_game_id = ? AND is_active = true ORDER BY amount_minor ASC LIMIT 50
            $table->index(['video_game_id', 'is_active', 'amount_minor'], 'idx_vgp_show_page_query');
        });
    }

    /**
     * Reverse the migrations.
     */
    public function down(): void
    {
        Schema::table('video_game_prices', function (Blueprint $table) {
            $table->dropIndex('idx_vgp_show_page_query');
        });
    }
};
