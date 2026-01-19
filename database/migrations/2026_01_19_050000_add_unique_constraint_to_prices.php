<?php

declare(strict_types=1);

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    public function up(): void
    {
        Schema::table('video_game_prices', function (Blueprint $table) {
            // Drop duplicates first if any (cleanup)
            // This is tricky in migration, assuming empty or clean for now for this specific tuple
            
            // Add unique constraint for upsert support
            $table->unique(['video_game_id', 'retailer', 'country_code'], 'vgp_unique_pricing_idx');
        });
    }

    public function down(): void
    {
        Schema::table('video_game_prices', function (Blueprint $table) {
            $table->dropUnique('vgp_unique_pricing_idx');
        });
    }
};
