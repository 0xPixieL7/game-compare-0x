<?php

declare(strict_types=1);

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
            // Link to the actual retailers table
            $table->foreignId('retailer_id')
                ->after('retailer')
                ->nullable()
                ->constrained('retailers')
                ->nullOnDelete();

            // Add BTC normalization column for historical tracking
            $table->decimal('amount_btc', 20, 10)->nullable()->after('amount_minor');

            // Add a unique constraint to prevent duplicate price points for the same item
            // We use a shorter name to avoid PostgreSQL's 63-character limit
            $table->unique(
                ['video_game_id', 'retailer', 'currency', 'country_code', 'condition', 'sku'],
                'idx_vgp_unique_price_point'
            );
        });
    }

    /**
     * Reverse the migrations.
     */
    public function down(): void
    {
        Schema::table('video_game_prices', function (Blueprint $table) {
            $table->dropUnique('idx_vgp_unique_price_point');
            $table->dropConstrainedForeignId('retailer_id');
            $table->dropColumn('amount_btc');
        });
    }
};
