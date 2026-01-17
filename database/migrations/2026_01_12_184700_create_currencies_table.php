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
        Schema::create('currencies', function (Blueprint $table) {
            $table->id();
            $table->char('code', 3)->unique()->comment('ISO 4217 currency code (USD, EUR, BTC)');
            $table->string('name')->comment('Full currency name (US Dollar, Bitcoin)');
            $table->string('symbol', 10)->nullable()->comment('Currency symbol ($, €, ₿)');
            $table->smallInteger('decimals')->default(2)->comment('Number of decimal places (2 for USD, 0 for JPY, 8 for BTC)');
            $table->boolean('is_crypto')->default(false)->comment('Flag for cryptocurrency');
            $table->json('metadata')->nullable()->comment('Additional currency metadata');
            $table->timestamps();

            // Indexes
            $table->index('is_crypto');
            $table->index(['code', 'is_crypto']);
        });

        // Add FK constraint to video_game_prices
        if (Schema::hasTable('video_game_prices') && Schema::hasColumn('video_game_prices', 'currency')) {
            Schema::table('video_game_prices', function (Blueprint $table) {
                $table->foreign('currency')
                    ->references('code')
                    ->on('currencies')
                    ->onUpdate('cascade')
                    ->onDelete('restrict');
            });
        }
    }

    /**
     * Reverse the migrations.
     */
    public function down(): void
    {
        // Drop FK constraint first
        if (Schema::hasTable('video_game_prices') && Schema::hasColumn('video_game_prices', 'currency')) {
            Schema::table('video_game_prices', function (Blueprint $table) {
                $table->dropForeign(['currency']);
            });
        }

        Schema::dropIfExists('currencies');
    }
};
