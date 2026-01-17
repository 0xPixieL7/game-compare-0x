<?php

declare(strict_types=1);

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    public function up(): void
    {
        Schema::create('video_game_prices', function (Blueprint $table) {
            $table->id();

            $table->foreignId('video_game_id')
                ->constrained('video_games')
                ->cascadeOnDelete();

            // Direct product association (maps from sku_regions.csv)
            $table->foreignId('product_id')
                ->nullable()
                ->constrained('products')
                ->nullOnDelete();

            // ISO 4217 (USD, EUR, JPY, ...)
            $table->char('currency', 3);

            // ISO 2-letter country code (US, GB, JP, EU, etc.)
            $table->char('country_code', 2)->nullable();

            // Region code for regional pricing
            $table->char('region_code', 2)->nullable();

            // Item condition (new, used, refurbished, etc.) - for PriceCharting
            $table->string('condition')->nullable();

            // Store in minor units (e.g. cents) for correctness.
            $table->unsignedBigInteger('amount_minor');

            $table->timestamp('recorded_at');

            $table->string('retailer')->nullable();
            $table->text('url')->nullable();
            $table->boolean('tax_inclusive')->default(false);

            // SKU identifier from retailer
            $table->string('sku')->nullable();

            // Active flag for current prices
            $table->boolean('is_active')->default(true);

            // PriceCharting specific fields
            $table->boolean('is_retail_buy')->default(false);
            $table->integer('sales_volume')->nullable();

            // Metadata JSON for flexible extra data
            $table->jsonb('metadata')->nullable();

            $table->timestamps();

            // Indexes
            $table->index(['video_game_id', 'recorded_at']);
            $table->index('product_id');
            $table->index('country_code');
            $table->index('region_code');
            $table->index('condition');
            $table->index(['product_id', 'region_code']);
        });
    }

    public function down(): void
    {
        Schema::dropIfExists('video_game_prices');
    }
};
