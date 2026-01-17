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
        Schema::create('exchange_rates', function (Blueprint $table) {
            $table->id();
            $table->char('base_currency', 3)->comment('Source currency code (USD, EUR)');
            $table->char('quote_currency', 3)->comment('Target currency code (BTC, EUR)');
            $table->decimal('rate', 18, 8)->comment('Exchange rate (high precision for crypto)');
            $table->timestamp('fetched_at')->comment('When this rate was fetched from provider');
            $table->string('provider', 100)->nullable()->comment('Rate provider (coingecko, cryptocompare)');
            $table->json('metadata')->nullable()->comment('Additional rate metadata');
            $table->timestamps();

            // Foreign keys
            $table->foreign('base_currency')
                ->references('code')
                ->on('currencies')
                ->onUpdate('cascade')
                ->onDelete('restrict');

            $table->foreign('quote_currency')
                ->references('code')
                ->on('currencies')
                ->onUpdate('cascade')
                ->onDelete('restrict');

            // Indexes for fast lookups
            $table->index(['base_currency', 'quote_currency', 'fetched_at']);
            $table->index('fetched_at');
            $table->index('provider');

            // Unique constraint: one rate per currency pair per fetch time
            $table->unique(['base_currency', 'quote_currency', 'fetched_at'], 'unique_rate_per_fetch');
        });
    }

    /**
     * Reverse the migrations.
     */
    public function down(): void
    {
        Schema::dropIfExists('exchange_rates');
    }
};
