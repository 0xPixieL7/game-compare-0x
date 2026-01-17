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
        Schema::create('countries', function (Blueprint $table) {
            $table->id();
            $table->char('code', 2)->unique()->comment('ISO 3166-1 alpha-2 country code');
            $table->string('name');
            $table->foreignId('currency_id')->constrained('currencies')->restrictOnDelete();
            $table->string('region')->nullable()->comment('Geographic region like "North America", "Europe"');
            $table->json('metadata')->nullable()->comment('Additional country-specific data');
            $table->timestamps();

            $table->index('region');
            $table->index('currency_id');
        });
    }

    /**
     * Reverse the migrations.
     */
    public function down(): void
    {
        Schema::dropIfExists('countries');
    }
};
