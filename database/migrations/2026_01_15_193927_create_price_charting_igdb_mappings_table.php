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
        Schema::create('price_charting_igdb_mappings', function (Blueprint $table) {
            $table->id();

            // Price Charting data
            $table->string('price_charting_id')->index();
            $table->string('price_charting_name');
            $table->string('price_charting_console')->index();
            $table->string('price_charting_price')->nullable();

            // IGDB data
            $table->unsignedBigInteger('video_game_title_id')->index();
            $table->string('igdb_name');
            $table->text('igdb_platforms')->nullable(); // JSON array of platforms
            $table->string('igdb_slug')->nullable();
            $table->string('igdb_external_id')->nullable();

            // Match metadata
            $table->decimal('confidence_score', 3, 2)->default(1.00); // 0.00 to 1.00
            $table->string('match_type', 20)->default('exact'); // 'exact', 'fuzzy', 'manual'

            $table->timestamps();

            // Foreign key
            $table->foreign('video_game_title_id')
                ->references('id')
                ->on('video_game_titles')
                ->onDelete('cascade');

            // Unique constraint: one Price Charting game can only map to one IGDB game
            $table->unique(['price_charting_id', 'video_game_title_id']);
        });
    }

    /**
     * Reverse the migrations.
     */
    public function down(): void
    {
        Schema::dropIfExists('price_charting_igdb_mappings');
    }
};
