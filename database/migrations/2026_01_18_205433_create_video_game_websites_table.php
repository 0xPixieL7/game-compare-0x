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
        Schema::create('video_game_websites', function (Blueprint $table) {
            $table->id();
            $table->foreignId('video_game_id')->constrained()->cascadeOnDelete();
            $table->integer('category')->index(); // IGDB Website Category Enum
            $table->string('url', 2048);
            $table->boolean('trusted')->default(false);
            $table->timestamps();

            $table->unique(['video_game_id', 'category', 'url']);
        });

        Schema::create('video_game_external_links', function (Blueprint $table) {
            $table->id();
            $table->foreignId('video_game_id')->constrained()->cascadeOnDelete();
            $table->integer('category')->index(); // IGDB External Game Category Enum
            $table->string('external_id')->index();
            $table->string('url', 2048)->nullable();
            $table->timestamps();

            $table->unique(['video_game_id', 'category', 'external_id']);
        });
    }

    /**
     * Reverse the migrations.
     */
    public function down(): void
    {
        Schema::dropIfExists('video_game_external_links');
        Schema::dropIfExists('video_game_websites');
    }
};
