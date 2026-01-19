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
        Schema::create('video_game_alternative_names', function (Blueprint $table) {
            $table->id();
            $table->foreignId('video_game_id')->constrained()->cascadeOnDelete();
            $table->string('name')->index();
            $table->string('comment')->nullable();
            $table->timestamps();

            $table->unique(['video_game_id', 'name']);
        });
    }

    /**
     * Reverse the migrations.
     */
    public function down(): void
    {
        Schema::dropIfExists('video_game_alternative_names');
    }
};
