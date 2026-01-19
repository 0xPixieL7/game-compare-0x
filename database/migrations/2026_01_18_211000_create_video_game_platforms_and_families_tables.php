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
        Schema::create('video_game_platform_families', function (Blueprint $table) {
            $table->id();
            $table->string('name')->unique();
            $table->string('slug')->unique();
            $table->timestamps();
        });

        Schema::create('video_game_platforms', function (Blueprint $table) {
            $table->id();
            $table->foreignId('platform_family_id')->nullable()->constrained('video_game_platform_families')->nullOnDelete();
            $table->string('name')->unique();
            $table->string('slug')->unique();
            $table->string('abbreviation')->nullable();
            $table->text('summary')->nullable();
            $table->string('logo_path')->nullable();
            $table->timestamps();
        });
    }

    /**
     * Reverse the migrations.
     */
    public function down(): void
    {
        Schema::dropIfExists('video_game_platforms');
        Schema::dropIfExists('video_game_platform_families');
    }
};
