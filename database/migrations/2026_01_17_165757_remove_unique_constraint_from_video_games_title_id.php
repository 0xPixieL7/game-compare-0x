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
        Schema::table('video_games', function (Blueprint $table) {
            $table->dropUnique('video_games_video_game_title_id_unique');
        });
    }

    /**
     * Reverse the migrations.
     */
    public function down(): void
    {
        Schema::table('video_games', function (Blueprint $table) {
            $table->unique('video_game_title_id');
        });
    }
};
