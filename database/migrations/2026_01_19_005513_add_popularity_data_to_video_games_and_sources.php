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
        Schema::table('video_games', function (Blueprint $table) {
            if (! Schema::hasColumn('video_games', 'hypes')) {
                $table->unsignedInteger('hypes')->nullable()->index();
            }
            if (! Schema::hasColumn('video_games', 'follows')) {
                $table->unsignedInteger('follows')->nullable()->index();
            }
            if (! Schema::hasColumn('video_games', 'popularity_score')) {
                $table->decimal('popularity_score', 20, 10)->nullable()->index();
            }
        });

        Schema::table('video_game_title_sources', function (Blueprint $table) {
            if (! Schema::hasColumn('video_game_title_sources', 'hypes')) {
                $table->unsignedInteger('hypes')->nullable();
            }
            if (! Schema::hasColumn('video_game_title_sources', 'follows')) {
                $table->unsignedInteger('follows')->nullable();
            }
            if (! Schema::hasColumn('video_game_title_sources', 'popularity_score')) {
                $table->decimal('popularity_score', 20, 10)->nullable();
            }
        });
    }

    /**
     * Reverse the migrations.
     */
    public function down(): void
    {
        Schema::table('video_games', function (Blueprint $table) {
            $table->dropColumn(['hypes', 'follows', 'popularity_score']);
        });

        Schema::table('video_game_title_sources', function (Blueprint $table) {
            $table->dropColumn(['hypes', 'follows', 'popularity_score']);
        });
    }
};
