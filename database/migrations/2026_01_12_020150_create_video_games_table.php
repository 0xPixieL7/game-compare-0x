<?php

declare(strict_types=1);

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    public function up(): void
    {
        Schema::create('video_games', function (Blueprint $table) {
            $table->id();
            $table->foreignId('video_game_title_id')
                ->constrained('video_game_titles')
                ->cascadeOnDelete();
            $table->string('slug')->index();
            $table->string('provider');
            $table->unsignedBigInteger('external_id');
            $table->unique(['provider', 'external_id']);
            $table->string('name')->nullable();
            $table->text('description')->nullable();
            $table->text('summary')->nullable();
            $table->text('storyline')->nullable();
            $table->string('url')->nullable();
            $table->date('release_date')->nullable();
            $table->json('platform')->nullable();
            $table->decimal('rating', 20, 10)->nullable();
            $table->unsignedInteger('rating_count')->nullable();
            $table->string('developer')->nullable();
            $table->string('publisher')->nullable();
            $table->json('genre')->nullable();
            $table->json('media')->nullable();
            $table->decimal('opencritic_score', 5, 2)->nullable()
                ->comment('OpenCritic Top Critic Average score (0-100)');
            $table->unsignedInteger('opencritic_review_count')->nullable()
                ->comment('Number of critic reviews aggregated');
            $table->string('opencritic_tier')->nullable()
                ->comment('OpenCritic tier: Mighty, Strong, Fair, Weak');
            $table->decimal('opencritic_user_score', 5, 2)->nullable()
                ->comment('OpenCritic user average score (0-100)');
            $table->unsignedInteger('opencritic_user_count')->nullable()
                ->comment('Number of user ratings');
            $table->decimal('opencritic_percent_recommended', 5, 2)->nullable()
                ->comment('Percentage of critics who recommended the game (0-100)');
            $table->unsignedBigInteger('opencritic_id')->nullable()->unique()
                ->comment('OpenCritic game ID for API lookups');
            $table->timestamp('opencritic_updated_at')->nullable()
                ->comment('Last time OpenCritic data was fetched');
            $table->json('source_payload')->nullable();
            $table->timestamps();
            $table->unique('video_game_title_id');
            $table->index('video_game_title_id');
            $table->index(['opencritic_score', 'opencritic_review_count']);
            // Indexes from optimizations
            $table->index('rating', 'video_games_rating_idx');
            $table->index('name', 'video_games_name_idx');
            $table->index('release_date', 'video_games_release_date_idx');
            $table->index('created_at', 'video_games_created_at_idx');
            $table->index('updated_at', 'video_games_updated_at_idx');
        });
    }

    public function down(): void
    {
        Schema::dropIfExists('video_games');
    }
};
