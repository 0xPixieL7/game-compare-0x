<?php

declare(strict_types=1);

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    public function up(): void
    {
        Schema::create('videos', function (Blueprint $table) {
            $table->id();

            // UUID for Spatie compatibility
            $table->uuid('uuid')->nullable();

            // Collection names stored as JSON array (trailers, gameplay, previews)
            $table->json('collection_names')->nullable()
                ->comment('JSON array of collection names: trailers, gameplay, previews');

            // Primary collection
            $table->string('primary_collection')->nullable()
                ->comment('Primary collection name for quick filtering');

            // Polymorphic relation target for videos (e.g. App\Models\VideoGame).
            $table->morphs('videoable');

            // Optional direct link to a video game for convenience / legacy code.
            $table->foreignId('video_game_id')->nullable()->constrained('video_games')->cascadeOnDelete();

            // Keep detailed fields (legacy/rich schema) while also supporting one-row-per-game arrays.
            $table->foreignId('media_id')->nullable()->constrained('media')->cascadeOnDelete();

            // Primary video URL used by imports (e.g. IGDB/YouTube URLs).
            $table->text('url');

            // Provider-specific video ID
            $table->string('video_id')->nullable();

            // External ID for consistency with images
            $table->string('external_id')->nullable()
                ->comment('Provider-specific video ID');

            // Optional original/source URL if different from the canonical URL.
            $table->text('source_url')->nullable();
            $table->json('urls')->nullable()->comment('Array of video URLs or video IDs');
            $table->string('provider')->nullable()->comment('youtube, vimeo, etc.');
            $table->unsignedInteger('duration')->nullable()->comment('Duration in seconds');
            $table->unsignedInteger('width')->nullable();
            $table->unsignedInteger('height')->nullable();
            $table->text('thumbnail_url')->nullable();
            $table->string('title')->nullable();
            $table->text('description')->nullable();

            // Order column for Spatie sorting
            $table->unsignedInteger('order_column')->nullable();

            $table->json('metadata')->nullable()->comment('Duration, width, height, thumbnails, titles, etc.');
            $table->timestamps();

            // Ensure we only store one row per (videoable_type, videoable_id, url).
            $table->unique(['videoable_type', 'videoable_id', 'url'], 'videos_videoable_url_unique');

            // Indexes
            $table->index('uuid');
            $table->index('primary_collection');
            $table->index('video_game_id');
            $table->index('media_id');
            $table->index('external_id');
            $table->index('provider');
            $table->index('order_column');
            $table->index(['provider', 'video_id']);
            $table->index(['videoable_type', 'videoable_id', 'primary_collection'], 'videos_model_collection_idx');
        });
    }

    public function down(): void
    {
        Schema::dropIfExists('videos');
    }
};
