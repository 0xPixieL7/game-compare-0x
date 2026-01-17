<?php

declare(strict_types=1);

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    public function up(): void
    {
        Schema::create('images', function (Blueprint $table) {
            $table->id();

            // UUID for Spatie compatibility
            $table->uuid('uuid')->nullable();

            // Collection names stored as JSON array (covers, screenshots, artworks)
            $table->json('collection_names')->nullable()
                ->comment('JSON array of collection names: covers, screenshots, artworks');

            // Primary collection (the main/first collection for this row)
            $table->string('primary_collection')->nullable()
                ->comment('Primary collection name for quick filtering');

            // Polymorphic relation target for images (e.g. App\Models\VideoGame).
            $table->morphs('imageable');

            // Optional direct link to a video game for convenience / legacy code.
            $table->foreignId('video_game_id')->nullable()->constrained('video_games')->cascadeOnDelete();

            // Keep detailed fields (legacy/rich schema) while also supporting one-row-per-game arrays.
            $table->foreignId('media_id')->nullable()->constrained('media')->cascadeOnDelete();

            // Primary image URL used by imports (e.g. IGDB URLs).
            $table->text('url');

            // External ID for deduplication (e.g., IGDB image_id like "co1234")
            $table->string('external_id')->nullable()
                ->comment('Provider-specific image ID (e.g., IGDB image_id)');

            // Provider source (igdb, steam, rawg, etc.)
            $table->string('provider')->nullable()
                ->comment('Media provider source');

            // Optional original/source URL if different from the canonical URL.
            $table->text('source_url')->nullable();
            $table->unsignedInteger('width')->nullable();
            $table->unsignedInteger('height')->nullable();
            $table->text('alt_text')->nullable();
            $table->text('caption')->nullable();
            $table->boolean('is_thumbnail')->default(false);

            // Order column for Spatie sorting compatibility
            $table->unsignedInteger('order_column')->nullable();

            $table->json('urls')->nullable()->comment('Array of image URLs');
            $table->json('metadata')->nullable()->comment('Width, height, alt text, captions, etc.');
            $table->timestamps();

            // Ensure we only store one row per (imageable_type, imageable_id, url).
            $table->unique(['imageable_type', 'imageable_id', 'url'], 'images_imageable_url_unique');

            // Indexes
            $table->index('uuid');
            $table->index('primary_collection');
            $table->index('video_game_id');
            $table->index('media_id');
            $table->index('external_id');
            $table->index('provider');
            $table->index('is_thumbnail');
            $table->index('order_column');
            $table->index(['imageable_type', 'imageable_id', 'primary_collection'], 'images_model_collection_idx');
        });
    }

    public function down(): void
    {
        Schema::dropIfExists('images');
    }
};
