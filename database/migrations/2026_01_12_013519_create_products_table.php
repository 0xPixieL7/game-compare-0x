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
        Schema::create('products', function (Blueprint $table) {
            $table->id();

            /**
             * IMPORTANT DOMAIN INVARIANT:
             * `products` do NOT relate to `video_games` directly.
             * The ONLY allowed traversal is:
             *   products (1) -> video_game_titles (many) -> video_games (many)
             */

            // "video_game" and "console" are expected types.
            $table->string('type')->default('video_game');

            $table->timestamps();
            $table->string('name');
            $table->string('slug')->unique();

            // Platform for this product (e.g., "PlayStation 5", "Xbox Series X")
            $table->string('platform')->nullable();

            // Category (e.g., "action", "rpg", "sports")
            $table->string('category')->nullable();

            // Optional display/normalized fields used for grouping & search.
            $table->string('title')->nullable();
            $table->string('normalized_title')->nullable();

            $table->text('synopsis')->nullable();

            // Release date for sorting/filtering
            $table->date('release_date')->nullable();

            // Popularity score for ranking
            $table->decimal('popularity_score', 12, 4)->nullable();

            // Rating (0-100 scale)
            $table->decimal('rating', 5, 2)->nullable();

            // External IDs from various providers (IGDB, Steam, etc.)
            $table->jsonb('external_ids')->nullable();

            // Flexible metadata storage
            $table->jsonb('metadata')->nullable();

            $table->index(['type']);
            $table->index(['normalized_title']);
            $table->index(['platform']);
            $table->index(['release_date']);
        });
    }

    /**
     * Reverse the migrations.
     */
    public function down(): void
    {
        Schema::dropIfExists('products');
    }
};
