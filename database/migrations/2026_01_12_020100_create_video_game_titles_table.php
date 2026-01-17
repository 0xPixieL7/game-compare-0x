<?php

declare(strict_types=1);

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    public function up(): void
    {
        // Guard against duplicate creation if the table already exists
        if (Schema::hasTable('video_game_titles')) {
            return; // Table already present; invariant still enforced via FKs and uniques
        }

        Schema::create('video_game_titles', function (Blueprint $table) {
            $table->id();

            /**
             * CRITICAL SCHEMA INVARIANT:
             * ONLY `video_game_titles` have `product_id`.
             * `products` relate to `video_games` ONLY through this table:
             *   products (1) -> video_game_titles (many) -> video_games (many)
             */
            $table->foreignId('product_id')
                ->constrained('products')
                ->cascadeOnDelete();

            // Canonical display title for this source item.
            $table->string('name');

            // Normalized title to group variants.
            $table->string('normalized_title')->nullable();

            // URL-friendly slug for this video game title.
            $table->string('slug')->unique();

            /**
             * List of providers that contribute metadata/media for this title.
             *
             * NOTE:
             * Provider item IDs and mirrored payloads live in `video_game_title_sources`.
             */
            $table->json('providers')->nullable();

            $table->timestamps();

            $table->index(['product_id']);
            $table->index(['normalized_title']);
        });
    }

    public function down(): void
    {
        Schema::dropIfExists('video_game_titles');
    }
};
