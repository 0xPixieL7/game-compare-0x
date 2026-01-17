<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    public function up(): void
    {
        Schema::create('video_game_title_sources', function (Blueprint $table) {
            $table->id();

            $table->foreignId('video_game_title_id')
                ->constrained('video_game_titles')
                ->cascadeOnDelete();

            $table->foreignId('video_game_source_id')
                ->constrained('video_game_sources')
                ->cascadeOnDelete();

            // Provider identity
            $table->string('provider');           // e.g. 'igdb', 'steam', 'psn'
            $table->unsignedBigInteger('external_id'); // provider's game ID

            // Optional: slug/name as seen on that provider
            $table->string('slug')->nullable();
            $table->string('name')->nullable();

            // Optional denormalized fields (per-provider)
            $table->text('description')->nullable();
            $table->date('release_date')->nullable();
            $table->unsignedBigInteger('provider_item_id');
            $table->json('platform')->nullable();
            $table->decimal('rating', 20, 10)->nullable();
            $table->unsignedInteger('rating_count')->nullable();

            $table->string('developer')->nullable();
            $table->string('publisher')->nullable();
            $table->json('genre')->nullable();

            // Raw unmodified payload from provider
            $table->json('raw_payload')->nullable();

            $table->timestamps();

            // One row per (title, source, provider_item_id)
            $table->unique(
                ['video_game_title_id', 'video_game_source_id', 'provider_item_id'],
                'vg_title_sources_title_source_item_unique'
            );

            // Often you will query by title to rebuild canonical
            $table->index('video_game_title_id');
        });
    }

    public function down(): void
    {
        Schema::dropIfExists('video_game_title_sources');
    }
};
