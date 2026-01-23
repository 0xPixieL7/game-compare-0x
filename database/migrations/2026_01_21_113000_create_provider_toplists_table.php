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
        Schema::create('provider_toplists', function (Blueprint $table) {
            $table->id();
            $table->string('provider_key')->index(); // e.g., 'rawg', 'igdb'
            $table->string('list_key')->index();     // e.g., 'trending', 'action', 'upcoming'
            $table->string('list_type')->default('top_list'); // e.g., 'genre', 'monthly', 'trending'
            $table->string('name');                  // Human readable name: e.g. "RAWG Trending"
            $table->timestamp('snapshot_at')->useCurrent();
            $table->date('period_start')->nullable();
            $table->date('period_end')->nullable();
            $table->timestamps();

            $table->unique(['provider_key', 'list_key', 'snapshot_at']);
        });

        Schema::create('provider_toplist_items', function (Blueprint $table) {
            $table->id();
            $table->foreignId('provider_toplist_id')->constrained()->cascadeOnDelete();
            $table->unsignedBigInteger('video_game_id')->nullable()->index();
            $table->unsignedBigInteger('product_id')->nullable()->index();
            $table->unsignedBigInteger('external_id')->index(); // The ID in the provider (e.g. RAWG ID)
            $table->integer('rank');
            $table->jsonb('metadata')->nullable();
            $table->timestamps();
        });
    }

    /**
     * Reverse the migrations.
     */
    public function down(): void
    {
        Schema::dropIfExists('provider_toplist_items');
        Schema::dropIfExists('provider_toplists');
    }
};
