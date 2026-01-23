<?php

declare(strict_types=1);

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    public function up(): void
    {
        // Some environments may leave views/materialized views behind that collide with these names.
        if (Schema::getConnection()->getDriverName() === 'pgsql') {
            DB::statement('DROP MATERIALIZED VIEW IF EXISTS video_game_sources CASCADE');
            DB::statement('DROP VIEW IF EXISTS video_game_sources CASCADE');
            DB::statement('DROP TABLE IF EXISTS video_game_sources CASCADE');
        }

        Schema::create('video_game_sources', function (Blueprint $table) {
            $table->id();

            /**
             * Provider identifier, e.g. "giantbomb", "igdb", "rawg".
             *
             * CRITICAL DOMAIN SEMANTICS:
             * `video_game_sources` is provider-level aggregation ONLY.
             * There MUST be only ONE row per provider.
             *
             * Per-provider-per-item IDs/payloads (e.g. IGDB game id) live in
             * `video_game_title_sources`, which maps providers to `video_game_titles`.
             */
            $table->string('provider')->unique();
            $table->string('provider_key')->nullable()->index(); // Added for compatibility with Providers
            $table->string('display_name')->nullable();
            $table->string('category')->nullable();
            $table->string('slug')->nullable()->index();
            $table->text('base_url')->nullable();
            $table->json('metadata')->nullable();

            /**
             * Denormalized count of provider items observed/ingested.
             * This is maintained by ingestion jobs for fast admin/telemetry.
             */
            $table->unsignedBigInteger('items_count')->default(0);

            $table->timestamps();
        });
    }

    public function down(): void
    {
        Schema::dropIfExists('video_game_sources');
    }
};
