<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    /**
     * Run the migrations.
     */
    public function up(): void
    {
        // Fix 1: Video Games missing columns
        Schema::table('video_games', function (Blueprint $table) {
            if (! Schema::hasColumn('video_games', 'slug')) {
                $table->string('slug')->nullable()->unique(); // nullable first to avoid error on existing rows? No, table is empty.
            }
            if (! Schema::hasColumn('video_games', 'description')) {
                $table->text('description')->nullable();
            }
            if (! Schema::hasColumn('video_games', 'summary')) {
                $table->text('summary')->nullable();
            }
            if (! Schema::hasColumn('video_games', 'storyline')) {
                $table->text('storyline')->nullable();
            }
            if (! Schema::hasColumn('video_games', 'url')) {
                $table->string('url')->nullable();
            }
            if (! Schema::hasColumn('video_games', 'platform')) {
                $table->json('platform')->nullable();
            }
            if (! Schema::hasColumn('video_games', 'rating_count')) {
                $table->unsignedInteger('rating_count')->nullable();
            }
            if (! Schema::hasColumn('video_games', 'developer')) {
                $table->string('developer')->nullable();
            }
            if (! Schema::hasColumn('video_games', 'publisher')) {
                $table->string('publisher')->nullable();
            }
            if (! Schema::hasColumn('video_games', 'genre')) {
                $table->json('genre')->nullable();
            }
            if (! Schema::hasColumn('video_games', 'media')) {
                $table->json('media')->nullable();
            }
            if (! Schema::hasColumn('video_games', 'source_payload')) {
                $table->json('source_payload')->nullable();
            }
        });

        // Fix 2: Products unique name constraint
        // The constraint name is often 'products_name_unique' but could verify using SQL if needed.
        // We caught 'products_name_unique' in the log.
        try {
            Schema::table('products', function (Blueprint $table) {
                // Check if index exists is hard in generic way, but we can try dropping it.
                // Or use raw SQL to drop constraint if exists
                $table->dropUnique('products_name_unique');
            });
        } catch (\Exception $e) {
            // Index might not exist or name is different. Try raw SQL for Postgres just in case.
            try {
                DB::statement('ALTER TABLE products DROP CONSTRAINT IF EXISTS products_name_unique');
            } catch (\Exception $ex) {
                // Ignore if it fails, maybe it doesn't exist.
            }
        }
    }

    /**
     * Reverse the migrations.
     */
    public function down(): void
    {
        // No reverse for fixes usually, or implementation would be dropping columns.
    }
};
