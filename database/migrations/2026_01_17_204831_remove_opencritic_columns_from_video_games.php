<?php

declare(strict_types=1);

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    /**
     * Run the migrations.
     *
     * Removes OpenCritic columns from video_games table.
     * OpenCritic integration removed in favor of IGDB as single source of truth.
     */
    public function up(): void
    {
        // Only proceed if OpenCritic columns exist
        if (Schema::hasColumn('video_games', 'opencritic_id')) {
            Schema::table('video_games', function (Blueprint $table) {
                // Drop all indexes referencing OpenCritic columns first
                // This is critical for SQLite compatibility
                try {
                    $table->dropUnique(['opencritic_id']); // unique index
                } catch (\Exception $e) {
                    // Index might not exist - safe to ignore
                }
                
                try {
                    $table->dropIndex(['opencritic_score', 'opencritic_review_count']); // composite index
                } catch (\Exception $e) {
                    // Index might not exist or have different name - safe to ignore
                }

                // Now safe to drop OpenCritic columns
                $table->dropColumn([
                    'opencritic_score',
                    'opencritic_review_count',
                    'opencritic_tier',
                    'opencritic_user_score',
                    'opencritic_user_count',
                    'opencritic_percent_recommended',
                    'opencritic_id',
                    'opencritic_updated_at',
                ]);
            });
        }
    }

    /**
     * Reverse the migrations.
     *
     * Recreates OpenCritic columns with original types and indexes.
     */
    public function down(): void
    {
        Schema::table('video_games', function (Blueprint $table) {
            // Recreate OpenCritic columns with original types
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

            // Recreate composite index
            $table->index(['opencritic_score', 'opencritic_review_count']);
        });
    }
};
