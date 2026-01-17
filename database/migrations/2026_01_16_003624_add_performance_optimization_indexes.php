<?php

declare(strict_types=1);

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

/**
 * Performance optimization indexes for frequently queried columns.
 *
 * This migration adds indexes on:
 * - Timestamp columns (created_at, updated_at) for time-based queries
 * - Soft delete columns (deleted_at) for scope filtering
 * - Status/boolean columns (is_active) for filtering
 *
 * Indexes improve query performance for ORDER BY, WHERE, and JOIN operations.
 */
return new class extends Migration
{
    public function up(): void
    {

        // retailers table - is_active filtering
        Schema::table('retailers', function (Blueprint $table) {
            $table->index('is_active');
        });

        // video_game_prices table - is_active filtering for active price queries
        Schema::table('video_game_prices', function (Blueprint $table) {
            $table->index('is_active');
        });

        // video_game_titles table - timestamp indexes for recent queries
        Schema::table('video_game_titles', function (Blueprint $table) {
            $table->index('created_at');
            $table->index('updated_at');
        });

        // video_game_title_sources table - timestamp indexes
        Schema::table('video_game_title_sources', function (Blueprint $table) {
            $table->index('created_at');
            $table->index('updated_at');
        });

        // images table - timestamp indexes for recent media queries
        Schema::table('images', function (Blueprint $table) {
            $table->index('created_at');
            $table->index('updated_at');
        });

        // videos table - timestamp indexes
        Schema::table('videos', function (Blueprint $table) {
            $table->index('created_at');
            $table->index('updated_at');
        });

        // Composite indexes for common query patterns
        // video_game_prices: active prices ordered by recorded date
        Schema::table('video_game_prices', function (Blueprint $table) {
            $table->index(['is_active', 'recorded_at'], 'vgp_active_recorded_idx');
        });

        // retailers: active retailers lookup
        Schema::table('retailers', function (Blueprint $table) {
            $table->index(['is_active', 'slug'], 'retailers_active_slug_idx');
        });
    }

    public function down(): void
    {
        Schema::table('users', function (Blueprint $table) {
            $table->dropIndex(['deleted_at']);
        });

        Schema::table('retailers', function (Blueprint $table) {
            $table->dropIndex(['is_active']);
            $table->dropIndex('retailers_active_slug_idx');
        });

        Schema::table('video_game_prices', function (Blueprint $table) {
            $table->dropIndex(['is_active']);
            $table->dropIndex('vgp_active_recorded_idx');
        });

        Schema::table('video_game_titles', function (Blueprint $table) {
            $table->dropIndex(['created_at']);
            $table->dropIndex(['updated_at']);
        });

        Schema::table('video_game_title_sources', function (Blueprint $table) {
            $table->dropIndex(['created_at']);
            $table->dropIndex(['updated_at']);
        });

        Schema::table('images', function (Blueprint $table) {
            $table->dropIndex(['created_at']);
            $table->dropIndex(['updated_at']);
        });

        Schema::table('videos', function (Blueprint $table) {
            $table->dropIndex(['created_at']);
            $table->dropIndex(['updated_at']);
        });
    }
};
