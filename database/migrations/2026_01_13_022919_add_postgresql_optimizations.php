<?php

declare(strict_types=1);

use Illuminate\Database\Migrations\Migration;
use Illuminate\Support\Facades\DB;

return new class extends Migration
{
    /**
     * Run the migrations.
     *
     * Adds PostgreSQL-specific optimizations:
     * - Bloom filters for space-efficient set membership queries
     * - pg_trgm (trigram) indexes for fast fuzzy text search
     * - Composite indexes for common query patterns
     */
    public function up(): void
    {
        // Only apply these optimizations for PostgreSQL
        if (DB::connection()->getDriverName() !== 'pgsql') {
            return;
        }

        // Enable required extensions
        DB::statement('CREATE EXTENSION IF NOT EXISTS bloom');
        DB::statement('CREATE EXTENSION IF NOT EXISTS pg_trgm');

        // ==========================================
        // BLOOM INDEXES (space-efficient membership testing)
        // ==========================================

        // video_games: Bloom index for multi-column equality filters
        // Useful for queries filtering by developer and publisher combinations
        // Note: BLOOM only supports types with default operator classes (text, not numeric)
        DB::statement('
            CREATE INDEX IF NOT EXISTS idx_video_games_bloom_metadata
            ON video_games USING bloom (developer, publisher)
            WITH (length=80, col1=2, col2=2)
        ');

        // images: Regular B-tree index for thumbnail filtering
        // Note: BLOOM doesn't support boolean or numeric types
        // Use B-tree for efficient boolean equality checks
        DB::statement('
            CREATE INDEX IF NOT EXISTS idx_images_is_thumbnail
            ON images (is_thumbnail)
        ');

        // ==========================================
        // PG_TRGM INDEXES (fuzzy text search)
        // ==========================================

        // video_games.name: Fast ILIKE/LIKE queries and fuzzy matching
        DB::statement('
            CREATE INDEX IF NOT EXISTS idx_video_games_name_trgm
            ON video_games USING gin (name gin_trgm_ops)
        ');

        // video_games.description: Full-text fuzzy search
        DB::statement('
            CREATE INDEX IF NOT EXISTS idx_video_games_description_trgm
            ON video_games USING gin (description gin_trgm_ops)
        ');

        // video_games.developer: Fast developer search
        DB::statement('
            CREATE INDEX IF NOT EXISTS idx_video_games_developer_trgm
            ON video_games USING gin (developer gin_trgm_ops)
        ');

        // video_games.publisher: Fast publisher search
        DB::statement('
            CREATE INDEX IF NOT EXISTS idx_video_games_publisher_trgm
            ON video_games USING gin (publisher gin_trgm_ops)
        ');

        // products.name: Product name search
        DB::statement('
            CREATE INDEX IF NOT EXISTS idx_products_name_trgm
            ON products USING gin (name gin_trgm_ops)
        ');

        // ==========================================
        // COMPOSITE INDEXES (common query patterns)
        // ==========================================

        // video_games: Common filtering by rating + release date
        DB::statement('
            CREATE INDEX IF NOT EXISTS idx_video_games_rating_release
            ON video_games (rating DESC, release_date DESC)
            WHERE rating IS NOT NULL AND release_date IS NOT NULL
        ');

        // video_games: Search + sort by name
        DB::statement('
            CREATE INDEX IF NOT EXISTS idx_video_games_name_created
            ON video_games (name, created_at DESC)
        ');

        // video_game_titles: Product lookup with normalized title for grouping
        // Note: product_id already has a single-column index from table creation
        DB::statement('
            CREATE INDEX IF NOT EXISTS idx_video_game_titles_normalized
            ON video_game_titles (normalized_title)
            WHERE normalized_title IS NOT NULL
        ');

        // images: Fast lookup by game + thumbnail status
        DB::statement('
            CREATE INDEX IF NOT EXISTS idx_images_game_thumbnail
            ON images (video_game_id, is_thumbnail)
        ');

        // images: Dimension-based filtering
        DB::statement('
            CREATE INDEX IF NOT EXISTS idx_images_dimensions
            ON images (width, height)
            WHERE width IS NOT NULL AND height IS NOT NULL
        ');

        // videos: Game + provider lookup
        DB::statement('
            CREATE INDEX IF NOT EXISTS idx_videos_game_provider
            ON videos (video_game_id, provider)
        ');

        // ==========================================
        // JSON INDEXES (for JSON column queries)
        // ==========================================

        // video_games.platform: Fast platform array lookups
        // Cast json to jsonb for GIN indexing
        DB::statement('
            CREATE INDEX IF NOT EXISTS idx_video_games_platform_gin
            ON video_games USING gin ((platform::jsonb))
        ');

        // video_games.genre: Fast genre array lookups
        // Cast json to jsonb for GIN indexing
        DB::statement('
            CREATE INDEX IF NOT EXISTS idx_video_games_genre_gin
            ON video_games USING gin ((genre::jsonb))
        ');

        // images.urls: Fast URL array searches
        // Cast json to jsonb for GIN indexing
        DB::statement('
            CREATE INDEX IF NOT EXISTS idx_images_urls_gin
            ON images USING gin ((urls::jsonb))
        ');

        // images.metadata: Fast metadata JSONB queries
        // Cast json to jsonb for GIN indexing
        DB::statement('
            CREATE INDEX IF NOT EXISTS idx_images_metadata_gin
            ON images USING gin ((metadata::jsonb))
        ');
    }

    /**
     * Reverse the migrations.
     */
    public function down(): void
    {
        if (DB::connection()->getDriverName() !== 'pgsql') {
            return;
        }

        // Drop indexes (extensions remain for other uses)
        $indexes = [
            'idx_video_games_bloom_metadata',
            'idx_images_is_thumbnail',
            'idx_video_games_name_trgm',
            'idx_video_games_description_trgm',
            'idx_video_games_developer_trgm',
            'idx_video_games_publisher_trgm',
            'idx_products_name_trgm',
            'idx_video_games_rating_release',
            'idx_video_games_name_created',
            'idx_video_game_titles_normalized',
            'idx_images_game_thumbnail',
            'idx_images_dimensions',
            'idx_videos_game_provider',
            'idx_video_games_platform_gin',
            'idx_video_games_genre_gin',
            'idx_images_urls_gin',
            'idx_images_metadata_gin',
        ];

        foreach ($indexes as $index) {
            DB::statement("DROP INDEX IF EXISTS {$index}");
        }
    }
};
