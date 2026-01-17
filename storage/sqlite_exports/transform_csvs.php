<?php

/**
 * CSV Schema Transformation Script
 *
 * Transforms old schema CSVs to match current database schema.
 * Run with: php transform_csvs.php
 */

declare(strict_types=1);

$baseDir = __DIR__;

// Helper function to read CSV
function readCsv(string $file): array
{
    $rows = [];
    if (!file_exists($file)) {
        echo "❌ File not found: $file\n";
        return [];
    }

    $handle = fopen($file, 'r');
    $headers = fgetcsv($handle, 0, ',', '"', '\\');

    while (($data = fgetcsv($handle, 0, ',', '"', '\\')) !== false) {
        // Handle rows with mismatched column counts
        if (count($data) !== count($headers)) {
            // Pad with empty strings or truncate to match headers
            $data = array_pad(array_slice($data, 0, count($headers)), count($headers), '');
        }
        $rows[] = array_combine($headers, $data);
    }
    fclose($handle);

    echo "✅ Read " . count($rows) . " rows from " . basename($file) . "\n";
    return $rows;
}

// Helper function to write CSV
function writeCsv(string $file, array $rows, array $headers): void
{
    $handle = fopen($file, 'w');
    fputcsv($handle, $headers, ',', '"', '\\');

    foreach ($rows as $row) {
        $csvRow = [];
        foreach ($headers as $header) {
            $value = $row[$header] ?? '';
            // Clean up any literal \n in strings (convert to actual newlines or remove)
            if (is_string($value)) {
                // Remove literal \n sequences but preserve actual newlines for CSV
                $value = str_replace('\\n', ' ', $value);
                // Also clean up multiple spaces
                $value = preg_replace('/\s+/', ' ', $value);
                $value = trim($value);
            }
            $csvRow[] = $value;
        }
        fputcsv($handle, $csvRow, ',', '"', '\\');
    }
    fclose($handle);

    echo "✅ Wrote " . count($rows) . " rows to " . basename($file) . "\n";
}

// Helper to normalize title
function normalizeTitle(string $title): string
{
    return strtolower(preg_replace('/[^a-z0-9]+/i', '-', $title));
}

// Helper to generate slug
function generateSlug(string $text): string
{
    $slug = strtolower(trim(preg_replace('/[^A-Za-z0-9-]+/', '-', $text)));
    return trim($slug, '-');
}

echo "\n";
echo "=".str_repeat("=", 70)."=\n";
echo "  CSV Schema Transformation\n";
echo "=".str_repeat("=", 70)."=\n\n";

// =============================================================================
// 1. TRANSFORM products.csv
// =============================================================================
echo "📦 Phase 1: Transforming products.csv...\n";

$oldProducts = readCsv("$baseDir/products.csv");
$newProducts = [];
$videoGameTitles = [];
$videoGames = [];
$videoGameTitleSources = [];

$sourceId = 1; // For video_game_sources - we'll assume IGDB source exists with id=1

foreach ($oldProducts as $product) {
    $productId = (int)$product['id'];
    $metadata = json_decode($product['metadata'], true);
    $igdbData = $metadata['sources']['igdb'] ?? null;
    $externalIds = json_decode($product['external_ids'], true);
    $igdbId = $externalIds['igdb'] ?? null;

    // Get synopsis from IGDB data if available, otherwise use product synopsis
    $synopsis = '';
    if ($igdbData && !empty($igdbData['summary'])) {
        $synopsis = $igdbData['summary'];
    } elseif (!empty($product['synopsis'])) {
        $synopsis = $product['synopsis'];
    }

    // Transform product
    $newProducts[] = [
        'id' => $productId,
        'type' => 'video_game',
        'name' => $product['name'],
        'slug' => $product['slug'],
        'title' => $product['name'], // Use name as title
        'normalized_title' => normalizeTitle($product['name']),
        'synopsis' => $synopsis,
        'created_at' => $product['created_at'],
        'updated_at' => $product['updated_at'],
    ];

    // Create video_game_title (1:1 with product for now)
    $videoGameTitles[] = [
        'id' => $productId,
        'product_id' => $productId,
        'name' => $product['name'],
        'normalized_title' => normalizeTitle($product['name']),
        'slug' => $product['slug'],
        'providers' => json_encode(['igdb']), // JSON array of provider names
        'created_at' => $product['created_at'],
        'updated_at' => $product['updated_at'],
    ];

    // Create video_game (canonical amalgamated record)
    if ($igdbData) {
        $platforms = [];
        if (!empty($igdbData['platform_names'])) {
            foreach ($igdbData['platform_names'] as $platformName) {
                $platforms[] = $platformName;
            }
        }

        $genres = [];
        if (!empty($igdbData['genres'])) {
            foreach ($igdbData['genres'] as $genre) {
                $genres[] = $genre['name'] ?? $genre;
            }
        }

        // Build media JSON
        $media = [
            'videos' => $igdbData['videos'] ?? [],
            'screenshots' => $igdbData['screenshots'] ?? [],
            'cover' => null,
        ];

        // Add cover if available
        if (!empty($igdbData['cover'])) {
            $media['cover'] = $igdbData['cover'];
        }

        $videoGames[] = [
            'id' => $productId,
            'video_game_title_id' => $productId,
            'slug' => $product['slug'],
            'provider' => 'igdb',
            'external_id' => $igdbId,
            'name' => $igdbData['name'] ?? $product['name'],
            'description' => '',
            'summary' => $igdbData['summary'] ?? $product['synopsis'],
            'storyline' => $igdbData['storyline'] ?? '',
            'url' => '',
            'release_date' => isset($igdbData['first_release_date'])
                ? date('Y-m-d', strtotime($igdbData['first_release_date']))
                : '',
            'platform' => json_encode($platforms),
            'rating' => null,
            'rating_count' => null,
            'developer' => '',
            'publisher' => '',
            'genre' => json_encode($genres),
            'media' => json_encode($media),
            'source_payload' => json_encode($igdbData),
            'created_at' => $product['created_at'],
            'updated_at' => $product['updated_at'],
        ];

        // Create video_game_title_source
        $videoGameTitleSources[] = [
            'id' => $productId,
            'video_game_title_id' => $productId,
            'video_game_source_id' => $sourceId, // Assume IGDB source id = 1
            'provider' => 'igdb',
            'external_id' => $igdbId,
            'slug' => $igdbData['slug'] ?? $product['slug'],
            'name' => $igdbData['name'] ?? $product['name'],
            'description' => $igdbData['summary'] ?? '',
            'release_date' => isset($igdbData['first_release_date'])
                ? date('Y-m-d', strtotime($igdbData['first_release_date']))
                : '',
            'provider_item_id' => $igdbId,
            'platform' => json_encode($platforms),
            'rating' => null,
            'rating_count' => null,
            'developer' => '',
            'publisher' => '',
            'genre' => json_encode($genres),
            'raw_payload' => json_encode($igdbData),
            'created_at' => $product['created_at'],
            'updated_at' => $product['updated_at'],
        ];
    }
}

// Write transformed products
$productsHeaders = ['id', 'type', 'name', 'slug', 'title', 'normalized_title', 'synopsis', 'created_at', 'updated_at'];
writeCsv("$baseDir/products_TRANSFORMED.csv", $newProducts, $productsHeaders);

$titlesHeaders = ['id', 'product_id', 'name', 'normalized_title', 'slug', 'providers', 'created_at', 'updated_at'];
writeCsv("$baseDir/video_game_titles_TRANSFORMED.csv", $videoGameTitles, $titlesHeaders);

$videoGamesHeaders = ['id', 'video_game_title_id', 'slug', 'provider', 'external_id', 'name', 'description', 'summary',
    'storyline', 'url', 'release_date', 'platform', 'rating', 'rating_count', 'developer', 'publisher', 'genre', 'media',
    'source_payload', 'created_at', 'updated_at'];
writeCsv("$baseDir/video_games_TRANSFORMED.csv", $videoGames, $videoGamesHeaders);

$sourcesHeaders = ['id', 'video_game_title_id', 'video_game_source_id', 'provider', 'external_id', 'slug', 'name',
    'description', 'release_date', 'provider_item_id', 'platform', 'rating', 'rating_count', 'developer', 'publisher',
    'genre', 'raw_payload', 'created_at', 'updated_at'];
writeCsv("$baseDir/video_game_title_sources_TRANSFORMED.csv", $videoGameTitleSources, $sourcesHeaders);

echo "\n";

// =============================================================================
// 2. TRANSFORM game_videos.csv → videos.csv
// =============================================================================
echo "🎬 Phase 2: Transforming game_videos.csv...\n";

$oldVideos = readCsv("$baseDir/game_videos.csv");
$newVideos = [];

foreach ($oldVideos as $video) {
    $metadata = json_decode($video['metadata'], true);
    $thumbnails = json_decode($video['thumbnails'], true);
    $providerPayload = json_decode($video['provider_payload'], true);

    $provider = $metadata['provider'] ?? 'youtube';
    $thumbnailUrl = $thumbnails['hq'] ?? $thumbnails['default'] ?? '';

    // Build urls JSON
    $urls = [
        'embed' => $video['embed_url'],
        'watch' => $video['site_detail_url'],
    ];
    if ($video['stream_url']) {
        $urls['stream'] = $video['stream_url'];
    }

    // Build metadata JSON
    $metadataJson = [
        'provider' => $provider,
        'thumbnails' => $thumbnails,
        'igdb_video_id' => $metadata['igdb_video_id'] ?? null,
        'checksum' => $metadata['checksum'] ?? null,
    ];

    $newVideos[] = [
        'id' => $video['id'],
        'videoable_type' => 'App\\Models\\VideoGame',
        'videoable_id' => $video['game_provider_id'], // Maps to old product id = new video_game id
        'video_game_id' => $video['game_provider_id'],
        'media_id' => $video['media_id'] ?: '',
        'url' => $video['embed_url'],
        'source_url' => $video['site_detail_url'],
        'video_id' => $video['video_key'],
        'urls' => json_encode($urls),
        'provider' => $provider,
        'duration' => $video['duration_seconds'] ?: '',
        'width' => '',
        'height' => '',
        'thumbnail_url' => $thumbnailUrl,
        'title' => $video['name'],
        'description' => $video['description'],
        'metadata' => json_encode($metadataJson),
        'created_at' => $video['created_at'],
        'updated_at' => $video['updated_at'],
    ];
}

$videosHeaders = ['id', 'videoable_type', 'videoable_id', 'video_game_id', 'media_id', 'url', 'source_url', 'video_id',
    'urls', 'provider', 'duration', 'width', 'height', 'thumbnail_url', 'title', 'description', 'metadata',
    'created_at', 'updated_at'];
writeCsv("$baseDir/videos_TRANSFORMED.csv", $newVideos, $videosHeaders);

echo "\n";

// =============================================================================
// 3. TRANSFORM game_images.csv → images.csv
// =============================================================================
echo "🖼️  Phase 3: Transforming game_images.csv...\n";

$oldImages = readCsv("$baseDir/game_images.csv");
$newImages = [];

foreach ($oldImages as $image) {
    $variants = json_decode($image['variants'], true);
    $metadata = json_decode($image['metadata'], true);

    // Determine if thumbnail (rank 0 or cover image)
    $isThumbnail = ($image['rank'] == 0) || str_starts_with($image['image_key'], 'cover:');

    // Build urls JSON from variants
    $urls = $variants ?? [];

    // Build metadata JSON
    $metadataJson = [
        'kind' => $metadata['kind'] ?? 'screenshot',
        'image_id' => $metadata['image_id'] ?? $image['image_key'],
        'checksum' => $metadata['checksum'] ?? null,
        'alpha_channel' => $metadata['alpha_channel'] ?? false,
        'animated' => $metadata['animated'] ?? false,
        'source' => $metadata['source'] ?? 'igdb',
        'ordinal' => $metadata['ordinal'] ?? $image['rank'],
        'variants' => $variants,
    ];

    $newImages[] = [
        'id' => $image['id'],
        'imageable_type' => 'App\\Models\\VideoGame',
        'imageable_id' => $image['game_provider_id'], // Maps to old product id = new video_game id
        'video_game_id' => $image['game_provider_id'],
        'media_id' => $image['media_id'] ?: '',
        'url' => $image['url'],
        'source_url' => $image['url'], // Same as url for IGDB images
        'width' => $image['width'],
        'height' => $image['height'],
        'alt_text' => $image['caption'],
        'caption' => $image['caption'],
        'is_thumbnail' => $isThumbnail ? '1' : '0',
        'urls' => json_encode($urls),
        'metadata' => json_encode($metadataJson),
        'created_at' => $image['created_at'],
        'updated_at' => $image['updated_at'],
    ];
}

$imagesHeaders = ['id', 'imageable_type', 'imageable_id', 'video_game_id', 'media_id', 'url', 'source_url', 'width',
    'height', 'alt_text', 'caption', 'is_thumbnail', 'urls', 'metadata', 'created_at', 'updated_at'];
writeCsv("$baseDir/images_TRANSFORMED.csv", $newImages, $imagesHeaders);

echo "\n";
echo "=".str_repeat("=", 70)."=\n";
echo "  ✅ Transformation Complete!\n";
echo "=".str_repeat("=", 70)."=\n\n";

echo "Transformed files created:\n";
echo "  • products_TRANSFORMED.csv\n";
echo "  • video_game_titles_TRANSFORMED.csv\n";
echo "  • video_games_TRANSFORMED.csv\n";
echo "  • video_game_title_sources_TRANSFORMED.csv\n";
echo "  • videos_TRANSFORMED.csv\n";
echo "  • images_TRANSFORMED.csv\n\n";

echo "Note: game_media.csv was NOT transformed (deprecated - data should be aggregated from images + videos)\n\n";
