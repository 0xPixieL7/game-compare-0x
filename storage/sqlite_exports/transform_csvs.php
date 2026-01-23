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
    // RFC4180: quotes escaped by doubling; no backslash-escaping
    $headers = fgetcsv($handle, 0, ',', '"', '');

    while (($data = fgetcsv($handle, 0, ',', '"', '')) !== false) {
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
    // RFC4180: quotes escaped by doubling; no backslash-escaping
    fputcsv($handle, $headers, ',', '"', '');

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
        fputcsv($handle, $csvRow, ',', '"', '');
    }
    fclose($handle);

    echo "✅ Wrote " . count($rows) . " rows to " . basename($file) . "\n";
}

// Helper to normalize title
function normalizeTitle(string $title): string
{
    return strtolower(preg_replace('/[^a-z0-9]+/i', '-', $title));
}

function normalizeProviderKey(string $provider): string
{
    $provider = strtolower(trim($provider));

    return match ($provider) {
        'thegamesdb' => 'tgdb',
        default => $provider,
    };
}

/**
 * @param array<string, mixed> $map
 * @return array<string, mixed>
 */
function normalizeProviderMap(array $map): array
{
    $normalized = [];

    foreach ($map as $key => $value) {
        if (! is_string($key)) {
            continue;
        }

        $normalized[normalizeProviderKey($key)] = $value;
    }

    return $normalized;
}

function isIntLike(mixed $value): bool
{
    if (is_int($value)) {
        return true;
    }

    if (! is_string($value)) {
        return false;
    }

    $value = trim($value);

    return $value !== '' && ctype_digit($value);
}

/**
 * @param array<string, mixed> $externalIds
 * @return array{provider: string, external_id: int}
 */
function pickCanonicalProvider(array $externalIds, int $fallbackId): array
{
    foreach (['igdb', 'tgdb', 'steam', 'steam_store', 'playstation_store', 'microsoft_store', 'xbox'] as $preferred) {
        $value = $externalIds[$preferred] ?? null;
        if (isIntLike($value)) {
            return ['provider' => $preferred, 'external_id' => (int) $value];
        }
    }

    foreach ($externalIds as $provider => $value) {
        if (isIntLike($value)) {
            return ['provider' => (string) $provider, 'external_id' => (int) $value];
        }
    }

    return ['provider' => 'legacy', 'external_id' => $fallbackId];
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

$sourceRows = readCsv("$baseDir/video_game_sources.csv");
$sourceIdByProvider = [];
foreach ($sourceRows as $row) {
    $providerKey = $row['provider_key'] ?? '';
    $providerKey = $providerKey !== '' ? $providerKey : ($row['provider'] ?? '');

    $providerKey = normalizeProviderKey((string) $providerKey);

    if ($providerKey === '') {
        continue;
    }

    $sourceIdByProvider[$providerKey] = (int) ($row['id'] ?? 0);
}

$igdbSourceId = $sourceIdByProvider['igdb'] ?? 1;
$tgdbSourceId = $sourceIdByProvider['tgdb'] ?? null;

foreach ($oldProducts as $product) {
    $productId = (int)$product['id'];
    $metadata = json_decode($product['metadata'] ?? '[]', true);
    $metadata = is_array($metadata) ? $metadata : [];

    $externalIds = json_decode($product['external_ids'] ?? '[]', true);
    $externalIds = is_array($externalIds) ? $externalIds : [];

    // Provider normalization (legacy -> canonical)
    $metadata['sources'] = normalizeProviderMap($metadata['sources'] ?? []);
    $externalIds = normalizeProviderMap($externalIds);

    // Preserve legacy-only columns under metadata.legacy
    $metadata['legacy'] = array_filter([
        'uid' => $product['uid'] ?? null,
        'primary_platform_family' => $product['primary_platform_family'] ?? null,
        'freshness_score' => $product['freshness_score'] ?? null,
    ], fn ($v) => $v !== null && $v !== '');

    $igdbData = $metadata['sources']['igdb'] ?? null;
    $igdbId = $externalIds['igdb'] ?? (is_array($igdbData) ? ($igdbData['id'] ?? null) : null);

    $providers = array_merge(
        array_keys($metadata['sources'] ?? []),
        array_keys($externalIds)
    );
    $providers = array_values(array_unique(array_filter(array_map('strval', $providers))));
    sort($providers);

    $canonical = pickCanonicalProvider($externalIds, $productId);
    $canonicalProvider = $canonical['provider'];
    $canonicalExternalId = $canonical['external_id'];
    $canonicalPayload = $metadata['sources'][$canonicalProvider] ?? null;
    $canonicalPayload = is_array($canonicalPayload) ? $canonicalPayload : [];

    // Get synopsis from best available provider data if available, otherwise use product synopsis
    $synopsis = '';
    if (! empty($canonicalPayload['summary'])) {
        $synopsis = (string) $canonicalPayload['summary'];
    } elseif (! empty($canonicalPayload['overview'])) {
        $synopsis = (string) $canonicalPayload['overview'];
    } elseif (!empty($product['synopsis'])) {
        $synopsis = $product['synopsis'];
    }

    $type = ($product['category'] ?? null) === 'Hardware' ? 'console' : 'video_game';

    $releaseDate = null;
    if (! empty($canonicalPayload['first_release_date'])) {
        $releaseDate = date('Y-m-d', strtotime((string) $canonicalPayload['first_release_date']));
    } elseif (! empty($product['release_date'])) {
        $releaseDate = date('Y-m-d', strtotime($product['release_date']));
    }

    // Transform product
    $newProducts[] = [
        'id' => $productId,
        'type' => $type,
        'name' => $product['name'],
        'slug' => $product['slug'],
        'title' => $product['name'], // Use name as title
        'normalized_title' => normalizeTitle($product['name']),
        'synopsis' => $synopsis,
        'platform' => $product['platform'] ?? null,
        'category' => $product['category'] ?? null,
        'release_date' => $releaseDate,
        'popularity_score' => $product['popularity_score'] ?? null,
        'rating' => $product['rating'] ?? null,
        'external_ids' => json_encode($externalIds, JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE),
        'metadata' => json_encode($metadata, JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE),
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
        'providers' => json_encode($providers, JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE), // JSON array of provider keys
        'created_at' => $product['created_at'],
        'updated_at' => $product['updated_at'],
    ];

    // Create video_game (canonical amalgamated record)
    $platforms = [];
    if ($canonicalProvider === 'igdb' && ! empty($canonicalPayload['platform_names']) && is_array($canonicalPayload['platform_names'])) {
        $platforms = $canonicalPayload['platform_names'];
    } elseif (! empty($product['platform'])) {
        $platforms = [$product['platform']];
    }

    $genres = [];
    if ($canonicalProvider === 'igdb' && ! empty($canonicalPayload['genres']) && is_array($canonicalPayload['genres'])) {
        foreach ($canonicalPayload['genres'] as $genre) {
            $genres[] = is_array($genre) ? ($genre['name'] ?? '') : (string) $genre;
        }
        $genres = array_values(array_filter($genres, fn ($v) => $v !== ''));
    }

    $media = [
        'videos' => $canonicalProvider === 'igdb' ? ($canonicalPayload['videos'] ?? []) : [],
        'screenshots' => $canonicalProvider === 'igdb' ? ($canonicalPayload['screenshots'] ?? []) : [],
        'cover' => $canonicalProvider === 'igdb' ? ($canonicalPayload['cover'] ?? null) : null,
    ];

    $videoGames[] = [
        'id' => $productId,
        'video_game_title_id' => $productId,
        'slug' => $product['slug'],
        'provider' => $canonicalProvider,
        'external_id' => $canonicalExternalId,
        'name' => $canonicalPayload['name'] ?? $product['name'],
        'description' => null,
        'summary' => $canonicalPayload['summary'] ?? $canonicalPayload['overview'] ?? $synopsis,
        'storyline' => $canonicalPayload['storyline'] ?? null,
        'url' => null,
        'release_date' => $releaseDate,
        'platform' => ! empty($platforms) ? json_encode($platforms, JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE) : null,
        'rating' => $product['rating'] !== '' ? $product['rating'] : null,
        'rating_count' => null,
        'developer' => null,
        'publisher' => null,
        'genre' => ! empty($genres) ? json_encode($genres, JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE) : null,
        'media' => json_encode($media, JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE),
        'source_payload' => ! empty($canonicalPayload) ? json_encode($canonicalPayload, JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE) : null,
        'created_at' => $product['created_at'],
        'updated_at' => $product['updated_at'],
    ];

    // Create per-provider title sources (igdb + tgdb when possible)
    $candidateProviders = [
        'igdb' => $igdbSourceId,
        'tgdb' => $tgdbSourceId,
    ];

    foreach ($candidateProviders as $providerKey => $sourceId) {
        if (! $sourceId) {
            continue;
        }

        $providerPayload = $metadata['sources'][$providerKey] ?? null;
        $providerPayload = is_array($providerPayload) ? $providerPayload : [];

        $providerItemId = $externalIds[$providerKey] ?? ($providerPayload['id'] ?? null);
        if (! isIntLike($providerItemId)) {
            continue;
        }

        $description = null;
        if ($providerKey === 'igdb') {
            $description = $providerPayload['summary'] ?? null;
        } elseif ($providerKey === 'tgdb') {
            $description = $providerPayload['overview'] ?? ($providerPayload['summary'] ?? null);
        }

        $videoGameTitleSources[] = [
            'video_game_title_id' => $productId,
            'video_game_source_id' => $sourceId,
            'provider' => $providerKey,
            'external_id' => (int) $providerItemId,
            'slug' => $providerPayload['slug'] ?? $product['slug'],
            'name' => $providerPayload['name'] ?? $product['name'],
            'description' => $description ?? $synopsis,
            'release_date' => $releaseDate,
            'provider_item_id' => (int) $providerItemId,
            'platform' => $providerKey === 'igdb' ? json_encode($platforms, JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE) : null,
            'rating' => null,
            'rating_count' => null,
            'developer' => null,
            'publisher' => null,
            'genre' => $providerKey === 'igdb' && ! empty($genres)
                ? json_encode($genres, JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE)
                : null,
            'raw_payload' => ! empty($providerPayload)
                ? json_encode($providerPayload, JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE)
                : null,
            'created_at' => $product['created_at'],
            'updated_at' => $product['updated_at'],
        ];
    }
}

// Write transformed products
$productsHeaders = [
    'id',
    'type',
    'name',
    'slug',
    'platform',
    'category',
    'title',
    'normalized_title',
    'synopsis',
    'release_date',
    'popularity_score',
    'rating',
    'external_ids',
    'metadata',
    'created_at',
    'updated_at',
];
writeCsv("$baseDir/products_TRANSFORMED.csv", $newProducts, $productsHeaders);

$titlesHeaders = ['id', 'product_id', 'name', 'normalized_title', 'slug', 'providers', 'created_at', 'updated_at'];
writeCsv("$baseDir/video_game_titles_TRANSFORMED.csv", $videoGameTitles, $titlesHeaders);

$videoGamesHeaders = ['id', 'video_game_title_id', 'slug', 'provider', 'external_id', 'name', 'description', 'summary',
    'storyline', 'url', 'release_date', 'platform', 'rating', 'rating_count', 'developer', 'publisher', 'genre', 'media',
    'source_payload', 'created_at', 'updated_at'];
writeCsv("$baseDir/video_games_TRANSFORMED.csv", $videoGames, $videoGamesHeaders);

$sourcesHeaders = ['video_game_title_id', 'video_game_source_id', 'provider', 'external_id', 'slug', 'name',
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
