<?php

declare(strict_types=1);

namespace App\Services\Igdb;

use Illuminate\Http\Client\Response;
use Illuminate\Support\Facades\Cache;
use Illuminate\Support\Facades\Http;
use Illuminate\Support\Facades\Log;

/**
 * IGDB Media Service.
 *
 * Handles fetching media (covers, screenshots, artworks, videos) from IGDB API.
 * Manages OAuth token lifecycle and provides clean media data structures.
 */
class IgdbMediaService
{
    private const BASE_URL = 'https://api.igdb.com/v4';

    private const TOKEN_CACHE_KEY = 'igdb_oauth_token';

    private const TOKEN_CACHE_TTL = 3600; // 1 hour (tokens last ~60 days but refresh often)

    private const IMAGE_BASE_URL = 'https://images.igdb.com/igdb/image/upload';

    /**
     * IGDB website category mappings.
     *
     * @see https://api-docs.igdb.com/#website-category-enum
     */
    public const WEBSITE_CATEGORIES = [
        1 => 'official',
        2 => 'wikia',
        3 => 'wikipedia',
        4 => 'facebook',
        5 => 'twitter',
        6 => 'twitch',
        8 => 'instagram',
        9 => 'youtube',
        10 => 'iphone',
        11 => 'ipad',
        12 => 'android',
        13 => 'steam',
        14 => 'reddit',
        15 => 'itch',
        16 => 'epicgames',
        17 => 'gog',
        18 => 'discord',
    ];

    /**
     * Store categories for price discovery.
     */
    public const STORE_CATEGORIES = [13, 15, 16, 17]; // Steam, Itch, Epic, GOG

    /**
     * IGDB image size prefixes.
     *
     * @see https://api-docs.igdb.com/#images
     */
    public const IMAGE_SIZES = [
        'thumb' => 't_thumb',           // 90x128
        'cover_small' => 't_cover_small', // 90x128
        'cover_big' => 't_cover_big',   // 264x374
        'logo_med' => 't_logo_med',     // 284x160
        'screenshot_med' => 't_screenshot_med', // 569x320
        'screenshot_big' => 't_screenshot_big', // 889x500
        'screenshot_huge' => 't_screenshot_huge', // 1280x720
        '720p' => 't_720p',             // 1280x720
        '1080p' => 't_1080p',           // 1920x1080
    ];

    /**
     * Fetch comprehensive data for a game by IGDB ID.
     * Includes media (cover, screenshots, artworks, videos) AND store URLs.
     *
     * @return array{cover: array|null, screenshots: array, artworks: array, videos: array, stores: array, websites: array}
     */
    public function fetchFullDataForGame(int $igdbId): array
    {
        $token = $this->getAccessToken();

        if (! $token) {
            Log::warning('IgdbMediaService: Failed to obtain access token');

            return $this->emptyFullResponse();
        }

        $query = $this->buildFullQuery($igdbId);

        $response = Http::withHeaders([
            'Client-ID' => config('services.igdb.client_id'),
            'Authorization' => "Bearer {$token}",
        ])->withBody($query, 'text/plain')->post(self::BASE_URL.'/games');

        if (! $response->successful()) {
            Log::warning('IgdbMediaService: API request failed', [
                'igdb_id' => $igdbId,
                'status' => $response->status(),
                'body' => $response->body(),
            ]);

            return $this->emptyFullResponse();
        }

        $games = $response->json();

        if (empty($games)) {
            return $this->emptyFullResponse();
        }

        return $this->parseFullResponse($games[0]);
    }

    /**
     * Fetch media for a game by IGDB ID (legacy method for backward compatibility).
     *
     * @return array{cover: array|null, screenshots: array, artworks: array, videos: array}
     */
    public function fetchMediaForGame(int $igdbId): array
    {
        $fullData = $this->fetchFullDataForGame($igdbId);

        return [
            'cover' => $fullData['cover'],
            'screenshots' => $fullData['screenshots'],
            'artworks' => $fullData['artworks'],
            'videos' => $fullData['videos'],
        ];
    }

    /**
     * Get or refresh OAuth access token.
     */
    public function getAccessToken(): ?string
    {
        $cached = Cache::get(self::TOKEN_CACHE_KEY);

        if ($cached) {
            return $cached;
        }

        $clientId = config('services.igdb.client_id');
        $clientSecret = config('services.igdb.client_secret');

        if (! $clientId || ! $clientSecret) {
            Log::error('IgdbMediaService: Missing IGDB_CLIENT_ID or IGDB_CLIENT_SECRET');

            return null;
        }

        /** @var Response $response */
        $response = Http::asForm()->post('https://id.twitch.tv/oauth2/token', [
            'client_id' => $clientId,
            'client_secret' => $clientSecret,
            'grant_type' => 'client_credentials',
        ]);

        if (! $response->successful()) {
            Log::error('IgdbMediaService: OAuth token request failed', [
                'status' => $response->status(),
                'body' => $response->body(),
            ]);

            return null;
        }

        $token = $response->json('access_token');

        if ($token) {
            // Cache slightly less than expiry to avoid race conditions
            $expiresIn = $response->json('expires_in', self::TOKEN_CACHE_TTL) - 300;
            Cache::put(self::TOKEN_CACHE_KEY, $token, max(60, $expiresIn));
        }

        return $token;
    }

    /**
     * Build IGDB API query for full data (media + websites).
     */
    private function buildFullQuery(int $igdbId): string
    {
        $fields = implode(',', [
            'id',
            'name',
            'cover.image_id',
            'cover.width',
            'cover.height',
            'cover.checksum',
            'screenshots.image_id',
            'screenshots.width',
            'screenshots.height',
            'screenshots.checksum',
            'artworks.image_id',
            'artworks.width',
            'artworks.height',
            'artworks.checksum',
            'videos.video_id',
            'videos.name',
            'websites.category',
            'websites.url',
            'websites.trusted',
        ]);

        return "fields {$fields}; where id = {$igdbId};";
    }

    /**
     * Build IGDB API query for media fields only (legacy).
     */
    private function buildMediaQuery(int $igdbId): string
    {
        $fields = implode(',', [
            'id',
            'name',
            'cover.image_id',
            'cover.width',
            'cover.height',
            'cover.checksum',
            'screenshots.image_id',
            'screenshots.width',
            'screenshots.height',
            'screenshots.checksum',
            'artworks.image_id',
            'artworks.width',
            'artworks.height',
            'artworks.checksum',
            'videos.video_id',
            'videos.name',
        ]);

        return "fields {$fields}; where id = {$igdbId};";
    }

    /**
     * Parse IGDB API response into full data structure.
     *
     * @return array{cover: array|null, screenshots: array, artworks: array, videos: array, stores: array, websites: array}
     */
    private function parseFullResponse(array $game): array
    {
        $websites = $this->parseWebsites($game['websites'] ?? []);

        return [
            'cover' => $this->parseCover($game['cover'] ?? null),
            'screenshots' => $this->parseImages($game['screenshots'] ?? [], 'screenshot_huge'),
            'artworks' => $this->parseImages($game['artworks'] ?? [], '1080p'),
            'videos' => $this->parseVideos($game['videos'] ?? []),
            'stores' => $websites['stores'],
            'websites' => $websites['all'],
        ];
    }

    /**
     * Parse IGDB API response into structured media data (legacy).
     *
     * @return array{cover: array|null, screenshots: array, artworks: array, videos: array}
     */
    private function parseMediaResponse(array $game): array
    {
        return [
            'cover' => $this->parseCover($game['cover'] ?? null),
            'screenshots' => $this->parseImages($game['screenshots'] ?? [], 'screenshot_huge'),
            'artworks' => $this->parseImages($game['artworks'] ?? [], '1080p'),
            'videos' => $this->parseVideos($game['videos'] ?? []),
        ];
    }

    /**
     * Parse websites and extract store URLs for price discovery.
     *
     * @return array{stores: array, all: array}
     */
    private function parseWebsites(array $websites): array
    {
        $stores = [];
        $all = [];

        foreach ($websites as $website) {
            $category = $website['category'] ?? 0;
            $url = $website['url'] ?? null;

            if (! $url) {
                continue;
            }

            $categoryName = self::WEBSITE_CATEGORIES[$category] ?? 'unknown';

            $parsed = [
                'category' => $category,
                'category_name' => $categoryName,
                'url' => $url,
                'trusted' => $website['trusted'] ?? false,
            ];

            $all[] = $parsed;

            // Extract store URLs for price discovery
            if (in_array($category, self::STORE_CATEGORIES, true)) {
                $storeData = $this->parseStoreUrl($url, $categoryName);
                if ($storeData) {
                    $stores[] = $storeData;
                }
            }
        }

        return [
            'stores' => $stores,
            'all' => $all,
        ];
    }

    /**
     * Parse store URL to extract store-specific data.
     *
     * @return array{store: string, url: string, app_id: string|null}|null
     */
    private function parseStoreUrl(string $url, string $categoryName): ?array
    {
        $data = [
            'store' => $categoryName,
            'url' => $url,
            'app_id' => null,
        ];

        // Extract Steam app ID from URL
        if ($categoryName === 'steam' && preg_match('/store\.steampowered\.com\/app\/(\d+)/', $url, $matches)) {
            $data['app_id'] = $matches[1];
        }

        // Extract GOG product slug
        if ($categoryName === 'gog' && preg_match('/gog\.com\/(?:en\/)?game\/([^\/\?]+)/', $url, $matches)) {
            $data['app_id'] = $matches[1];
        }

        // Extract Epic Games slug
        if ($categoryName === 'epicgames' && preg_match('/store\.epicgames\.com\/(?:en-US\/)?p\/([^\/\?]+)/', $url, $matches)) {
            $data['app_id'] = $matches[1];
        }

        // Extract Itch.io slug
        if ($categoryName === 'itch' && preg_match('/([^\/]+)\.itch\.io\/([^\/\?]+)/', $url, $matches)) {
            $data['app_id'] = $matches[1].'/'.$matches[2];
        }

        return $data;
    }

    /**
     * Parse cover image data.
     *
     * @return array{image_id: string, url: string, size_variants: array, width: int|null, height: int|null, checksum: string|null}|null
     */
    private function parseCover(?array $cover): ?array
    {
        if (! $cover || ! isset($cover['image_id'])) {
            return null;
        }

        $imageId = $cover['image_id'];

        return [
            'image_id' => $imageId,
            'url' => $this->buildImageUrl($imageId, 'cover_big'),
            'size_variants' => $this->buildSizeVariants($imageId),
            'width' => $cover['width'] ?? null,
            'height' => $cover['height'] ?? null,
            'checksum' => $cover['checksum'] ?? null,
        ];
    }

    /**
     * Parse array of images (screenshots or artworks).
     *
     * @return array<int, array{image_id: string, url: string, size_variants: array, width: int|null, height: int|null, checksum: string|null}>
     */
    private function parseImages(array $images, string $defaultSize): array
    {
        return array_values(array_filter(array_map(function ($image) use ($defaultSize) {
            if (! isset($image['image_id'])) {
                return null;
            }

            $imageId = $image['image_id'];

            return [
                'image_id' => $imageId,
                'url' => $this->buildImageUrl($imageId, $defaultSize),
                'size_variants' => $this->buildSizeVariants($imageId),
                'width' => $image['width'] ?? null,
                'height' => $image['height'] ?? null,
                'checksum' => $image['checksum'] ?? null,
            ];
        }, $images)));
    }

    /**
     * Parse array of videos.
     *
     * @return array<int, array{video_id: string, url: string, provider: string, name: string|null}>
     */
    private function parseVideos(array $videos): array
    {
        return array_values(array_filter(array_map(function ($video) {
            if (! isset($video['video_id'])) {
                return null;
            }

            return [
                'video_id' => $video['video_id'],
                'url' => 'https://www.youtube.com/watch?v='.$video['video_id'],
                'thumbnail_url' => 'https://img.youtube.com/vi/'.$video['video_id'].'/maxresdefault.jpg',
                'provider' => 'youtube',
                'name' => $video['name'] ?? null,
            ];
        }, $videos)));
    }

    /**
     * Build full image URL for a given size.
     */
    public function buildImageUrl(string $imageId, string $size = 'cover_big'): string
    {
        $sizePrefix = self::IMAGE_SIZES[$size] ?? self::IMAGE_SIZES['cover_big'];

        return self::IMAGE_BASE_URL."/{$sizePrefix}/{$imageId}.jpg";
    }

    /**
     * Build all size variants for an image.
     *
     * @return array<string, string>
     */
    public function buildSizeVariants(string $imageId): array
    {
        $variants = [];

        foreach (self::IMAGE_SIZES as $name => $prefix) {
            $variants[$name] = self::IMAGE_BASE_URL."/{$prefix}/{$imageId}.jpg";
        }

        return $variants;
    }

    /**
     * Return empty full response structure.
     *
     * @return array{cover: null, screenshots: array, artworks: array, videos: array, stores: array, websites: array}
     */
    private function emptyFullResponse(): array
    {
        return [
            'cover' => null,
            'screenshots' => [],
            'artworks' => [],
            'videos' => [],
            'stores' => [],
            'websites' => [],
        ];
    }

    /**
     * Return empty media response structure (legacy).
     *
     * @return array{cover: null, screenshots: array, artworks: array, videos: array}
     */
    private function emptyMediaResponse(): array
    {
        return [
            'cover' => null,
            'screenshots' => [],
            'artworks' => [],
            'videos' => [],
        ];
    }

    /**
     * Clear the cached OAuth token.
     */
    public function clearTokenCache(): void
    {
        Cache::forget(self::TOKEN_CACHE_KEY);
    }
}
