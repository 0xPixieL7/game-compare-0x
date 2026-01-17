<?php

declare(strict_types=1);

namespace App\Jobs\Enrichment\Traits;

/**
 * Trait for building IGDB image URLs with size variants.
 *
 * IGDB uses a CDN with size prefixes for different image resolutions.
 * This trait provides consistent URL building across all IGDB-related jobs.
 *
 * @see https://api-docs.igdb.com/#images
 */
trait BuildsIgdbImageUrls
{
    /**
     * IGDB image CDN base URL.
     */
    protected const IGDB_IMAGE_BASE_URL = 'https://images.igdb.com/igdb/image/upload';

    /**
     * IGDB image size prefixes.
     *
     * @var array<string, string>
     */
    protected const IGDB_IMAGE_SIZES = [
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
     * Build full IGDB image URL for a given size.
     */
    protected function buildIgdbImageUrl(string $imageId, string $size = 'cover_big'): string
    {
        $sizePrefix = self::IGDB_IMAGE_SIZES[$size] ?? self::IGDB_IMAGE_SIZES['cover_big'];

        return self::IGDB_IMAGE_BASE_URL."/{$sizePrefix}/{$imageId}.jpg";
    }

    /**
     * Build all size variants for an IGDB image.
     *
     * @return array<string, string>
     */
    protected function buildIgdbSizeVariants(string $imageId): array
    {
        $variants = [];

        foreach (self::IGDB_IMAGE_SIZES as $name => $prefix) {
            $variants[$name] = self::IGDB_IMAGE_BASE_URL."/{$prefix}/{$imageId}.jpg";
        }

        return $variants;
    }

    /**
     * Get the recommended size for a specific image type.
     */
    protected function getRecommendedSize(string $imageType): string
    {
        return match ($imageType) {
            'cover' => 'cover_big',
            'screenshot' => 'screenshot_huge',
            'artwork' => '1080p',
            'logo' => 'logo_med',
            default => 'cover_big',
        };
    }
}
