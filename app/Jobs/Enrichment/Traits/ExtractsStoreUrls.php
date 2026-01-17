<?php

declare(strict_types=1);

namespace App\Jobs\Enrichment\Traits;

/**
 * Trait for extracting store IDs from retailer URLs.
 *
 * Parses URLs from IGDB websites, RAWG, and other sources to extract
 * store-specific app/product IDs for price fetching.
 */
trait ExtractsStoreUrls
{
    /**
     * IGDB website category to store name mapping.
     *
     * @var array<int, string>
     */
    protected const IGDB_STORE_CATEGORIES = [
        13 => 'steam',
        15 => 'itch',
        16 => 'epicgames',
        17 => 'gog',
    ];

    /**
     * Extract app/product ID from a store URL.
     *
     * @return string|null The extracted ID or null if not found
     */
    protected function extractStoreAppId(string $url, string $store): ?string
    {
        return match (strtolower($store)) {
            'steam' => $this->extractSteamAppId($url),
            'gog' => $this->extractGogProductId($url),
            'epicgames', 'epic_games', 'epic' => $this->extractEpicSlug($url),
            'itch', 'itch_io' => $this->extractItchSlug($url),
            'playstation', 'psn', 'playstation_store' => $this->extractPsnProductId($url),
            'xbox', 'microsoft' => $this->extractXboxProductId($url),
            'nintendo', 'eshop' => $this->extractNintendoProductId($url),
            default => null,
        };
    }

    /**
     * Extract Steam app ID from URL.
     *
     * Handles: store.steampowered.com/app/123456/
     */
    protected function extractSteamAppId(string $url): ?string
    {
        if (preg_match('/store\.steampowered\.com\/app\/(\d+)/', $url, $matches)) {
            return $matches[1];
        }

        return null;
    }

    /**
     * Extract GOG product slug from URL.
     *
     * Handles: gog.com/game/product_slug, gog.com/en/game/product_slug
     */
    protected function extractGogProductId(string $url): ?string
    {
        if (preg_match('/gog\.com\/(?:en\/)?game\/([^\\/\\?]+)/', $url, $matches)) {
            return $matches[1];
        }

        return null;
    }

    /**
     * Extract Epic Games slug from URL.
     *
     * Handles: store.epicgames.com/p/product-slug, store.epicgames.com/en-US/p/product-slug
     */
    protected function extractEpicSlug(string $url): ?string
    {
        if (preg_match('/store\.epicgames\.com\/(?:[a-z]{2}-[A-Z]{2}\/)?p\/([^\\/\\?]+)/', $url, $matches)) {
            return $matches[1];
        }

        return null;
    }

    /**
     * Extract Itch.io slug from URL.
     *
     * Handles: developer.itch.io/game-name
     * Returns: developer/game-name
     */
    protected function extractItchSlug(string $url): ?string
    {
        if (preg_match('/([^\\/]+)\.itch\.io\/([^\\/\\?]+)/', $url, $matches)) {
            return $matches[1].'/'.$matches[2];
        }

        return null;
    }

    /**
     * Extract PlayStation Store product ID from URL.
     *
     * Handles: store.playstation.com/en-us/product/UP0001-CUSA00001_00-GAME000000000001
     */
    protected function extractPsnProductId(string $url): ?string
    {
        if (preg_match('/store\.playstation\.com\/[a-z]{2}-[a-z]{2}\/product\/([A-Z0-9_-]+)/', $url, $matches)) {
            return $matches[1];
        }

        return null;
    }

    /**
     * Extract Xbox/Microsoft Store product ID from URL.
     *
     * Handles: xbox.com/en-us/games/store/game-name/9NBLGGH5FV84
     */
    protected function extractXboxProductId(string $url): ?string
    {
        if (preg_match('/xbox\.com\/[a-z]{2}-[a-z]{2}\/games\/store\/[^\/]+\/([A-Z0-9]+)/', $url, $matches)) {
            return $matches[1];
        }

        // Also handle microsoft.com store URLs
        if (preg_match('/microsoft\.com\/[a-z]{2}-[a-z]{2}\/p\/[^\/]+\/([A-Z0-9]+)/', $url, $matches)) {
            return $matches[1];
        }

        return null;
    }

    /**
     * Extract Nintendo eShop product ID from URL.
     *
     * Handles: nintendo.com/store/products/game-name-switch/
     */
    protected function extractNintendoProductId(string $url): ?string
    {
        if (preg_match('/nintendo\.com\/store\/products\/([^\\/\\?]+)/', $url, $matches)) {
            return $matches[1];
        }

        return null;
    }

    /**
     * Normalize store name from IGDB/RAWG to internal provider naming.
     */
    protected function normalizeStoreProvider(string $store): ?string
    {
        return match (strtolower($store)) {
            'steam' => 'steam_store',
            'gog' => 'gog',
            'epicgames', 'epic games', 'epic' => 'epic_games',
            'itch', 'itch.io' => 'itch_io',
            'playstation', 'psn', 'playstation store' => 'playstation_store',
            'xbox', 'microsoft', 'microsoft store' => 'xbox',
            'nintendo', 'eshop', 'nintendo eshop' => 'nintendo_eshop',
            default => null,
        };
    }

    /**
     * Check if a website category is a store (price-providing).
     */
    protected function isStoreCategory(int $category): bool
    {
        return isset(self::IGDB_STORE_CATEGORIES[$category]);
    }

    /**
     * Get store name from IGDB website category.
     */
    protected function getStoreFromCategory(int $category): ?string
    {
        return self::IGDB_STORE_CATEGORIES[$category] ?? null;
    }
}
