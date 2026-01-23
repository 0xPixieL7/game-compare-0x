<?php

declare(strict_types=1);

namespace App\Services\Price\PlayStation;

use Illuminate\Support\Facades\Http;
use Illuminate\Support\Facades\Log;

/**
 * PlayStation Store Service using the community PlayStation Store API.
 * Package: https://github.com/mrt1m/playstation-store-api
 */
final class PlayStationStoreService
{
    /**
     * Get full details including price, media, and metadata.
     * 
     * @param string $productId PlayStation product ID (e.g., "UP0001-CUSA00744_00-GTAVDIGITALDOWNL")
     * @param string $country Country code (US, GB, DE, etc.)
     * @param string $language Language code (en, de, fr, etc.)
     */
    public function getFullDetails(string $productId, string $country = 'US', string $language = 'en'): ?array
    {
        try {
            // PlayStation Store API endpoint
            // Using community API: https://store.playstation.com/api/chihiro/00_09_000/container/{country}/{language}/999/{productId}
            $apiUrl = "https://store.playstation.com/store/api/chihiro/00_09_000/container/{$country}/{$language}/999/{$productId}";

            $response = Http::timeout(10)
                ->withHeaders([
                    'User-Agent' => 'game-compare/1.0',
                ])
                ->get($apiUrl);

            if ($response->failed()) {
                return null;
            }

            $data = $response->json();

            if (empty($data)) {
                return null;
            }

            return [
                'price' => $this->extractPrice($data, $country),
                'media' => $this->extractMedia($data),
                'metadata' => $this->extractMetadata($data),
            ];
        } catch (\Throwable $e) {
            Log::debug('PlayStationStoreService: full details lookup failed', [
                'product_id' => $productId,
                'country' => $country,
                'error' => $e->getMessage(),
            ]);

            return null;
        }
    }

    /**
     * Extract price data from PlayStation response.
     */
    private function extractPrice(array $data, string $country): ?array
    {
        $defaultSku = $data['default_sku'] ?? null;
        if (!$defaultSku) {
            return null;
        }

        // Price info
        $price = $defaultSku['price'] ?? null;
        $displayPrice = $defaultSku['display_price'] ?? null;

        if (!$price || !is_numeric($price)) {
            return null;
        }

        // PlayStation prices are usually in cents already
        $amountMinor = (int) $price;
        
        // Determine currency from country
        $currency = $this->getCurrencyForCountry($country);

        return [
            'currency' => $currency,
            'amount_minor' => $amountMinor,
            'display_price' => $displayPrice,
            'is_free' => ($defaultSku['is_free'] ?? false),
            'country' => $country,
        ];
    }

    /**
     * Extract media (images, videos) from PlayStation response.
     */
    private function extractMedia(array $data): array
    {
        $images = [];
        $videos = [];

        // Extract screenshots
        $screenshots = $data['screenshots'] ?? [];
        foreach ($screenshots as $screenshot) {
            $images[] = [
                'url' => $screenshot['url'] ?? null,
                'type' => 'screenshot',
            ];
        }

        // Extract videos/trailers
        $mediaList = $data['mediaList'] ?? $data['media_list'] ?? [];
        foreach ($mediaList as $media) {
            if (isset($media['type']) && $media['type'] === 'video') {
                $videos[] = [
                    'url' => $media['url'] ?? null,
                    'thumbnail' => $media['preview_url'] ?? $media['thumbnail'] ?? null,
                    'type' => 'trailer',
                ];
            }
        }

        // Box art / Cover images
        $images[] = [
            'url' => $data['images'][0]['url'] ?? null,
            'type' => 'cover',
        ];

        return [
            'images' => array_filter($images, fn($img) => !empty($img['url'])),
            'videos' => array_filter($videos, fn($vid) => !empty($vid['url'])),
            'cover_image' => $data['images'][0]['url'] ?? null,
            'screenshots' => array_filter($images, fn($img) => ($img['type'] ?? '') === 'screenshot'),
        ];
    }

    /**
     * Extract metadata from PlayStation response.
     */
    private function extractMetadata(array $data): array
    {
        return [
            'title' => $data['name'] ?? null,
            'description' => $data['long_desc'] ?? $data['description'] ?? null,
            'short_description' => $data['short_desc'] ?? null,
            'publisher' => $data['provider_name'] ?? null,
            'release_date' => $data['release_date'] ?? null,
            'genres' => $data['genres'] ?? [],
            'platforms' => $data['playable_platform'] ?? [],
        ];
    }

    /**
     * Get price only (legacy method for backward compatibility).
     */
    public function getPrice(string $productId, string $country = 'US', string $language = 'en'): ?array
    {
        $fullDetails = $this->getFullDetails($productId, $country, $language);
        return $fullDetails['price'] ?? null;
    }

    /**
     * Map country code to currency.
     */
    private function getCurrencyForCountry(string $country): string
    {
        return match(strtoupper($country)) {
            'GB', 'UK' => 'GBP',
            'JP' => 'JPY',
            'KR' => 'KRW',
            'BR' => 'BRL',
            'CA' => 'CAD',
            'AU' => 'AUD',
            'NZ' => 'NZD',
            'RU' => 'RUB',
            'IN' => 'INR',
            'TR' => 'TRY',
            'ZA' => 'ZAR',
            'SG' => 'SGD',
            'DE', 'FR', 'ES', 'IT', 'NL', 'BE', 'AT', 'IE', 'FI', 'PT' => 'EUR',
            default => 'USD',
        };
    }
}
