<?php

declare(strict_types=1);

namespace App\Services\Price\Gog;

use Illuminate\Support\Facades\Http;
use Illuminate\Support\Facades\Log;

class GogStoreService
{
    /**
     * Get price from GOG.com
     * API: https://api.gog.com/products/{id}?expand=downloads,expanded_dlcs,related_products,in_series,description,screenshots,videos,related_pages,changelog,z_reviews,critic_reviews
     * or scraper via https://www.gog.com/en/game/{slug}
     */
    public function getPrice(string $url, string $countryCode = 'US'): ?array
    {
        $details = $this->getFullDetails($url, $countryCode);

        return $details ? $details['price'] : null;
    }

    /**
     * Get full details including media from GOG.
     */
    public function getFullDetails(string $url, string $countryCode = 'US'): ?array
    {
        if (! preg_match('/\/game\/([^\/\?]+)/', $url, $matches)) {
            return null;
        }
        $slug = $matches[1];

        $apiUrl = "https://catalog.gog.com/v1/catalog?limit=1&slug={$slug}&countryCode={$countryCode}";

        try {
            $response = Http::get($apiUrl);
            $data = $response->json();

            if (empty($data['products'])) {
                return null;
            }

            $product = $data['products'][0];

            // Extract prices
            $price = $product['price'];
            $amount = $price['finalMoney']['amount'] ?? $price['final'];
            $currency = $price['finalMoney']['currency'] ?? 'USD';
            $minor = (int) (floatval($amount) * 100);

            // Extract Media
            $media = [
                'screenshots' => [],
                'header_image' => $product['coverHorizontal'] ?? null,
                'background' => $product['coverVertical'] ?? null, // Fallback/Alternative
                'movies' => [],
            ];

            // GOG API usually provides screenshots in 'screenshots' array or 'images' depending on endpoint version.
            // Catalog API v1 returns 'screenshots' array: [{ "url": "..." }]
            // Wait, catalog API returns simplified data. 'images' field?

            if (! empty($product['screenshots'])) {
                foreach ($product['screenshots'] as $screen) {
                    // URL format: https://images.gog-statics.com/{id}_{format}.jpg
                    // usually we prefer {format} to be '1920' or similar
                    $baseUrl = str_replace('_{formatter}', '', $screen['url'] ?? ''); // Some URLs have placeholder
                    // Actually GOG URLs in catalog often look like: "https://images.gog-statics.com/..."
                    // and we append .jpg or resolution.
                    // Let's assume the URL is usable or needs simple suffix.

                    // Example: "https://images.gog-statics.com/abc..."
                    // We can try to format if needed, but often they are raw.

                    $media['screenshots'][] = [
                        'full' => $baseUrl.'.jpg', // basic guess
                        'thumbnail' => $baseUrl.'_320.jpg',
                    ];
                }
            }

            return [
                'price' => [
                    'amount_minor' => $minor,
                    'currency' => $currency,
                    'discount_percent' => $product['price']['discountPercentage'] ?? 0,
                ],
                'media' => $media,
            ];

        } catch (\Exception $e) {
            Log::error('GogStoreService: '.$e->getMessage());

            return null;
        }
    }
}
