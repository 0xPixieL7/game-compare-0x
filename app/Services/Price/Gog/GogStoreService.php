<?php

declare(strict_types=1);

namespace App\Services\Price\Gog;

use Illuminate\Http\Client\PendingRequest;
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
        $slug = $this->extractSlug($url);
        if (! $slug) {
            return null;
        }

        $apiUrl = "https://catalog.gog.com/v1/catalog?limit=1&slug={$slug}&countryCode={$countryCode}";

        try {
            $response = $this->request()->get($apiUrl);

            if ($response->failed()) {
                Log::warning('GogStoreService: Request failed', [
                    'status' => $response->status(),
                    'url' => $apiUrl,
                    'body' => substr((string) $response->body(), 0, 500),
                ]);

                return null;
            }

            $data = $response->json();

            if (empty($data['products'])) {
                Log::warning('GogStoreService: No products returned', [
                    'url' => $apiUrl,
                    'slug' => $slug,
                    'keys' => is_array($data) ? array_keys($data) : null,
                ]);

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
                'movies' => $this->fetchVideos($product['id'] ?? null),
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

    private function request(): PendingRequest
    {
        return Http::withHeaders([
            'Accept' => 'application/json',
            'Accept-Language' => 'en-US,en;q=0.9',
            'User-Agent' => 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0 Safari/537.36',
        ])
            ->connectTimeout(5)
            ->timeout(10)
            ->retry([200, 500, 1000], throw: false);
    }

    private function extractSlug(string $input): ?string
    {
        if (preg_match('/\/game\/([^\/\?]+)/', $input, $matches)) {
            return $matches[1];
        }

        if (preg_match('/^https?:\/\//', $input) === 1) {
            $path = parse_url($input, PHP_URL_PATH) ?? '';
            $segments = array_values(array_filter(explode('/', trim($path, '/'))));
            $last = end($segments);

            return $last ?: null;
        }

        return $input !== '' ? $input : null;
    }

    private function fetchVideos(?string $productId): array
    {
        if (! $productId) {
            return [];
        }

        try {
            $response = $this->request()->get("https://api.gog.com/products/{$productId}?expand=videos");

            if ($response->failed()) {
                return [];
            }

            $data = $response->json();
            $videos = $data['videos'] ?? [];

            return array_map(function ($video) {
                return [
                    'url' => $video['video_url'] ?? null,
                    'thumbnail_url' => $video['thumbnail_url'] ?? null,
                    'provider' => $video['provider'] ?? 'unknown',
                ];
            }, $videos);
        } catch (\Exception $e) {
            Log::warning('GogStoreService: Failed to fetch videos', ['error' => $e->getMessage()]);

            return [];
        }
    }
}
