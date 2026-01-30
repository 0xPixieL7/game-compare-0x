<?php

declare(strict_types=1);

namespace App\Services\Price\EpicGames;

use Illuminate\Http\Client\PendingRequest;
use Illuminate\Support\Facades\Http;
use Illuminate\Support\Facades\Log;

class EpicGamesStoreService
{
    /**
     * Get price from Epic Games Store.
     * Note: Epic doesn't have a simple public API by URL.
     * We often need the product slug/namespace.
     * For scraping from URL like: https://store.epicgames.com/en-US/p/path-of-exile-2
     */
    public function getPrice(string $url, string $countryCode = 'US'): ?array
    {
        $details = $this->getFullDetails($url, $countryCode);

        return $details ? $details['price'] : null;
    }

    /**
     * Get full details including media from Epic Games.
     */
    public function getFullDetails(string $url, string $countryCode = 'US'): ?array
    {
        $slug = $this->extractSlug($url);
        if (! $slug) {
            return null;
        }

        // Use the same endpoint as getPrice but process for media too
        // Map country code to locale. Epic uses standard locales like en-US, fr-FR.
        // A simple heuristic: lower-UPPER.
        $locale = strtolower($countryCode).'-'.strtoupper($countryCode);
        if ($countryCode === 'US' || $countryCode === 'UK') {
            $locale = 'en-'.($countryCode === 'UK' ? 'GB' : 'US');
        }

        $contentLocale = 'en-US';
        $apiUrl = "https://store-content-ipv4.ak.epicgames.com/api/{$contentLocale}/content/products/{$slug}";

        try {
            $response = $this->request()->get($apiUrl);

            if ($response->failed()) {
                Log::warning('EpicGamesStoreService: Request failed', [
                    'status' => $response->status(),
                    'url' => $apiUrl,
                    'body' => substr((string) $response->body(), 0, 500),
                ]);

                return null;
            }

            $data = $response->json();

            // Extract Offer Identifiers
            $offer = $data['pages'][0]['offer'] ?? null;
            $offerId = is_array($offer) ? ($offer['id'] ?? null) : null;
            $namespace = is_array($offer) ? ($offer['namespace'] ?? null) : null;

            // Extract Price via Catalog API
            $price = null;
            $priceData = null;

            if ($offerId && $namespace) {
                $priceData = $this->fetchOfferPrice($namespace, $offerId, $countryCode, $locale);
            }

            if ($priceData) {
                $price = [
                    'amount_minor' => $priceData['amount_minor'],
                    'currency' => $priceData['currency'],
                ];
            } else {
                Log::warning('EpicGamesStoreService: Price missing', [
                    'url' => $apiUrl,
                    'slug' => $slug,
                    'offer_id' => $offerId,
                    'namespace' => $namespace,
                ]);
            }

            // Extract Media
            // Epic pages data structure:
            // "pages": [ { "data": { "about": { "image": { "src": "..." } }, "gallery": { "galleryImages": [...] }, "carousel": { "items": [...] } } } ]

            $media = [
                'screenshots' => [],
                'header_image' => null,
                'background' => null,
                'movies' => $this->extractVideos($data['pages'][0]['data'] ?? []),
            ];

            // Attempt to find images in the complex JSON structure
            // This is a simplified traversal
            if (isset($data['pages'][0]['data']['gallery']['galleryImages'])) {
                foreach ($data['pages'][0]['data']['gallery']['galleryImages'] as $img) {
                    if (isset($img['src'])) {
                        $media['screenshots'][] = [
                            'full' => $img['src'],
                            'thumbnail' => $img['src'].'?w=300', // fast hack
                        ];
                    }
                }
            }

            // Header image
            if (isset($data['pages'][0]['data']['about']['image']['src'])) {
                $media['header_image'] = $data['pages'][0]['data']['about']['image']['src'];
            }

            return [
                'price' => $price,
                'media' => $media,
            ];

        } catch (\Exception $e) {
            Log::error('EpicGamesStoreService: '.$e->getMessage());

            return null;
        }
    }

    private function scrapeJsonLd(string $url): ?array
    {
        try {
            $response = Http::withHeaders(['User-Agent' => 'Mozilla/5.0...'])->get($url);
            if ($response->failed()) {
                return null;
            }

            $html = $response->body();

            // Look for specific price patterns in HTML if JSON-LD is missing
            // Epic usually renders client-side, so static scraping is hard without Puppeteer/Browser tools.
            // However, they sometimes embed initial state.

            return null;
        } catch (\Exception $e) {
            return null;
        }
    }

    private function request(): PendingRequest
    {
        $headers = [
            'Accept' => 'application/json',
            'Accept-Language' => 'en-US,en;q=0.9',
            'User-Agent' => 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0 Safari/537.36',
        ];

        $bearer = config('services.epic.bearer_token');
        if (is_string($bearer) && $bearer !== '') {
            $headers['Authorization'] = 'bearer '.$bearer;
        }

        $deviceId = config('services.epic.device_id');
        if (is_string($deviceId) && $deviceId !== '') {
            $headers['X-Epic-Device-Id'] = $deviceId;
        }

        return Http::withHeaders($headers)
            ->connectTimeout(5)
            ->timeout(10)
            ->retry([200, 500, 1000], throw: false);
    }

    private function fetchOfferPrice(string $namespace, string $offerId, string $countryCode, string $locale): ?array
    {
        $url = "https://catalog-public-service-prod06.ol.epicgames.com/catalog/api/shared/namespace/{$namespace}/offers/{$offerId}?country={$countryCode}&locale={$locale}";

        $response = $this->request()->get($url);

        if ($response->failed()) {
            Log::warning('EpicGamesStoreService: Offer request failed', [
                'status' => $response->status(),
                'url' => $url,
                'body' => substr((string) $response->body(), 0, 500),
            ]);

            return null;
        }

        Log::info('EpicGamesStoreService: Offer response ok', [
            'url' => $url,
            'body' => substr((string) $response->body(), 0, 500),
        ]);

        $data = $response->json();
        $currency = $data['currencyCode'] ?? 'USD';
        $decimals = (int) ($data['currencyDecimals'] ?? 2);

        if (isset($data['price']['totalPrice']) && is_array($data['price']['totalPrice'])) {
            $total = $data['price']['totalPrice'];

            return [
                'amount_minor' => (int) ($total['discountPrice'] ?? 0),
                'currency' => $total['currencyCode'] ?? $currency,
            ];
        }

        $raw = $data['currentPrice'] ?? $data['price'] ?? null;
        if ($raw === null) {
            return null;
        }

        $amountMinor = is_numeric($raw) ? (int) $raw : 0;
        if ($amountMinor === 0 && $decimals > 0 && is_numeric($raw)) {
            $amountMinor = (int) round(((float) $raw) * (10 ** $decimals));
        }

        return [
            'amount_minor' => $amountMinor,
            'currency' => $currency,
        ];
    }

    private function extractSlug(string $input): ?string
    {
        if (preg_match('/\/p\/([^\/\?]+)/', $input, $matches)) {
            return $matches[1];
        }

        if (preg_match('/^https?:\/\//', $input) === 1) {
            $path = parse_url($input, PHP_URL_PATH) ?? '';
            $segments = array_values(array_filter(explode('/', trim($path, '/'))));

            foreach ($segments as $index => $segment) {
                if ($segment === 'p' && isset($segments[$index + 1])) {
                    return $segments[$index + 1];
                }
            }

            $last = end($segments);

            return $last ?: null;
        }

        return $input !== '' ? $input : null;
    }

    private function extractVideos(array $pageData): array
    {
        $videos = [];

        // Extract from carousel items
        $carouselItems = $pageData['carousel']['items'] ?? [];

        foreach ($carouselItems as $item) {
            if (! isset($item['video'])) {
                continue;
            }

            $video = $item['video'];
            $recipes = json_decode($video['recipes'] ?? '{}', true);
            $enUsRecipes = $recipes['en-US'] ?? [];

            // Prefer HLS (m3u8) for compatibility
            $hlsRecipe = collect($enUsRecipes)->firstWhere('recipe', 'video-hls');
            $thumbnail = null;
            $url = null;

            if ($hlsRecipe) {
                foreach ($hlsRecipe['outputs'] ?? [] as $output) {
                    if ($output['key'] === 'manifest') {
                        $url = $output['url'];
                    }
                    if ($output['key'] === 'thumbnail') {
                        $thumbnail = $output['url'];
                    }
                }
            }

            if ($url) {
                $videos[] = [
                    'url' => $url,
                    'thumbnail_url' => $thumbnail,
                    'type' => 'hls',
                ];
            }
        }

        return $videos;
    }
}
