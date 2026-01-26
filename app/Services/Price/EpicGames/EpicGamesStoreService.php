<?php

declare(strict_types=1);

namespace App\Services\Price\EpicGames;

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
        if (! preg_match('/\/p\/([^\/\?]+)/', $url, $matches)) {
            return null;
        }
        $slug = $matches[1];

        // Use the same endpoint as getPrice but process for media too
        // Map country code to locale. Epic uses standard locales like en-US, fr-FR.
        // A simple heuristic: lower-UPPER.
        $locale = strtolower($countryCode).'-'.strtoupper($countryCode);
        if ($countryCode === 'US' || $countryCode === 'UK') {
            $locale = 'en-'.($countryCode === 'UK' ? 'GB' : 'US');
        }

        $apiUrl = "https://store-content-ipv4.ak.epicgames.com/api/{$locale}/content/products/slugs/{$slug}";

        try {
            $response = Http::get($apiUrl);

            if ($response->failed()) {
                return null;
            }

            $data = $response->json();

            // Extract Price
            $minor = 0;
            $currency = 'USD';

            if (isset($data['product']['price']['totalPrice']['discountPrice'])) {
                $price = $data['product']['price']['totalPrice'];
                $minor = $price['discountPrice'];
                $currency = $price['currencyCode'];
            }

            // Extract Media
            // Epic pages data structure:
            // "pages": [ { "data": { "about": { "image": { "src": "..." } }, "gallery": { "galleryImages": [...] } } } ]

            $media = [
                'screenshots' => [],
                'header_image' => null,
                'background' => null,
                'movies' => [],
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
                'price' => [
                    'amount_minor' => $minor,
                    'currency' => $currency,
                ],
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
}
