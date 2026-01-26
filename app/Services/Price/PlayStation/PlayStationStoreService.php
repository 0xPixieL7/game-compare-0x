<?php

declare(strict_types=1);

namespace App\Services\Price\PlayStation;

use GuzzleHttp\Client as GuzzleClient;
use Illuminate\Support\Facades\Log;
use PlaystationStoreApi\Client;
use PlaystationStoreApi\Enum\RegionEnum;
use PlaystationStoreApi\Request\RequestProductById;

/**
 * PlayStation Store Service using the community PlayStation Store API.
 * Package: https://github.com/mrt1m/playstation-store-api
 */
final class PlayStationStoreService
{
    private const API_URL = 'https://web.np.playstation.com/api/graphql/v1/';

    /**
     * Get full details including price, media, and metadata.
     *
     * @param  string  $productId  PlayStation product ID (e.g., "UP0001-CUSA00744_00-GTAVDIGITALDOWNL")
     * @param  string  $country  Country code (US, GB, DE, etc.)
     * @param  string  $language  Language code (en, de, fr, etc.)
     */
    public function getFullDetails(string $productId, string $country = 'US', string $language = 'en'): ?array
    {
        try {
            $region = $this->findRegion($country, $language);

            // Create client with specific region
            $httpClient = new GuzzleClient(['base_uri' => self::API_URL, 'timeout' => 10]);
            $client = new Client($region, $httpClient);

            $response = $client->get(new RequestProductById($productId));

            if (empty($response['data']['productRetrieve'])) {
                return null;
            }

            $product = $response['data']['productRetrieve'];

            return [
                'price' => $this->extractPrice($product, $country),
                'media' => $this->extractMedia($product),
                'metadata' => $this->extractMetadata($product),
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
     * Resolve legacy country/lang to RegionEnum
     */
    private function findRegion(string $country, string $language): RegionEnum
    {
        $country = strtolower($country);
        $language = strtolower($language);
        $target = "{$language}-{$country}";

        // 1. Try exact match
        if ($case = RegionEnum::tryFrom($target)) {
            return $case;
        }

        // 2. Try match by country code
        foreach (RegionEnum::cases() as $case) {
            $parts = explode('-', $case->value);
            if (count($parts) === 2 && $parts[1] === $country) {
                return $case;
            }
        }

        // 3. Fallback
        return RegionEnum::UNITED_STATES;
    }

    /**
     * Extract price data from GraphQL response.
     */
    private function extractPrice(array $data, string $country): ?array
    {
        $priceInfo = $data['price'] ?? null;

        // Handle "free" or missing price
        if (! $priceInfo && ($data['isFree'] ?? false)) {
            return [
                'currency' => $this->getCurrencyForCountry($country),
                'amount_minor' => 0,
                'display_price' => 'Free',
                'is_free' => true,
                'country' => $country,
            ];
        }

        if (! $priceInfo) {
            return null;
        }

        $amountMinor = $priceInfo['basePriceValue'] ?? null;
        $formattedPrice = $priceInfo['basePrice'] ?? null;
        $discountedValue = $priceInfo['discountedPriceValue'] ?? null;
        $discountedPrice = $priceInfo['discountedPrice'] ?? null;

        // Use discounted price if available and lower
        $finalAmount = $discountedValue ?? $amountMinor;
        $finalDisplay = $discountedPrice ?? $formattedPrice;

        if ($finalAmount === null) {
            return null;
        }

        return [
            'currency' => $this->getCurrencyForCountry($country), // API doesn't always return currency code easily
            'amount_minor' => (int) $finalAmount,
            'display_price' => $finalDisplay,
            'is_free' => $finalAmount === 0,
            'country' => $country,
            'discount_percent' => $priceInfo['discountText'] ?? 0, // Sometimes roughly parsed
        ];
    }

    /**
     * Extract media (images, videos) from GraphQL response.
     */
    private function extractMedia(array $data): array
    {
        $images = [];
        $videos = [];

        // Media is usually under 'media' array in GraphQL
        $mediaList = $data['media'] ?? [];

        foreach ($mediaList as $media) {
            $role = $media['role'] ?? 'IMAGE'; // SCREENSHOT, MASTER, etc?
            $type = $media['type'] ?? 'IMAGE'; // IMAGE or VIDEO

            if ($type === 'VIDEO') {
                $videos[] = [
                    'url' => $media['url'] ?? null,
                    'thumbnail' => $media['posterUrl'] ?? null, // Often posterUrl
                    'type' => 'trailer',
                ];
            } else {
                $images[] = [
                    'url' => $media['url'] ?? null,
                    'type' => $role === 'MASTER' ? 'cover' : 'screenshot',
                ];
            }
        }

        // If no explicit cover found, grab standard image
        $cover = null;
        foreach ($images as $img) {
            if (($img['type'] ?? '') === 'cover') {
                $cover = $img['url'];
                break;
            }
        }
        if (! $cover && ! empty($images)) {
            $cover = $images[0]['url'];
        }

        return [
            'images' => array_filter($images, fn ($img) => ! empty($img['url'])),
            'videos' => array_filter($videos, fn ($vid) => ! empty($vid['url'])),
            'cover_image' => $cover,
            'screenshots' => array_values(array_filter($images, fn ($img) => ($img['type'] ?? '') === 'screenshot')),
        ];
    }

    /**
     * Extract metadata from GraphQL response.
     */
    private function extractMetadata(array $data): array
    {
        return [
            'title' => $data['name'] ?? null,
            'description' => $data['longDescription'] ?? null, // check if shortDescription exists
            'short_description' => $data['shortDescription'] ?? null,
            'publisher' => $data['publisherName'] ?? null, // often top level
            'release_date' => $data['releaseDate'] ?? null,
            'genres' => array_column($data['genres'] ?? [], 'name'), // usually array of objects
            'platforms' => array_column($data['platforms'] ?? [], 'name') ?? ['PS4', 'PS5'],
        ];
    }

    /**
     * Search using search endpoint
     */
    public function search(string $query, string $country = 'US', string $language = 'en'): array
    {
        // Not implemented in this wrapper for now, returning empty
        return [];
    }

    public function resolveProductId(string $title, string $country = 'US'): ?string
    {
        // 1. Try to find the product ID via web search (DuckDuckGo fallback)
        // This is necessary because the official search API is not easily accessible.
        try {
            return $this->searchViaDuckDuckGo($title, $country);
        } catch (\Exception $e) {
            Log::warning("PlayStationStoreService: Search failed for '{$title}'", ['error' => $e->getMessage()]);

            return null;
        }
    }

    /**
     * Search for a game on PlayStation Store using DuckDuckGo.
     * Parses the first result to extract the Product ID.
     */
    private function searchViaDuckDuckGo(string $query, string $country): ?string
    {
        $country = strtolower($country);
        $searchTerm = urlencode("site:store.playstation.com/{$country}/product \"{$query}\"");
        $url = "https://html.duckduckgo.com/html/?q={$searchTerm}";

        $response = \Illuminate\Support\Facades\Http::timeout(10)->withHeaders([
            'User-Agent' => 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
        ])->get($url);

        if ($response->successful()) {
            $html = $response->body();
            $pattern = '/store\.playstation\.com\/[a-z]{2}-[a-z]{2}\/product\/([A-Za-z0-9_\-]+)/';
            if (preg_match($pattern, $html, $matches)) {
                Log::info('PS Search (DDG) Found ID: '.$matches[1]);

                return $matches[1];
            }
        }

        // Fallback to Bing
        Log::info('PS Search (DDG) failed. Trying Bing...');
        $bingUrl = 'https://www.bing.com/search?q='.urlencode("site:store.playstation.com/{$country}/product \"{$query}\"");
        $response = \Illuminate\Support\Facades\Http::timeout(10)->withHeaders([
            'User-Agent' => 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
        ])->get($bingUrl);

        if ($response->successful()) {
            $html = $response->body();
            // Bing results often encoded or slightly different. Let's look for the pattern.
            // Matches product/UP... or product/PPSA...
            $pattern = '/store\.playstation\.com\/[a-z]{2}-[a-z]{2}\/product\/([A-Za-z0-9_\-]+)/';
            if (preg_match($pattern, $html, $matches)) {
                Log::info('PS Search (Bing) Found ID: '.$matches[1]);

                return $matches[1];
            }
        }

        Log::warning("PS Search failed for: '{$query}' (DDG & Bing)");

        return null;
    }

    public function getPrice(string $productId, string $country = 'US', string $language = 'en'): ?array
    {
        $fullDetails = $this->getFullDetails($productId, $country, $language);

        return $fullDetails['price'] ?? null;
    }

    private function getCurrencyForCountry(string $country): string
    {
        return match (strtoupper($country)) {
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
