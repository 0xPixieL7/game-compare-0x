<?php

declare(strict_types=1);

namespace App\Services\Price\Xbox;

use Illuminate\Support\Facades\Http;
use Illuminate\Support\Facades\Log;

final class XboxStoreService
{
    private const BASE_URL = 'https://displaycatalog.mp.microsoft.com/v7.0/products';

    private const AUTOSUGGEST_URL = 'https://displaycatalog.mp.microsoft.com/v7.0/productFamilies/autosuggest';

    /**
     * Search for a game by title and return potential Product IDs.
     */
    public function search(string $query, string $market = 'US', string $language = 'en-US'): array
    {
        try {
            $resp = Http::get(self::AUTOSUGGEST_URL, [
                'query' => $query,
                'market' => strtoupper($market),
                'languages' => $language,
                'productFamilyNames' => 'Games',
            ]);

            if (! $resp->successful()) {
                return [];
            }

            $data = $resp->json();
            $results = [];

            foreach ($data['Results'] ?? [] as $family) {
                foreach ($family['Products'] ?? [] as $product) {
                    $results[] = [
                        'id' => $product['ProductId'],
                        'title' => $product['Title'],
                        'type' => $product['Type'] ?? null,
                    ];
                }
            }

            return $results;
        } catch (\Throwable $e) {
            Log::debug('XboxStoreService: search failed', ['query' => $query, 'error' => $e->getMessage()]);

            return [];
        }
    }

    /**
     * Resolve a title to a single most likely Product ID.
     */
    public function resolveProductId(string $title, string $market = 'US'): ?string
    {
        $results = $this->search($title, $market);

        if (empty($results)) {
            return null;
        }

        // Clean titles for comparison
        $cleanSearch = strtolower(preg_replace('/[^a-z0-9]/', '', $title));

        foreach ($results as $result) {
            $cleanResult = strtolower(preg_replace('/[^a-z0-9]/', '', $result['title']));
            if ($cleanResult === $cleanSearch) {
                return (string) $result['id'];
            }
        }

        // Fallback to first result if it's reasonably similar
        return (string) $results[0]['id'];
    }

    /**
     * Get full details including price, media, and metadata.
     */
    public function getFullDetails(string $bigId, string $market = 'US', string $language = 'en-US'): ?array
    {
        $bigId = strtoupper(trim($bigId));
        if ($bigId === '') {
            return null;
        }

        try {
            $timeout = (int) (config('services.xbox.timeout', 10) ?? 10);

            $resp = Http::timeout($timeout)
                ->withHeaders([
                    'User-Agent' => 'game-compare/1.0',
                ])
                ->get(self::BASE_URL, [
                    'bigIds' => $bigId,
                    'market' => strtoupper($market),
                    'languages' => $language,
                ]);

            if (! $resp->successful()) {
                return null;
            }

            $data = $resp->json();
            $products = is_array($data) ? ($data['Products'] ?? null) : null;
            if (! is_array($products) || $products === []) {
                return null;
            }

            $product = $products[0];
            if (! is_array($product)) {
                return null;
            }

            return [
                'price' => $this->extractPrice($product, $market, $language),
                'media' => $this->extractMedia($product),
                'metadata' => $this->extractMetadata($product),
            ];
        } catch (\Throwable $e) {
            Log::debug('XboxStoreService: full details lookup failed', [
                'big_id' => $bigId,
                'market' => $market,
                'error' => $e->getMessage(),
            ]);

            return null;
        }
    }

    /**
     * Extract price data from Xbox response.
     */
    private function extractPrice(array $product, string $market, string $language): ?array
    {
        $skuAvail = $product['DisplaySkuAvailabilities'][0] ?? null;
        $availability = is_array($skuAvail)
            ? ($skuAvail['Availabilities'][0] ?? null)
            : null;
        $price = is_array($availability)
            ? ($availability['OrderManagementData']['Price'] ?? null)
            : null;

        if (! is_array($price)) {
            return null;
        }

        $currency = (string) ($price['CurrencyCode'] ?? '');
        $list = $price['ListPrice'] ?? null;
        $msrp = $price['MSRP'] ?? null;

        if ($currency === '' || ! is_numeric($list)) {
            return null;
        }

        $minor = $this->toMinor((float) $list, $currency);

        return [
            'currency' => $currency,
            'amount_minor' => $minor,
            'list_price' => (float) $list,
            'msrp' => is_numeric($msrp) ? (float) $msrp : null,
            'market' => strtoupper($market),
            'language' => $language,
        ];
    }

    /**
     * Extract media (images, videos) from Xbox response.
     */
    private function extractMedia(array $product): array
    {
        $images = [];
        $videos = [];

        // Extract images from LocalizedProperties
        $localizedProps = $product['LocalizedProperties'][0] ?? null;
        if (is_array($localizedProps)) {
            // Product images (box art, screenshots)
            foreach ($localizedProps['Images'] ?? [] as $image) {
                $purpose = $image['ImagePurpose'] ?? '';
                $uri = $image['Uri'] ?? null;

                if (! $uri) {
                    continue;
                }

                $images[] = [
                    'url' => $uri,
                    'purpose' => $purpose, // e.g., "BoxArt", "Screenshot", "Hero", "Poster"
                    'width' => $image['Width'] ?? null,
                    'height' => $image['Height'] ?? null,
                ];
            }

            // Extract videos/trailers
            foreach ($localizedProps['Videos'] ?? [] as $video) {
                $videoUri = $video['Uri'] ?? null;
                $videoTitle = $video['Title'] ?? 'Trailer';
                $thumbnail = $video['VideoPosterImage']['Uri'] ?? null;

                if (! $videoUri) {
                    continue;
                }

                $videos[] = [
                    'url' => $videoUri,
                    'title' => $videoTitle,
                    'thumbnail' => $thumbnail,
                    'duration' => $video['VideoDuration'] ?? null,
                ];
            }
        }

        return [
            'images' => $images,
            'videos' => $videos,
            'hero_image' => $this->findImageByPurpose($images, 'Hero'),
            'box_art' => $this->findImageByPurpose($images, 'BoxArt'),
            'screenshots' => $this->filterImagesByPurpose($images, 'Screenshot'),
        ];
    }

    /**
     * Extract metadata from Xbox response.
     */
    private function extractMetadata(array $product): array
    {
        $localizedProps = $product['LocalizedProperties'][0] ?? null;

        return [
            'title' => $localizedProps['ProductTitle'] ?? null,
            'description' => $localizedProps['ProductDescription'] ?? null,
            'short_description' => $localizedProps['ShortDescription'] ?? null,
            'publisher' => $localizedProps['PublisherName'] ?? null,
            'developer' => $localizedProps['DeveloperName'] ?? null,
            'release_date' => $product['MarketProperties'][0]['OriginalReleaseDate'] ?? null,
        ];
    }

    private function findImageByPurpose(array $images, string $purpose): ?string
    {
        foreach ($images as $image) {
            if (($image['purpose'] ?? '') === $purpose) {
                return $image['url'];
            }
        }

        return null;
    }

    private function filterImagesByPurpose(array $images, string $purpose): array
    {
        return array_filter($images, fn ($img) => ($img['purpose'] ?? '') === $purpose);
    }

    /**
     * Query Microsoft Store DisplayCatalog by product BigId (legacy price-only method).
     */
    public function getPrice(string $bigId, string $market = 'US', string $language = 'en-US'): ?array
    {
        $fullDetails = $this->getFullDetails($bigId, $market, $language);

        return $fullDetails['price'] ?? null;
    }

    private function toMinor(float $amount, string $currency): int
    {
        $currency = strtoupper($currency);

        $zeroDecimal = ['JPY', 'KRW'];
        if (in_array($currency, $zeroDecimal, true)) {
            return (int) round($amount);
        }

        return (int) round($amount * 100);
    }
}
