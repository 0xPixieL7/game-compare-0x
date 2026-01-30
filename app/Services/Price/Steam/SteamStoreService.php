<?php

namespace App\Services\Price\Steam;

use Illuminate\Http\Client\PendingRequest;
use Illuminate\Support\Facades\Http;
use Illuminate\Support\Facades\Log;
use Illuminate\Support\Facades\Redis;

class SteamStoreService
{
    private static int $blockedUntil = 0;

    private static int $consecutiveForbidden = 0;

    /**
     * Fetch full app details including price, media, and metadata.
     *
     * Returns combined data structure:
     * [
     *     'price' => ['amount_minor' => int, 'currency' => string] | null,
     *     'media' => [
     *         'header_image' => string | null,
     *         'screenshots' => array,
     *         'movies' => array,
     *         'background' => string | null,
     *     ],
     *     'metadata' => [
     *         'name' => string | null,
     *         'short_description' => string | null,
     *         'developers' => array,
     *         'publishers' => array,
     *         'genres' => array,
     *         'release_date' => string | null,
     *     ],
     * ]
     */
    public function getFullDetails(string $appId, string $country = 'US', ?string $language = null): ?array
    {
        if ($this->isBlocked()) {
            return null;
        }

        $apiUrl = "https://store.steampowered.com/api/appdetails?appids={$appId}&cc={$country}";

        if ($language) {
            $apiUrl .= '&l='.$this->mapLanguage($language);
        }

        try {
            $response = $this->request()->get($apiUrl);

            if ($response->status() === 403) {
                $this->registerForbidden();
                Log::warning("SteamStoreService: API blocked by Akamai/CDN (403 Forbidden) for App ID {$appId} in {$country}. Service will pause for 15 minutes.", [
                    'status' => $response->status(),
                    'blocked_until' => date('Y-m-d H:i:s', self::$blockedUntil),
                    'body_preview' => substr((string) $response->body(), 0, 200),
                ]);

                return null;
            }

            if ($response->failed()) {
                Log::error("SteamStoreService: Full details request failed for App ID {$appId} in {$country}", [
                    'status' => $response->status(),
                ]);

                return null;
            }

            $this->resetForbidden();

            $data = $response->json();

            // Log full details for debugging
            Log::debug("SteamStoreService: Details for App ID {$appId} in {$country}: ".json_encode($data));

            if (empty($data[$appId]['success'])) {
                return null;
            }

            $gameData = $data[$appId]['data'] ?? [];

            return [
                'price' => $this->extractPrice($gameData, $country),
                'media' => $this->extractMedia($gameData),
                'metadata' => $this->extractMetadata($gameData),
            ];
        } catch (\Exception $e) {
            Log::error("SteamStoreService: Full details exception for App ID {$appId}: ".$e->getMessage());

            return null;
        }
    }

    /**
     * Extract price data from Steam response.
     */
    public function extractPrice(array $gameData, string $country): ?array
    {
        $priceOverview = $gameData['price_overview'] ?? null;

        if ($priceOverview) {
            return [
                'amount_minor' => (int) $priceOverview['final'],
                'currency' => $priceOverview['currency'],
                'discount_percent' => $priceOverview['discount_percent'] ?? 0,
                'initial_amount_minor' => $priceOverview['initial'] ?? null,
            ];
        }

        // Check if it's free
        if (! empty($gameData['is_free'])) {
            return [
                'amount_minor' => 0,
                'currency' => $this->getCurrencyForCountry($country),
                'discount_percent' => 0,
                'initial_amount_minor' => 0,
            ];
        }

        return null;
    }

    /**
     * Extract media URLs from Steam response.
     */
    public function extractMedia(array $gameData): array
    {
        $screenshots = [];
        foreach ($gameData['screenshots'] ?? [] as $screenshot) {
            $screenshots[] = [
                'id' => $screenshot['id'] ?? null,
                'thumbnail' => $screenshot['path_thumbnail'] ?? null,
                'full' => $screenshot['path_full'] ?? null,
            ];
        }

        $movies = [];
        foreach ($gameData['movies'] ?? [] as $movie) {
            $movies[] = [
                'id' => $movie['id'] ?? null,
                'name' => $movie['name'] ?? null,
                'thumbnail' => $movie['thumbnail'] ?? null,
                'webm_480' => $movie['webm']['480'] ?? null,
                'webm_max' => $movie['webm']['max'] ?? null,
                'mp4_480' => $movie['mp4']['480'] ?? null,
                'mp4_max' => $movie['mp4']['max'] ?? null,
                // Add HLS support for modern Steam trailers
                'hls_max' => $movie['hls_h264'] ?? null,
            ];
        }

        return [
            'header_image' => $gameData['header_image'] ?? null,
            'screenshots' => $screenshots,
            'movies' => $movies,
            'background' => $gameData['background'] ?? null,
            'background_raw' => $gameData['background_raw'] ?? null,
            'capsule_image' => $gameData['capsule_image'] ?? null,
            'capsule_imagev5' => $gameData['capsule_imagev5'] ?? null,
        ];
    }

    /**
     * Extract metadata from Steam response.
     */
    public function extractMetadata(array $gameData): array
    {
        $genres = [];
        foreach ($gameData['genres'] ?? [] as $genre) {
            $genres[] = $genre['description'] ?? null;
        }

        return [
            'name' => $gameData['name'] ?? null,
            'short_description' => $gameData['short_description'] ?? null,
            'developers' => $gameData['developers'] ?? [],
            'publishers' => $gameData['publishers'] ?? [],
            'genres' => array_filter($genres),
            'release_date' => $gameData['release_date']['date'] ?? null,
            'metacritic_score' => $gameData['metacritic']['score'] ?? null,
            'metacritic_url' => $gameData['metacritic']['url'] ?? null,
        ];
    }

    /**
     * Fetch price for a given Steam App ID (legacy method for backward compatibility).
     */
    public function getPrice(string $appId, string $country = 'US', ?string $language = null): ?array
    {
        if ($this->isBlocked()) {
            return null;
        }

        $apiUrl = "https://store.steampowered.com/api/appdetails?appids={$appId}&cc={$country}";

        if ($language) {
            $apiUrl .= '&l='.$this->mapLanguage($language);
        }

        try {
            $response = $this->request()->get($apiUrl);

            if ($response->status() === 403) {
                $this->registerForbidden();
                Log::warning("SteamStoreService: API blocked for App ID {$appId} in {$country}", [
                    'status' => $response->status(),
                ]);

                return null;
            }

            if ($response->failed()) {
                Log::error("SteamStoreService: API request failed for App ID {$appId} in {$country}", [
                    'status' => $response->status(),
                ]);

                return null;
            }

            $this->resetForbidden();

            $data = $response->json();

            // Log full price response for debugging
            Log::debug("SteamStoreService: Price for App ID {$appId} in {$country}: ".json_encode($data));

            if (empty($data[$appId]['success'])) {
                return null;
            }

            $gameData = $data[$appId]['data'] ?? [];
            $priceOverview = $gameData['price_overview'] ?? null;

            if (! $priceOverview) {
                // Check if it's free
                if (! empty($gameData['is_free'])) {
                    return [
                        'amount_minor' => 0,
                        'currency' => $this->getCurrencyForCountry($country),
                    ];
                }

                return null;
            }

            return [
                'amount_minor' => (int) $priceOverview['final'],
                'currency' => $priceOverview['currency'],
            ];
        } catch (\Exception $e) {
            Log::error("SteamStoreService: Exception for App ID {$appId}: ".$e->getMessage());

            return null;
        }
    }

    /**
     * Batch fetch prices for multiple Steam App IDs in one request.
     *
     * @param  array<int, int|string>  $appIds
     * @return array<int, array{amount_minor:int,currency:string,discount_percent?:int,initial_amount_minor?:int,url?:string,product_id?:string}|null>
     */
    public function getPricesForAppIds(array $appIds, string $country = 'US', ?string $language = null): array
    {
        $out = [];
        $ids = array_values(array_unique(array_map(static fn ($id): string => (string) $id, $appIds)));

        if ($ids === []) {
            return $out;
        }

        if ($this->isBlocked()) {
            foreach ($ids as $id) {
                $out[(int) $id] = null;
            }

            return $out;
        }

        $apiUrl = 'https://store.steampowered.com/api/appdetails?appids='.implode(',', $ids).'&cc='.$country;

        if ($language) {
            $apiUrl .= '&l='.$this->mapLanguage($language);
        }

        try {
            $response = $this->request()->get($apiUrl);

            if ($response->status() === 403) {
                $this->registerForbidden();
                Log::warning('SteamStoreService: Batch request blocked (403 Forbidden)', [
                    'country' => $country,
                    'count' => count($ids),
                ]);

                foreach ($ids as $id) {
                    $out[(int) $id] = null;
                }

                return $out;
            }

            if ($response->failed()) {
                Log::error('SteamStoreService: Batch request failed', [
                    'status' => $response->status(),
                    'country' => $country,
                    'count' => count($ids),
                ]);

                foreach ($ids as $id) {
                    $out[(int) $id] = null;
                }

                return $out;
            }

            $this->resetForbidden();
            $data = $response->json();

            foreach ($ids as $id) {
                $intId = (int) $id;
                if (! isset($data[$id]) && isset($data[$intId])) {
                    $idKey = (string) $intId;
                } else {
                    $idKey = (string) $id;
                }

                if (empty($data[$idKey]['success'])) {
                    $out[$intId] = null;

                    continue;
                }

                $gameData = $data[$idKey]['data'] ?? [];
                $price = $this->extractPrice($gameData, $country);
                if ($price === null) {
                    $out[$intId] = null;

                    continue;
                }

                $out[$intId] = array_merge($price, [
                    'product_id' => (string) $intId,
                    'url' => 'https://store.steampowered.com/app/'.$intId,
                ]);
            }

            return $out;
        } catch (\Exception $e) {
            Log::error('SteamStoreService: Batch exception: '.$e->getMessage());
            foreach ($ids as $id) {
                $out[(int) $id] = null;
            }

            return $out;
        }
    }

    /**
     * Map common locale/language codes to Steam language strings.
     */
    private function mapLanguage(string $language): string
    {
        $language = strtolower($language);

        // Handle full locales like en-US, ja-JP
        if (str_contains($language, '-') || str_contains($language, '_')) {
            $language = explode('-', str_replace('_', '-', $language))[0];
        }

        return match ($language) {
            'en', 'english' => 'english',
            'dk', 'danish' => 'danish',
            'ru', 'russian' => 'russian',
            'kr', 'ko', 'korean' => 'korean',
            'jp', 'ja', 'japanese' => 'japanese',
            'de', 'german' => 'german',
            'fr', 'french' => 'french',
            'es', 'spanish' => 'spanish',
            'it', 'italian' => 'italian',
            'pt', 'portuguese' => 'portuguese',
            'br', 'brazilian' => 'brazilian',
            'cn', 'zh-cn', 'schinese' => 'schinese',
            'tw', 'zh-tw', 'tchinese' => 'tchinese',
            'tr', 'turkish' => 'turkish',
            'pl', 'polish' => 'polish',
            'se', 'sv', 'swedish' => 'swedish',
            'no', 'nb', 'norwegian' => 'norwegian',
            'fi', 'finnish' => 'finnish',
            default => $language,
        };
    }

    /**
     * Helper to guess currency if price_overview is missing but game is free.
     */
    private function getCurrencyForCountry(string $country): string
    {
        return match (strtoupper($country)) {
            'GB' => 'GBP',
            'JP' => 'JPY',
            'KR' => 'KRW',
            'BR' => 'BRL',
            'CA' => 'CAD',
            'AU' => 'AUD',
            'NZ' => 'NZD',
            'RU' => 'RUB',
            'IN' => 'INR',
            'CN' => 'CNY',
            'UA' => 'UAH',
            'PL' => 'PLN',
            'SE' => 'SEK',
            'NO' => 'NOK',
            'DK' => 'DKK',
            'CH' => 'CHF',
            'ZA' => 'ZAR',
            'MX' => 'MXN',
            'CL' => 'CLP',
            'CO' => 'COP',
            'PE' => 'PEN',
            'TH' => 'THB',
            'PH' => 'PHP',
            'MY' => 'MYR',
            'VN' => 'VND',
            'ID' => 'IDR',
            'SG' => 'SGD',
            'HK' => 'HKD',
            'TW' => 'TWD',
            'KZ' => 'KZT',
            'DE', 'FR', 'ES', 'IT', 'NL', 'AT', 'BE', 'PT', 'IE', 'FI' => 'EUR',
            default => 'USD',
        };
    }

    /**
     * Search for a game on Steam store and return the best match App ID.
     */
    public function search(string $term): ?int
    {
        if ($this->isBlocked()) {
            return null;
        }

        // 1. Try Local Lookup (Fastest)
        $localId = $this->searchLocal($term);
        if ($localId) {
            return $localId;
        }

        // 2. Fallback to API (Slower, Rate Limited)
        $apiUrl = 'https://store.steampowered.com/api/storesearch/?term='.urlencode($term).'&l=english&cc=US';

        try {
            $response = $this->request()->get($apiUrl);

            if ($response->status() === 403) {
                $this->registerForbidden();
                Log::warning("SteamStoreService: Search blocked for term '{$term}'", [
                    'status' => $response->status(),
                ]);

                return null;
            }

            if ($response->failed()) {
                Log::warning("SteamStoreService: Search failed for term '{$term}'", [
                    'status' => $response->status(),
                ]);

                return null;
            }

            $this->resetForbidden();

            $data = $response->json();
            $items = $data['items'] ?? [];

            if (empty($items)) {
                return null;
            }

            // Return first result's ID
            return (int) $items[0]['id'];
        } catch (\Exception $e) {
            Log::error("SteamStoreService: Search exception for '{$term}': ".$e->getMessage());

            return null;
        }
    }

    /**
     * Search local JSON file for Steam ID.
     * Uses streaming read to avoid memory overhead.
     */
    public function searchLocal(string $term): ?int
    {
        $path = base_path('steam_apps_pretty.json');

        if (! file_exists($path)) {
            return null;
        }

        $handle = fopen($path, 'r');
        if (! $handle) {
            return null;
        }

        $prevLine = '';
        $normalizedTerm = strtolower($term);

        try {
            while (($line = fgets($handle)) !== false) {
                // Check if line has "name" match
                if (stripos($line, '"name":') !== false) {
                    // Extract name value
                    if (preg_match('/"name":\s*"(.*)"/', $line, $matches)) {
                        $name = $matches[1];

                        // Exact match (case-insensitive)
                        if (strtolower($name) === $normalizedTerm) {
                            // Extract ID from previous line: "appid": 12345,
                            if (preg_match('/"appid":\s*(\d+)/', $prevLine, $idMatches)) {
                                return (int) $idMatches[1];
                            }
                        }
                    }
                }
                $prevLine = $line;
            }
        } finally {
            fclose($handle);
        }

        return null;
    }

    private function request(): PendingRequest
    {
        $this->throttle();

        return Http::withHeaders([
            'Accept' => 'application/json',
            'Accept-Language' => 'en-US,en;q=0.9',
            'User-Agent' => 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0 Safari/537.36',
        ])
            ->connectTimeout(5)
            ->timeout(10)
            ->retry([200, 500, 1000], throw: false);
    }

    private function throttle(): void
    {
        // Global cross-process throttle (queue workers) - hard cap 4 req/sec.
        try {
            Redis::throttle('steam:api')
                ->allow(4)
                ->every(1)
                ->block(10)
                ->then(static function (): void {});

            return;
        } catch (\Throwable) {
            // Fallback: per-process throttle.
        }

        static $lastRequestAt = 0;
        $minGapUs = 250000; // 4 req/sec per process (fallback only)
        $now = (int) (microtime(true) * 1000000);
        $delta = $now - $lastRequestAt;

        if ($lastRequestAt > 0 && $delta < $minGapUs) {
            usleep($minGapUs - $delta);
        }

        $lastRequestAt = (int) (microtime(true) * 1000000);
    }

    private function isBlocked(): bool
    {
        return self::$blockedUntil > time();
    }

    private function registerForbidden(): void
    {
        self::$consecutiveForbidden++;

        if (self::$consecutiveForbidden >= 1) {
            // Block for 30 minutes (increased from 15) to allow Akamai rate limits to reset
            self::$blockedUntil = time() + 1800;
        }
    }

    private function resetForbidden(): void
    {
        self::$consecutiveForbidden = 0;
        self::$blockedUntil = 0;
    }
}
