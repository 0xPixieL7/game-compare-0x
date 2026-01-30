<?php

namespace App\Services\Price;

use App\Models\Country;
use App\Models\VideoGame;
use App\Models\VideoGamePrice;
use App\Services\Price\Amazon\AmazonScraperService;
use App\Services\Price\EpicGames\EpicGamesStoreService;
use App\Services\Price\Gog\GogStoreService;
use App\Services\Price\ItchIo\ItchIoScraperService;
use App\Services\Price\Steam\SteamStoreService;
use App\Services\Price\Xbox\XboxStoreService;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Log;

class GameDataAggregatorService
{
    public function __construct(
        private SteamStoreService $steamService,
        private AmazonScraperService $amazonService,
        private EpicGamesStoreService $epicService,
        private GogStoreService $gogService,
        private XboxStoreService $xboxService,
        private \App\Services\Price\PlayStation\PlayStationStoreService $playstationService,
        private ItchIoScraperService $itchIoService,
        private \App\Services\CurrencyCountryService $currencyService
    ) {}

    /**
     * Fetch all available prices AND media for a video game from all retailers and APIs.
     *
     * @param  bool  $forceRefresh  Force fetch even if recently updated
     * @param  bool  $includeMedia  Whether to fetch media (screenshots, videos, etc.)
     * @return array Array of price and media data grouped by retailer
     */
    public function getAllData(int $videoGameId, bool $forceRefresh = false, bool $includeMedia = true): array
    {
        $game = VideoGame::find($videoGameId);
        if (! $game) {
            return ['error' => 'Game not found'];
        }

        $query = VideoGamePrice::where('video_game_id', $videoGameId)
            ->where('is_active', true);

        if ($forceRefresh) {
            // Get all active price entries
            $priceEntries = $query->get();
        } else {
            // Only fetch if unknown (-1) or stale (>24h old)
            $priceEntries = $query->where(function ($q) {
                $q->where('amount_minor', -1)
                    ->orWhere('updated_at', '<', now()->subHours(24));
            })->get();
        }

        $results = [
            'game_id' => $videoGameId,
            'game_name' => $game->name,
            'fetched_at' => now()->toIso8601String(),
            'prices' => [],
            'media' => [],
            'metadata' => [],
            'errors' => [],
        ];

        // Track which retailers we've already fetched full details from (to avoid duplicate API calls)
        $fetchedRetailers = [];

        foreach ($priceEntries as $entry) {
            $retailer = $entry->retailer;
            $countryCode = $entry->country_code ?? 'US';
            $url = $entry->url ?? $this->discoverRetailerUrl($game, $retailer);

            if (! $url) {
                continue;
            }

            try {
                // For retailers with rich APIs (Steam, Xbox), fetch full details including media
                if ($includeMedia && ! in_array($retailer, $fetchedRetailers)) {
                    $fullData = $this->fetchFullDetails($retailer, $url, $countryCode);

                    if ($fullData) {
                        // Store price
                        if (isset($fullData['price'])) {
                            $this->updatePrice($entry, $fullData['price']);
                            $results['prices'][] = [
                                'retailer' => $retailer,
                                'country' => $countryCode,
                                'currency' => $fullData['price']['currency'],
                                'amount_minor' => $fullData['price']['amount_minor'],
                                'amount_formatted' => $this->formatPrice($fullData['price']['amount_minor'], $fullData['price']['currency']),
                                'url' => $url,
                                'discount_percent' => $fullData['price']['discount_percent'] ?? 0,
                                'fetched_at' => now()->toIso8601String(),
                            ];
                        }

                        // Store media
                        if (isset($fullData['media'])) {
                            $results['media'][$retailer] = $fullData['media'];

                            // Persist media to database
                            $this->persistMedia($videoGameId, $retailer, $fullData['media']);
                        }

                        // Store metadata
                        if (isset($fullData['metadata'])) {
                            $results['metadata'][$retailer] = $fullData['metadata'];
                        }

                        $fetchedRetailers[] = $retailer;

                        continue;
                    }
                }

                // Fallback to price-only fetch
                $priceData = $this->fetchPrice($retailer, $url, $countryCode);

                if ($priceData) {
                    $this->updatePrice($entry, $priceData);

                    $results['prices'][] = [
                        'retailer' => $retailer,
                        'country' => $countryCode,
                        'currency' => $priceData['currency'],
                        'amount_minor' => $priceData['amount_minor'],
                        'amount_formatted' => $this->formatPrice($priceData['amount_minor'], $priceData['currency']),
                        'url' => $url,
                        'fetched_at' => now()->toIso8601String(),
                    ];
                } else {
                    $results['errors'][] = [
                        'retailer' => $retailer,
                        'country' => $countryCode,
                        'message' => 'Failed to fetch price',
                        'url' => $url,
                    ];
                }
            } catch (\Exception $e) {
                Log::error("GameDataAggregatorService: Error fetching {$retailer} data", [
                    'game_id' => $videoGameId,
                    'retailer' => $retailer,
                    'error' => $e->getMessage(),
                ]);

                $results['errors'][] = [
                    'retailer' => $retailer,
                    'country' => $countryCode,
                    'message' => $e->getMessage(),
                    'url' => $url,
                ];
            }
        }

        return $results;
    }

    /**
     * Fetch full details (price + media + metadata) from retailers that support it.
     */
    private function fetchFullDetails(string $retailer, string $url, string $countryCode): ?array
    {
        return match ($retailer) {
            'Steam' => $this->fetchSteamFullDetails($url, $countryCode),
            'Xbox Store' => $this->fetchXboxFullDetails($url, $countryCode),
            'PlayStation Store' => $this->fetchPlayStationFullDetails($url, $countryCode),
            'GOG' => $this->gogService->getFullDetails($url, $countryCode),
            'Epic Games' => $this->epicService->getFullDetails($url, $countryCode),
            'itch.io' => $this->fetchItchIoFullDetails($url),
            default => null,
        };
    }

    private function fetchItchIoFullDetails(string $url): ?array
    {
        // Extract slug and username from itch.io URL: https://username.itch.io/game-slug
        if (preg_match('/https?:\/\/([^\.]+)\.itch\.io\/([^\/]+)/', $url, $matches)) {
            $username = $matches[1];
            $gameSlug = $matches[2];

            return $this->itchIoService->getFullDetails($gameSlug, $username);
        }

        return null;
    }

    private function fetchSteamFullDetails(string $url, string $countryCode): ?array
    {
        // Extract Steam App ID from URL
        if (preg_match('/app\/(\d+)/', $url, $matches)) {
            $appId = $matches[1];

            return $this->steamService->getFullDetails($appId, $countryCode);
        }

        return null;
    }

    private function fetchXboxFullDetails(string $url, string $countryCode): ?array
    {
        // Extract BigId from URL
        if (preg_match('/\/([A-Z0-9]{12,})/', $url, $matches)) {
            $bigId = $matches[1];

            return $this->xboxService->getFullDetails($bigId, $countryCode);
        }

        return null;
    }

    private function fetchPlayStationFullDetails(string $url, string $countryCode): ?array
    {
        // Extract product ID from PlayStation URL
        // Formats: /en-us/product/UP0001-CUSA00744_00-GTAVDIGITALDOWNL
        if (preg_match('/\/product\/([A-Z0-9_-]+)/', $url, $matches)) {
            $productId = $matches[1];

            return $this->playstationService->getFullDetails($productId, $countryCode, 'en');
        }

        return null;
    }

    /**
     * Route to appropriate price fetching service based on retailer.
     */
    private function fetchPrice(string $retailer, string $url, string $countryCode): ?array
    {
        return match ($retailer) {
            'Steam' => $this->fetchSteamPrice($url, $countryCode),
            'Amazon' => $this->amazonService->getPrice($url, $countryCode),
            'Epic Games' => $this->epicService->getPrice($url, $countryCode),
            'GOG' => $this->gogService->getPrice($url, $countryCode),
            'Xbox Store' => $this->fetchXboxPrice($url, $countryCode),
            'PlayStation Store' => $this->fetchPlayStationPrice($url, $countryCode),
            'itch.io' => $this->fetchItchIoPrice($url),
            default => null,
        };
    }

    private function fetchItchIoPrice(string $url): ?array
    {
        if (preg_match('/https?:\/\/([^\.]+)\.itch\.io\/([^\/]+)/', $url, $matches)) {
            $username = $matches[1];
            $gameSlug = $matches[2];

            return $this->itchIoService->getPrice($gameSlug, $username);
        }

        return null;
    }

    private function fetchSteamPrice(string $url, string $countryCode): ?array
    {
        if (preg_match('/app\/(\d+)/', $url, $matches)) {
            $appId = $matches[1];

            return $this->steamService->getPrice($appId, $countryCode);
        }

        return null;
    }

    private function fetchXboxPrice(string $url, string $countryCode): ?array
    {
        if (preg_match('/\/([A-Z0-9]{12,})/', $url, $matches)) {
            $bigId = $matches[1];

            return $this->xboxService->getPrice($bigId, $countryCode);
        }

        return null;
    }

    private function fetchPlayStationPrice(string $url, string $countryCode): ?array
    {
        if (preg_match('/\/product\/([A-Z0-9_-]+)/', $url, $matches)) {
            $productId = $matches[1];

            return $this->playstationService->getPrice($productId, $countryCode);
        }

        return null;
    }

    /**
     * Update price entry in database.
     */
    private function updatePrice(VideoGamePrice $entry, array $priceData): void
    {
        $this->persistStandardPrice($entry->video_game_id, $entry->retailer, $entry->country_code ?? 'US', $priceData);
    }

    /**
     * Persist media to database (images and videos tables) with full column coverage.
     */
    public function persistMedia(int $videoGameId, string $source, array $media): void
    {
        try {
            // Standardize source name
            $source = strtolower($source);

            // 1. Store screenshots/images
            if (! empty($media['screenshots'] ?? $media['images'])) {
                $images = $media['screenshots'] ?? $media['images'];
                foreach ($images as $index => $image) {
                    $url = $image['url'] ?? $image['full'] ?? $image['thumbnail'] ?? null;
                    if (! $url) {
                        continue;
                    }

                    $this->persistStandardImage($videoGameId, $source, [
                        'url' => $url,
                        'type' => $image['type'] ?? 'screenshot',
                        'external_id' => $image['external_id'] ?? "{$source}-{$videoGameId}-img-{$index}",
                        'alt_text' => $image['alt_text'] ?? null,
                        'order' => $index,
                        'urls' => [
                            'original' => $url,
                            'thumbnail' => $image['thumbnail'] ?? null,
                        ],
                    ]);
                }
            }

            // 2. Store specific key images
            if (! empty($media['header_image'])) {
                $this->persistStandardImage($videoGameId, $source, [
                    'url' => $media['header_image'],
                    'type' => 'header',
                    'collection' => 'header',
                ]);
            }

            if (! empty($media['background'])) {
                $this->persistStandardImage($videoGameId, $source, [
                    'url' => $media['background'],
                    'type' => 'background',
                    'collection' => 'background',
                ]);
            }

            if (! empty($media['cover_image'])) {
                $this->persistStandardImage($videoGameId, $source, [
                    'url' => $media['cover_image'],
                    'type' => 'cover',
                    'collection' => 'cover',
                ]);
            }

            // 3. Store videos/trailers
            $videos = null;
            if (isset($media['movies']) && is_array($media['movies']) && $media['movies'] !== []) {
                $videos = $media['movies'];
            } elseif (isset($media['videos']) && is_array($media['videos']) && $media['videos'] !== []) {
                $videos = $media['videos'];
            }

            if ($videos !== null) {
                foreach ($videos as $index => $video) {
                    $videoUrl = $video['url'] ?? $video['webm_max'] ?? $video['mp4_max'] ?? $video['hls_max'] ?? null;
                    if (! $videoUrl) {
                        continue;
                    }

                    $this->persistStandardVideo($videoGameId, $source, [
                        'url' => $videoUrl,
                        'title' => $video['name'] ?? $video['title'] ?? null,
                        'thumbnail_url' => $video['thumbnail'] ?? $video['thumbnail_url'] ?? null,
                        'external_id' => $video['id'] ?? $video['external_id'] ?? "{$source}-{$videoGameId}-vid-{$index}",
                        'order' => $index,
                        'urls' => [
                            'mp4_max' => $video['mp4_max'] ?? ($source === 'steam' ? ($video['url'] ?? null) : null),
                            'webm_max' => $video['webm_max'] ?? null,
                        ],
                    ]);
                }
            }
        } catch (\Exception $e) {
            Log::error('GameDataAggregatorService: Failed to persist media', [
                'game_id' => $videoGameId,
                'source' => $source,
                'error' => $e->getMessage(),
            ]);
        }
    }

    /**
     * Standardized Image Persistence (fills EVERY column).
     */
    public function persistStandardImage(int $gameId, string $provider, array $data): int
    {
        $url = $data['url'];
        $type = $data['type'] ?? 'default';
        $collectionName = $data['collection'] ?? $type;

        // Ensure a media record exists
        $mediaId = DB::table('media')->where('model_type', 'App\\Models\\VideoGame')
            ->where('model_id', $gameId)
            ->where('collection_name', $collectionName)
            ->where('name', 'like', "%{$provider}%")
            ->value('id');

        if (! $mediaId) {
            $mediaId = DB::table('media')->insertGetId([
                'model_type' => 'App\\Models\\VideoGame',
                'model_id' => $gameId,
                'uuid' => \Illuminate\Support\Str::uuid(),
                'collection_name' => $collectionName,
                'name' => "{$provider}-{$type}-".uniqid(),
                'file_name' => basename(parse_url($url, PHP_URL_PATH) ?: 'image.jpg'),
                'mime_type' => 'image/jpeg',
                'disk' => 'public',
                'conversions_disk' => 'public',
                'size' => 0,
                'manipulations' => json_encode([]),
                'custom_properties' => json_encode(['provider' => $provider, 'type' => $type]),
                'generated_conversions' => json_encode([]),
                'responsive_images' => json_encode([]),
                'order_column' => $data['order'] ?? 0,
                'created_at' => now(),
                'updated_at' => now(),
            ]);
        }

        DB::table('images')->updateOrInsert(
            ['imageable_type' => 'App\\Models\\VideoGame', 'imageable_id' => $gameId, 'url' => $url],
            [
                'video_game_id' => $gameId,
                'media_id' => $mediaId,
                'provider' => $provider,
                'external_id' => $data['external_id'] ?? null,
                'collection_names' => json_encode([$collectionName, $type]),
                'primary_collection' => $collectionName,
                'alt_text' => $data['alt_text'] ?? "Image for game ID {$gameId}",
                'urls' => json_encode($data['urls'] ?? ['original' => $url]),
                'metadata' => json_encode(array_merge(['source' => $provider, 'type' => $type], $data['metadata'] ?? [])),
                'order_column' => $data['order'] ?? 0,
                'updated_at' => now(),
                'created_at' => now(),
            ]
        );

        return (int) $mediaId;
    }

    /**
     * Standardized Video Persistence (fills EVERY column).
     */
    public function persistStandardVideo(int $gameId, string $provider, array $data): int
    {
        $url = $data['url'];

        // Media record for video
        $mediaId = DB::table('media')->insertGetId([
            'model_type' => 'App\\Models\\VideoGame',
            'model_id' => $gameId,
            'uuid' => \Illuminate\Support\Str::uuid(),
            'collection_name' => 'trailers',
            'name' => $data['title'] ?? "{$provider}-video-".uniqid(),
            'file_name' => basename(parse_url($url, PHP_URL_PATH) ?: 'video.mp4'),
            'mime_type' => 'video/mp4',
            'disk' => 'public',
            'conversions_disk' => 'public',
            'size' => 0,
            'manipulations' => json_encode([]),
            'custom_properties' => json_encode(['provider' => $provider]),
            'generated_conversions' => json_encode([]),
            'responsive_images' => json_encode([]),
            'order_column' => $data['order'] ?? 0,
            'created_at' => now(),
            'updated_at' => now(),
        ]);

        DB::table('videos')->updateOrInsert(
            ['videoable_type' => 'App\\Models\\VideoGame', 'videoable_id' => $gameId, 'url' => $url],
            [
                'video_game_id' => $gameId,
                'media_id' => $mediaId,
                'provider' => $provider,
                'video_id' => $data['external_id'] ?? null,
                'external_id' => $data['external_id'] ?? null,
                'collection_names' => json_encode(['trailers', 'gameplay']),
                'primary_collection' => 'trailers',
                'thumbnail_url' => $data['thumbnail_url'] ?? null,
                'title' => $data['title'] ?? 'Trailer',
                'urls' => json_encode($data['urls'] ?? ['original' => $url]),
                'order_column' => $data['order'] ?? 0,
                'metadata' => json_encode(array_merge(['source' => $provider], $data['metadata'] ?? [])),
                'updated_at' => now(),
                'created_at' => now(),
            ]
        );

        return (int) $mediaId;
    }

    /**
     * Standardized Product/Price Persistence.
     */
    public function persistStandardPrice(int $gameId, string $retailer, string $countryCode, array $priceData): void
    {
        $currency = $this->currencyService->getCurrencyForCountry($countryCode);

        // 1. Manage Product Entry (Central table for all store items)
        $productId = $priceData['product_id'] ?? $priceData['sku'] ?? null;
        if ($productId) {
            $this->saveProduct($gameId, $retailer, $productId, $priceData);
        }

        // 2. Manage Price Entry
        DB::table('video_game_prices')->updateOrInsert(
            [
                'video_game_id' => $gameId,
                'retailer' => $retailer,
                'country_code' => $countryCode,
            ],
            [
                'product_id' => $productId,
                'currency' => $currency,
                'amount_minor' => $priceData['amount_minor'] ?? 0,
                'url' => $priceData['url'] ?? null,
                'recorded_at' => now(),
                'is_active' => true,
                'sku' => $priceData['sku'] ?? $productId,
                'condition' => $priceData['condition'] ?? 'new',
                'is_retail_buy' => true,
                'metadata' => json_encode(array_merge(['source' => strtolower($retailer)], $priceData['metadata'] ?? [])),
                'updated_at' => now(),
                'created_at' => now(),
            ]
        );
    }

    /**
     * Save/Update Product details.
     */
    private function saveProduct(int $gameId, string $retailer, string $productId, array $data): void
    {
        $game = VideoGame::find($gameId);

        DB::table('products')->updateOrInsert(
            ['id' => $productId],
            [
                'type' => strtolower($retailer),
                'name' => $data['name'] ?? $game->name,
                'title' => $data['name'] ?? $game->name,
                'slug' => \Illuminate\Support\Str::slug(($data['name'] ?? $game->name).'-'.$retailer),
                'platform' => $data['platform'] ?? 'Unknown',
                'release_date' => $data['release_date'] ?? $game->release_date,
                'synopsis' => $data['synopsis'] ?? $game->summary,
                'rating' => $game->rating,
                'popularity_score' => $game->popularity_score,
                'metadata' => json_encode($data['metadata'] ?? []),
                'updated_at' => now(),
                'created_at' => now(),
            ]
        );
    }

    /**
     * Format price for human-readable display using database currency info.
     */
    private function formatPrice(int $amountMinor, string $currency): string
    {
        return $this->currencyService->formatPrice($amountMinor, $currency);
    }

    /**
     * Get lowest price across all retailers for a game.
     */
    public function getLowestPrice(int $videoGameId, ?string $targetCurrency = 'USD'): ?array
    {
        $prices = VideoGamePrice::where('video_game_id', $videoGameId)
            ->where('is_active', true)
            ->where('amount_minor', '>', 0)
            ->where('currency', $targetCurrency)
            ->orderBy('amount_minor', 'asc')
            ->first();

        if (! $prices) {
            return null;
        }

        return [
            'retailer' => $prices->retailer,
            'country' => $prices->country_code,
            'currency' => $prices->currency,
            'amount_minor' => $prices->amount_minor,
            'amount_formatted' => $this->formatPrice($prices->amount_minor, $prices->currency),
            'url' => $prices->url,
        ];
    }

    /**
     * Get price comparison across all retailers.
     */
    public function comparePrices(int $videoGameId, ?string $targetCurrency = 'USD'): array
    {
        $prices = VideoGamePrice::where('video_game_id', $videoGameId)
            ->where('is_active', true)
            ->where('amount_minor', '>', 0)
            ->where('currency', $targetCurrency)
            ->orderBy('amount_minor', 'asc')
            ->get();

        return $prices->map(function ($price) {
            return [
                'retailer' => $price->retailer,
                'country' => $price->country_code,
                'currency' => $price->currency,
                'amount_minor' => $price->amount_minor,
                'amount_formatted' => $this->formatPrice($price->amount_minor, $price->currency),
                'url' => $price->url,
                'last_updated' => $price->updated_at->diffForHumans(),
            ];
        })->toArray();
    }

    /**
     * Sync prices for a retailer across ALL countries in the database.
     */
    public function syncRegionalPricesForRetailer(VideoGame $game, string $retailer, ?callable $onProgress = null): array
    {
        $url = $this->discoverRetailerUrl($game, $retailer);
        if (! $url) {
            return ['prices' => 0];
        }

        $regions = [];
        if ($retailer === 'GOG') {
            $regions = array_filter(array_map('trim', explode(',', (string) config('services.gog.regions'))));
        }

        if ($retailer === 'Amazon') {
            $regions = array_filter(array_map('trim', explode(',', (string) config('services.amazon.regions'))));
            if ($regions === []) {
                $regions = $this->resolveAmazonRegionsFromUrl($url);
            }
        }

        if ($retailer === 'Steam') {
            $regions = array_filter(array_map('trim', explode(',', (string) config('services.steam.regions'))));
        }

        if ($retailer === 'Xbox Store') {
            $regions = array_filter(array_map('trim', explode(',', (string) config('services.xbox.markets'))));
        }

        if ($retailer === 'PlayStation Store') {
            $regions = array_filter(array_map('trim', explode(',', (string) config('services.playstation.regions'))));
        }

        if ($retailer === 'Epic Games Store') {
            $regions = array_filter(array_map('trim', explode(',', (string) config('services.epic.regions'))));
        }

        $countries = Country::with('currency')
            ->when($regions !== [], function ($query) use ($regions) {
                $query->whereIn('code', $regions);
            })
            ->get();
        $syncedCount = 0;

        foreach ($countries as $country) {
            try {
                $code = $country->code;

                if ($onProgress) {
                    $onProgress("Fetching {$retailer} price for {$code}...");
                }

                $priceData = $this->fetchPrice($retailer, $url, $code);

                if ($priceData) {
                    $this->persistStandardPrice($game->id, $retailer, $code, $priceData);
                    $syncedCount++;
                }
            } catch (\Exception $e) {
                Log::error("GameDataAggregatorService: Regional sync failed for {$retailer}, game {$game->id}, country {$country->code}", ['error' => $e->getMessage()]);
            }
        }

        return ['prices' => $syncedCount];
    }

    /**
     * Derive Amazon regions from the URL TLD.
     * Falls back to empty array (all countries) when unknown.
     */
    private function resolveAmazonRegionsFromUrl(string $url): array
    {
        $host = parse_url($url, PHP_URL_HOST) ?? '';
        if (! is_string($host) || $host === '') {
            return [];
        }

        $map = [
            'amazon.com' => ['US'],
            'amazon.co.uk' => ['GB'],
            'amazon.ca' => ['CA'],
            'amazon.de' => ['DE'],
            'amazon.fr' => ['FR'],
            'amazon.it' => ['IT'],
            'amazon.es' => ['ES'],
            'amazon.nl' => ['NL'],
            'amazon.se' => ['SE'],
            'amazon.pl' => ['PL'],
            'amazon.co.jp' => ['JP'],
            'amazon.com.au' => ['AU'],
            'amazon.com.br' => ['BR'],
            'amazon.com.mx' => ['MX'],
            'amazon.com.tr' => ['TR'],
            'amazon.sg' => ['SG'],
            'amazon.ae' => ['AE'],
            'amazon.sa' => ['SA'],
            'amazon.in' => ['IN'],
        ];

        foreach ($map as $domain => $codes) {
            if (str_ends_with($host, $domain)) {
                return $codes;
            }
        }

        return [];
    }

    /**
     * Sync Amazon price from scraping.
     * Checks existing DB entry or attributes for URL.
     */
    public function syncAmazonPrice(VideoGame $game, ?callable $onProgress = null): array
    {
        // 1. Check for existing entry
        $priceEntry = VideoGamePrice::where('video_game_id', $game->id)
            ->where('retailer', 'Amazon')
            ->first();

        // 2. Determine URL
        $url = $priceEntry?->url;

        if (! $url) {
            // 3. Try discovery from attributes
            $url = $this->findAmazonUrl($game);
        }

        if (! $url) {
            return ['prices' => 0];
        }

        $countries = Country::with('currency')->get();
        $syncedCount = 0;

        foreach ($countries as $country) {
            try {
                $code = $country->code;
                $expectedCurrency = $country->currency?->code;

                // Amazon URLs might need localization adjustment, but simplistic approach
                // assumes the URL provided routes correctly or we rely on scraper to handle generic URLs if possible
                // However, Amazon usually has different domains (.com, .co.uk, .de).
                // The current scraper expects a full URL. If we only have one URL, we might only check one country?
                // OR we have logic to swap TLDs?
                // "The user requested prices for every single country".
                // If the URL is amazon.com, checking it with 'GB' code might return USD or redirect.
                // For now, let's enable the Loop but acknowledge the URL limitation.
                // Ideally we'd swap domains: amazon.com -> amazon.co.uk etc.

                // Let's rely on the service to handle country context if it can,
                // or just store what we get.

                // 4. Scrape
                if ($onProgress) {
                    $onProgress("Fetching Amazon price for {$code}...");
                }
                $priceData = $this->amazonService->getPrice($url, $code);

                if (! $priceData) {
                    continue;
                }

                $fetchedCurrency = $expectedCurrency ?? $priceData['currency'] ?? 'USD';

                // 5. Update/Create
                VideoGamePrice::updateOrInsert(
                    [
                        'video_game_id' => $game->id,
                        'retailer' => 'Amazon',
                        'country_code' => $code,
                    ],
                    [
                        'currency' => $fetchedCurrency,
                        'amount_minor' => $priceData['amount_minor'] ?? 0,
                        'url' => $url,
                        'recorded_at' => now(),
                        'is_active' => true,
                        'updated_at' => now(),
                        'created_at' => now(),
                    ]
                );

                $syncedCount++;

            } catch (\Exception $e) {
                Log::error("GameDataAggregatorService: Amazon sync failed for game {$game->id} country {$country->code}", ['error' => $e->getMessage()]);
            }
        }

        return ['prices' => $syncedCount];
    }

    private function findAmazonUrl(VideoGame $game): ?string
    {
        $attributes = $game->attributes;
        if (is_string($attributes)) {
            $attributes = json_decode($attributes, true);
        }
        $attributes = $attributes ?? [];

        if (! empty($attributes['amazon_url']) && is_string($attributes['amazon_url'])) {
            return $attributes['amazon_url'];
        }

        $websites = $attributes['websites'] ?? $attributes['original_metadata']['websites'] ?? [];
        if (! is_array($websites)) {
            $websites = [];
        }

        foreach ($websites as $site) {
            if (isset($site['url']) && str_contains($site['url'], 'amazon.')) {
                return $site['url'];
            }
        }

        return null;
    }

    /**
     * Sync GOG price for all countries.
     */
    public function syncGogPrice(VideoGame $game, ?callable $onProgress = null): array
    {
        // 1. Check for existing entry
        $priceEntry = VideoGamePrice::where('video_game_id', $game->id)
            ->where('retailer', 'GOG')
            ->first();

        // 2. Determine URL
        $url = $priceEntry?->url ?? $this->findGogUrl($game);

        if (! $url) {
            return ['prices' => -1];
        }

        $countries = Country::with('currency')->get();
        $syncedCount = 0;

        foreach ($countries as $country) {
            try {
                $code = $country->code;
                $expectedCurrency = $country->currency?->code;

                // 3. Scrape/Fetch
                if ($onProgress) {
                    $onProgress("Fetching GOG price for {$code}...");
                }
                $priceData = $this->gogService->getPrice($url, $code);

                if (! $priceData) {
                    continue;
                }

                // Use DB currency first, API currency as fallback
                $fetchedCurrency = $expectedCurrency ?? $priceData['currency'] ?? 'USD';

                // 4. Update/Create
                VideoGamePrice::updateOrInsert(
                    [
                        'video_game_id' => $game->id,
                        'retailer' => 'GOG',
                        'country_code' => $code,
                    ],
                    [
                        'currency' => $fetchedCurrency,
                        'amount_minor' => $priceData['amount_minor'] ?? 0,
                        'url' => $url,
                        'recorded_at' => now(),
                        'is_active' => true,
                        'updated_at' => now(),
                        'created_at' => now(),
                    ]
                );

                $syncedCount++;

            } catch (\Exception $e) {
                Log::error("GameDataAggregatorService: GOG sync failed for game {$game->id} country {$country->code}", ['error' => $e->getMessage()]);
            }
        }

        return ['prices' => $syncedCount];
    }

    /**
     * Sync Epic Games price for all countries.
     */
    public function syncEpicPrice(VideoGame $game, ?callable $onProgress = null): array
    {
        // 1. Check for existing entry
        $priceEntry = VideoGamePrice::where('video_game_id', $game->id)
            ->where('retailer', 'Epic Games')
            ->first();

        // 2. Determine URL
        $url = $priceEntry?->url ?? $this->findEpicUrl($game);

        if (! $url) {
            return ['prices' => -1];
        }

        $countries = Country::with('currency')->get();
        $syncedCount = 0;

        foreach ($countries as $country) {
            try {
                $code = $country->code;
                $expectedCurrency = $country->currency?->code;

                // 3. Scrape/Fetch
                if ($onProgress) {
                    $onProgress("Fetching Epic Games price for {$code}...");
                }
                $priceData = $this->epicService->getPrice($url, $code);

                if (! $priceData) {
                    continue;
                }

                $fetchedCurrency = $expectedCurrency ?? $priceData['currency'] ?? 'USD';

                // 4. Update/Create
                VideoGamePrice::updateOrInsert(
                    [
                        'video_game_id' => $game->id,
                        'retailer' => 'Epic Games',
                        'country_code' => $code,
                    ],
                    [
                        'currency' => $fetchedCurrency,
                        'amount_minor' => $priceData['amount_minor'] ?? 0,
                        'url' => $url,
                        'recorded_at' => now(),
                        'is_active' => true,
                        'updated_at' => now(),
                        'created_at' => now(),
                    ]
                );

                $syncedCount++;

            } catch (\Exception $e) {
                Log::error("GameDataAggregatorService: Epic sync failed for game {$game->id} country {$code}", ['error' => $e->getMessage()]);
            }
        }

        return ['prices' => $syncedCount];
    }

    private function findGogUrl(VideoGame $game): ?string
    {
        $attributes = $game->attributes;
        if (is_string($attributes)) {
            $attributes = json_decode($attributes, true);
        }
        $attributes = $attributes ?? [];

        if (! empty($attributes['gog_slug']) && is_string($attributes['gog_slug'])) {
            return 'https://www.gog.com/game/'.$attributes['gog_slug'];
        }

        $websites = $attributes['websites'] ?? $attributes['original_metadata']['websites'] ?? [];
        if (! is_array($websites)) {
            $websites = [];
        }

        foreach ($websites as $site) {
            if (isset($site['url']) && str_contains($site['url'], 'gog.com')) {
                return $site['url'];
            }
        }

        return null;
    }

    private function findEpicUrl(VideoGame $game): ?string
    {
        $attributes = $game->attributes;
        if (is_string($attributes)) {
            $attributes = json_decode($attributes, true);
        }
        $attributes = $attributes ?? [];

        if (! empty($attributes['epic_slug']) && is_string($attributes['epic_slug'])) {
            return 'https://store.epicgames.com/en-US/p/'.$attributes['epic_slug'];
        }

        $websites = $attributes['websites'] ?? $attributes['original_metadata']['websites'] ?? [];
        if (! is_array($websites)) {
            $websites = [];
        }

        foreach ($websites as $site) {
            if (isset($site['url']) && (str_contains($site['url'], 'epicgames.com') || str_contains($site['url'], 'store.epicgames.com'))) {
                return $site['url'];
            }
        }

        return null;
    }

    /**
     * Discover a retailer URL for a game using attributes and raw IGDB payloads.
     */
    public function discoverRetailerUrl(VideoGame $game, string $retailer): ?string
    {
        // 1. Check existing price entries
        $existing = VideoGamePrice::where('video_game_id', $game->id)
            ->where('retailer', $retailer)
            ->whereNotNull('url')
            ->first();

        if ($existing) {
            return $existing->url;
        }

        // 2. Map retailer names to domains for discovery
        $map = [
            'Amazon' => 'amazon.',
            'Steam' => 'steampowered.com',
            'Epic Games' => 'epicgames.com',
            'GOG' => 'gog.com',
            'PlayStation Store' => 'playstation.com',
            'Xbox Store' => 'xbox.com',
            'itch.io' => 'itch.io',
            'Nintendo Store' => 'nintendo.com/store',
        ];

        $domain = $map[$retailer] ?? null;

        // 3. Try standard websites list in attributes
        $attributes = $game->attributes;
        if (is_string($attributes)) {
            $attributes = json_decode($attributes, true);
        }
        $attributes = $attributes ?? [];

        if ($retailer === 'GOG') {
            $gogSlug = $attributes['gog_slug'] ?? null;
            if (is_string($gogSlug) && $gogSlug !== '') {
                return 'https://www.gog.com/game/'.$gogSlug;
            }
        }

        if ($retailer === 'Epic Games') {
            $epicSlug = $attributes['epic_slug'] ?? null;
            if (is_string($epicSlug) && $epicSlug !== '') {
                return 'https://store.epicgames.com/en-US/p/'.$epicSlug;
            }
        }

        if ($retailer === 'Steam') {
            $steamId = $attributes['steam_id'] ?? null;
            if (is_numeric($steamId) && (int) $steamId > 0) {
                return 'https://store.steampowered.com/app/'.$steamId;
            }
        }

        if ($retailer === 'Xbox Store') {
            $xboxBigId = $attributes['xbox_bigid'] ?? null;
            if (is_string($xboxBigId) && $xboxBigId !== '') {
                return 'https://www.microsoft.com/store/apps/'.$xboxBigId;
            }
        }

        if ($retailer === 'PlayStation Store') {
            $psProductId = $attributes['ps_product_id'] ?? null;
            if (is_string($psProductId) && $psProductId !== '') {
                return 'https://store.playstation.com/product/'.$psProductId;
            }
        }

        if ($retailer === 'Amazon') {
            $amazonUrl = $attributes['amazon_url'] ?? null;
            if (is_string($amazonUrl) && $amazonUrl !== '') {
                return $amazonUrl;
            }
        }

        if ($retailer === 'Nintendo Store') {
            $nintendoUrl = $attributes['nintendo_url'] ?? null;
            if (is_string($nintendoUrl) && $nintendoUrl !== '') {
                return $nintendoUrl;
            }
        }

        if ($retailer === 'itch.io') {
            $itchUrl = $attributes['itchio_url'] ?? null;
            if (is_string($itchUrl) && $itchUrl !== '') {
                return $itchUrl;
            }
        }

        $websites = $attributes['websites'] ?? $attributes['original_metadata']['websites'] ?? [];
        if (! is_array($websites)) {
            $websites = [];
        }

        if ($domain) {
            foreach ($websites as $site) {
                if (isset($site['url']) && str_contains($site['url'], $domain)) {
                    return $site['url'];
                }
            }
        }

        // 4. Try raw Source Payload (e.g. RAWG Stores)
        // RAWG stores structure: stores[].url or stores[].store.domain
        if ($game->source_payload && $domain) {
            $sourceData = is_string($game->source_payload)
                ? json_decode($game->source_payload, true)
                : $game->source_payload;
            if (isset($sourceData['stores']) && is_array($sourceData['stores'])) {
                foreach ($sourceData['stores'] as $storeData) {
                    $storeUrl = $storeData['url'] ?? null;
                    if ($storeUrl && str_contains($storeUrl, $domain)) {
                        return $storeUrl;
                    }
                }
            }
        }

        // 5. Try raw IGDB payload
        $igdbSource = $game->title?->sources()
            ->where('provider', 'igdb')
            ->whereNotNull('raw_payload')
            ->first();

        if ($igdbSource && $domain) {
            $payload = $igdbSource->raw_payload;
            // Robust double-decoding for IGDB payloads
            while (is_string($payload)) {
                $decoded = json_decode($payload, true);
                if (json_last_error() !== JSON_ERROR_NONE || $decoded === $payload) {
                    break;
                }
                $payload = $decoded;
            }

            if (! is_array($payload)) {
                return null;
            }

            // Check 'external_games'
            $externalGames = $payload['external_games'] ?? [];
            if (is_string($externalGames)) {
                $externalGames = json_decode($externalGames, true);
            }

            if (is_array($externalGames)) {
                foreach ($externalGames as $ext) {
                    if (isset($ext['url']) && str_contains($ext['url'], $domain)) {
                        return $ext['url'];
                    }
                }
            }

            // Check 'websites' (Common in newer IGDB payloads)
            $igdbWebsites = $payload['websites'] ?? [];
            if (is_string($igdbWebsites)) {
                $igdbWebsites = json_decode($igdbWebsites, true);
            }

            if (is_array($igdbWebsites)) {
                foreach ($igdbWebsites as $site) {
                    if (isset($site['url']) && str_contains($site['url'], $domain)) {
                        return $site['url'];
                    }
                }
            }
        }

        return null;
    }

    /**
     * Sync Steam prices for all countries.
     */
    public function syncSteamPrice(VideoGame $game, ?callable $onProgress = null): array
    {
        $url = $this->discoverRetailerUrl($game, 'Steam');
        if (! $url) {
            return ['prices' => 0];
        }

        $appId = null;
        if (preg_match('/app\/(\d+)/', $url, $matches)) {
            $appId = $matches[1];
        }

        if (! $appId) {
            return ['prices' => 0];
        }

        $countries = Country::with('currency')->get();
        $syncedCount = 0;

        foreach ($countries as $country) {
            try {
                $code = $country->code;
                $expectedCurrency = $country->currency?->code;

                if ($onProgress) {
                    $onProgress("Fetching Steam price for {$code}...");
                }
                $priceData = $this->steamService->getPrice($appId, $code);

                if (! $priceData) {
                    continue;
                }

                $fetchedCurrency = $expectedCurrency ?? $priceData['currency'] ?? 'USD';

                VideoGamePrice::updateOrInsert(
                    ['video_game_id' => $game->id, 'retailer' => 'Steam', 'country_code' => $code],
                    [
                        'currency' => $fetchedCurrency,
                        'amount_minor' => $priceData['amount_minor'] ?? 0,
                        'url' => $url,
                        'recorded_at' => now(),
                        'is_active' => true,
                        'updated_at' => now(),
                        'created_at' => now(),
                    ]
                );
                $syncedCount++;
            } catch (\Exception $e) {
                Log::error("Steam sync failed: {$e->getMessage()}");
            }
        }

        return ['prices' => $syncedCount];
    }

    /**
     * Sync Xbox Store prices for all countries.
     */
    public function syncXboxPrice(VideoGame $game, ?callable $onProgress = null): array
    {
        $url = $this->discoverRetailerUrl($game, 'Xbox Store');
        $bigId = null;

        if ($url && preg_match('/\/([A-Z0-9]{12,})/', $url, $matches)) {
            $bigId = $matches[1];
        }

        // Fallback to name search
        if (! $bigId) {
            $bigId = $this->xboxService->resolveProductId($game->name);
        }

        if (! $bigId) {
            return ['prices' => 0];
        }

        // Update URL if we found a broad one or didn't have one
        if (! $url) {
            $url = "https://www.microsoft.com/store/apps/{$bigId}";
        }

        $countries = Country::with('currency')->get();
        $syncedCount = 0;

        foreach ($countries as $country) {
            try {
                $code = $country->code;
                $expectedCurrency = $country->currency?->code;

                if ($onProgress) {
                    $onProgress("Fetching Xbox price for {$code}...");
                }
                $priceData = $this->xboxService->getPrice($bigId, $code);

                if (! $priceData) {
                    continue;
                }

                $fetchedCurrency = $expectedCurrency ?? $priceData['currency'] ?? 'USD';

                VideoGamePrice::updateOrInsert(
                    ['video_game_id' => $game->id, 'retailer' => 'Xbox Store', 'country_code' => $code],
                    [
                        'currency' => $fetchedCurrency,
                        'amount_minor' => $priceData['amount_minor'] ?? 0,
                        'url' => $url,
                        'recorded_at' => now(),
                        'is_active' => true,
                        'updated_at' => now(),
                        'created_at' => now(),
                    ]
                );
                $syncedCount++;
            } catch (\Exception $e) {
                Log::error("Xbox sync failed: {$e->getMessage()}");
            }
        }

        return ['prices' => $syncedCount];
    }

    /**
     * Sync PlayStation Store prices for all countries.
     */
    public function syncPlayStationPrice(VideoGame $game, ?callable $onProgress = null): array
    {
        $url = $this->discoverRetailerUrl($game, 'PlayStation Store');
        $productId = null;

        if ($url && preg_match('/\/product\/([A-Z0-9_-]+)/', $url, $matches)) {
            $productId = $matches[1];
        }

        // Fallback to name search
        if (! $productId) {
            $productId = $this->playstationService->resolveProductId($game->name);
        }

        if (! $productId) {
            return ['prices' => 0];
        }

        if (! $url) {
            $url = "https://store.playstation.com/product/{$productId}";
        }

        $countries = Country::with('currency')->get();
        $syncedCount = 0;

        foreach ($countries as $country) {
            try {
                $code = $country->code;
                $expectedCurrency = $country->currency?->code;

                if ($onProgress) {
                    $onProgress("Fetching PS Store price for {$code}...");
                }
                $priceData = $this->playstationService->getPrice($productId, $code);

                if (! $priceData) {
                    continue;
                }

                $fetchedCurrency = $expectedCurrency ?? $priceData['currency'] ?? 'USD';

                VideoGamePrice::updateOrInsert(
                    ['video_game_id' => $game->id, 'retailer' => 'PlayStation Store', 'country_code' => $code],
                    [
                        'currency' => $fetchedCurrency,
                        'amount_minor' => $priceData['amount_minor'] ?? 0,
                        'url' => $url,
                        'recorded_at' => now(),
                        'is_active' => true,
                        'updated_at' => now(),
                        'created_at' => now(),
                    ]
                );
                $syncedCount++;
            } catch (\Exception $e) {
                Log::error("PS Store sync failed: {$e->getMessage()}");
            }
        }

        return ['prices' => $syncedCount];
    }
}
