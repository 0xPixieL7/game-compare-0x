<?php

namespace App\Services\Price;

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
     * @param int $videoGameId
     * @param bool $forceRefresh Force fetch even if recently updated
     * @param bool $includeMedia Whether to fetch media (screenshots, videos, etc.)
     * @return array Array of price and media data grouped by retailer
     */
    public function getAllData(int $videoGameId, bool $forceRefresh = false, bool $includeMedia = true): array
    {
        $game = VideoGame::find($videoGameId);
        if (!$game) {
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
            $url = $entry->url;

            try {
                // For retailers with rich APIs (Steam, Xbox), fetch full details including media
                if ($includeMedia && !in_array($retailer, $fetchedRetailers)) {
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
        return match($retailer) {
            'Steam' => $this->fetchSteamFullDetails($url, $countryCode),
            'Xbox Store' => $this->fetchXboxFullDetails($url, $countryCode),
            'PlayStation Store' => $this->fetchPlayStationFullDetails($url, $countryCode),
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
        return match($retailer) {
            'Steam' => $this->fetchSteamPrice($url, $countryCode),
            'Amazon' => $this->amazonService->getPrice($url, $countryCode),
            'Epic Games' => $this->epicService->getPrice($url, $countryCode),
            'GOG' => $this->gogService->getPrice($url, $countryCode),
            'Xbox Store' => $this->fetchXboxPrice($url, $countryCode),
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

    /**
     * Update price entry in database.
     */
    private function updatePrice(VideoGamePrice $entry, array $priceData): void
    {
        $entry->update([
            'amount_minor' => $priceData['amount_minor'],
            'currency' => $priceData['currency'],
            'recorded_at' => now(),
            'updated_at' => now(),
        ]);
    }

    /**
     * Persist media to database (images and videos tables).
     */
    private function persistMedia(int $videoGameId, string $source, array $media): void
    {
        try {
            // Store screenshots/images
            if (!empty($media['screenshots'])) {
                foreach ($media['screenshots'] as $screenshot) {
                    DB::table('images')->updateOrInsert(
                        [
                            'imageable_type' => 'App\\Models\\VideoGame',
                            'imageable_id' => $videoGameId,
                            'url' => $screenshot['full'] ?? $screenshot['thumbnail'],
                        ],
                        [
                            'thumbnail_url' => $screenshot['thumbnail'] ?? null,
                            'metadata' => json_encode(['source' => $source, 'type' => 'screenshot']),
                            'updated_at' => now(),
                            'created_at' => now(),
                        ]
                    );
                }
            }

            // Store header/cover image
            if (!empty($media['header_image'])) {
                DB::table('images')->updateOrInsert(
                    [
                        'imageable_type' => 'App\\Models\\VideoGame',
                        'imageable_id' => $videoGameId,
                        'url' => $media['header_image'],
                    ],
                    [
                        'metadata' => json_encode(['source' => $source, 'type' => 'header']),
                        'updated_at' => now(),
                        'created_at' => now(),
                    ]
                );
            }

            // Store background image
            if (!empty($media['background'])) {
                DB::table('images')->updateOrInsert(
                    [
                        'imageable_type' => 'App\\Models\\VideoGame',
                        'imageable_id' => $videoGameId,
                        'url' => $media['background'],
                    ],
                    [
                        'metadata' => json_encode(['source' => $source, 'type' => 'background']),
                        'updated_at' => now(),
                        'created_at' => now(),
                    ]
                );
            }

            // Store videos/trailers
            if (!empty($media['movies'])) {
                foreach ($media['movies'] as $movie) {
                    $videoUrl = $movie['webm_max'] ?? $movie['mp4_max'] ?? $movie['hls_max'] ?? null;
                    
                    if ($videoUrl) {
                        DB::table('videos')->updateOrInsert(
                            [
                                'videoable_type' => 'App\\Models\\VideoGame',
                                'videoable_id' => $videoGameId,
                                'url' => $videoUrl,
                            ],
                            [
                                'thumbnail_url' => $movie['thumbnail'] ?? null,
                                'title' => $movie['name'] ?? null,
                                'metadata' => json_encode([
                                    'source' => $source,
                                    'webm_480' => $movie['webm_480'] ?? null,
                                    'mp4_480' => $movie['mp4_480'] ?? null,
                                ]),
                                'updated_at' => now(),
                                'created_at' => now(),
                            ]
                        );
                    }
                }
            }
        } catch (\Exception $e) {
            Log::error("GameDataAggregatorService: Failed to persist media", [
                'game_id' => $videoGameId,
                'source' => $source,
                'error' => $e->getMessage(),
            ]);
        }
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

        if (!$prices) {
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
}
