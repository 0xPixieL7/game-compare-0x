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
use Illuminate\Support\Facades\Log;

class PriceAggregatorService
{
    public function __construct(
        private SteamStoreService $steamService,
        private AmazonScraperService $amazonService,
        private EpicGamesStoreService $epicService,
        private GogStoreService $gogService,
        private XboxStoreService $xboxService,
        private ItchIoScraperService $itchIoService
    ) {}

    /**
     * Fetch all available prices for a video game from all retailers and APIs.
     *
     * @param  bool  $forceRefresh  Force fetch even if recently updated
     * @return array Array of price data grouped by retailer
     */
    public function getAllPrices(int $videoGameId, bool $forceRefresh = false): array
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
            'errors' => [],
        ];

        foreach ($priceEntries as $entry) {
            $retailer = $entry->retailer;
            $countryCode = $entry->country_code ?? 'US';
            $url = $entry->url;

            try {
                $priceData = $this->fetchPrice($retailer, $url, $countryCode);

                if ($priceData) {
                    // Update database
                    $entry->update([
                        'amount_minor' => $priceData['amount_minor'],
                        'currency' => $priceData['currency'],
                        'recorded_at' => now(),
                        'updated_at' => now(),
                    ]);

                    // Add to results
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
                Log::error("PriceAggregatorService: Error fetching {$retailer} price", [
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
            'itch.io' => $this->fetchItchIoPrice($url),
            default => null,
        };
    }

    private function fetchItchIoPrice(string $url): ?array
    {
        // Extract slug from itch.io URL: https://author.itch.io/game-slug
        if (preg_match('/https?:\/\/([^\.]+)\.itch\.io\/([^\/]+)/', $url, $matches)) {
            $username = $matches[1];
            $gameSlug = $matches[2];

            return $this->itchIoService->getPrice($gameSlug, $username);
        }

        return null;
    }

    private function fetchSteamPrice(string $url, string $countryCode): ?array
    {
        // Extract Steam App ID from URL
        if (preg_match('/app\/(\d+)/', $url, $matches)) {
            $appId = $matches[1];

            return $this->steamService->getPrice($appId, $countryCode);
        }

        return null;
    }

    private function fetchXboxPrice(string $url, string $countryCode): ?array
    {
        // Extract BigId from URL: https://www.microsoft.com/store/productId/9NBLGGH4...
        if (preg_match('/\/([A-Z0-9]{12,})/', $url, $matches)) {
            $bigId = $matches[1];

            return $this->xboxService->getPrice($bigId, $countryCode);
        }

        return null;
    }

    /**
     * Format price for human-readable display.
     */
    private function formatPrice(int $amountMinor, string $currency): string
    {
        // Zero-decimal currencies (no cents)
        $zeroDecimal = ['JPY', 'KRW', 'CLP', 'VND'];

        if (in_array($currency, $zeroDecimal)) {
            return $currency.' '.number_format($amountMinor);
        }

        $major = $amountMinor / 100;

        // Currency symbols
        $symbols = [
            'USD' => '$', 'EUR' => '€', 'GBP' => '£', 'JPY' => '¥',
            'AUD' => 'A$', 'CAD' => 'C$', 'NZD' => 'NZ$', 'SGD' => 'S$',
        ];

        $symbol = $symbols[$currency] ?? $currency.' ';

        return $symbol.number_format($major, 2);
    }

    /**
     * Get lowest price across all retailers for a game.
     */
    public function getLowestPrice(int $videoGameId, ?string $targetCurrency = 'USD'): ?array
    {
        $prices = VideoGamePrice::where('video_game_id', $videoGameId)
            ->where('is_active', true)
            ->where('amount_minor', '>', 0) // Exclude unknown/free
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
}
