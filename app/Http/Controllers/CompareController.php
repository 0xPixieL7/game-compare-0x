<?php

declare(strict_types=1);

namespace App\Http\Controllers;

use App\Models\VideoGameTitleSource;
use Illuminate\Http\Request;
use Illuminate\Support\Facades\Cache;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Str;
use Inertia\Inertia;
use Inertia\Response;

class CompareController extends Controller
{
    public function index(Request $request): Response
    {
        set_time_limit(120); // Prevent timeouts during heavy DB/IO operations

        // ... (existing index code)
        // Check if request expects JSON (for simple polling/updates) - optional but good practice
        // For now, we return Inertia response as requested.

        $searchQuery = $request->get('search', '');
        $limit = min((int) $request->get('limit', 20), 100);

        // Get spotlight products (featured games)
        $spotlight = $this->getSpotlightProducts();

        // Get hero product (top rated)
        $hero = $spotlight[0] ?? null;

        // Get cross reference stats
        $crossReferenceStats = $this->getCrossReferenceStats();

        // Get matches using the optimized query
        $prioritizedMatches = $this->getGameComparisons($searchQuery, $limit);

        // Get platforms and currencies for filters
        $platforms = $this->getAvailablePlatforms();
        $currencies = ['USD', 'EUR', 'GBP', 'JPY'];

        // Get top lists from RAWG and IGDB
        $topLists = $this->getTopLists();

        return Inertia::render('Compare/Index', [
            'hero' => $hero,
            'spotlight' => $spotlight,
            'crossReferenceStats' => $crossReferenceStats,
            'prioritizedMatches' => $prioritizedMatches,
            'topLists' => $topLists,
            'crossReferencePlatforms' => $platforms,
            'crossReferenceCurrencies' => $currencies,
            'regionOptions' => ['US', 'EU', 'GB', 'JP'],
            'apiEndpoints' => [
                'stats' => route('compare.stats', [], false),
                'entries' => route('compare.entries', [], false),
                'spotlight' => route('compare.spotlight', [], false),
            ],
            // Filters can be passed back if needed by the frontend to maintain state
        ]);
    }

    public function stats(): \Illuminate\Http\JsonResponse
    {
        return response()->json($this->getCrossReferenceStats());
    }

    public function entries(Request $request): \Illuminate\Http\JsonResponse
    {
        $searchQuery = $request->get('search', '');
        $limit = min((int) $request->get('limit', 20), 100);

        return response()->json($this->getGameComparisons($searchQuery, $limit));
    }

    public function spotlight(): \Illuminate\Http\JsonResponse
    {
        return response()->json($this->getSpotlightProducts());
    }

    private function getSpotlightProducts(int $limit = 20): array
    {
        // 1. Get RAWG Trending games (Latest popularity)
        $trending = DB::table('public.video_games_toplists_mv')
            ->where('list_key', 'trending')
            ->limit(10)
            ->get();

        // 2. Get Upcoming games (High anticipation)
        $upcoming = DB::table('public.video_games_toplists_mv')
            ->where('list_key', 'upcoming')
            ->limit(10)
            ->get();

        // 3. Get Top Rated from the last 12 months
        $topRecent = DB::table('public.video_games_ranked_mv')
            ->where('release_date', '>', now()->subMonths(12))
            ->orderBy('review_score', 'desc')
            ->limit(10)
            ->get();

        // 4. Latest Shooters (with video and cover)
        $shooters = DB::table('public.video_games_ranked_mv')
            ->where('genre', 'ilike', '%Shooter%')
            ->whereNotNull('primary_video_id')
            ->whereNotNull('cover_url')
            ->where('release_date', '>', now()->subMonths(18))
            ->orderBy('review_score', 'desc')
            ->limit(5)
            ->get();

        // 5. Latest Sports (with video and cover)
        $sports = DB::table('public.video_games_ranked_mv')
            ->where(function ($q) {
                $q->where('genre', 'ilike', '%Sport%')
                    ->orWhere('genre', 'ilike', '%Sports%');
            })
            ->whereNotNull('primary_video_id')
            ->whereNotNull('cover_url')
            ->where('release_date', '>', now()->subMonths(18))
            ->orderBy('review_score', 'desc')
            ->limit(5)
            ->get();

        // 6. Specifically look for GTA VI if not present
        $gta = DB::table('public.video_games_ranked_mv')
            ->where('name', 'ilike', '%Grand Theft Auto VI%')
            ->orWhere('name', 'ilike', '%GTA VI%')
            ->first();

        // Merge and deduplicate
        $merged = collect($trending)
            ->concat($upcoming)
            ->concat($topRecent)
            ->concat($shooters)
            ->concat($sports)
            ->when($gta, fn ($c) => $c->prepend($gta))
            ->unique('id')
            ->sortByDesc('review_score')
            ->take($limit);

        return $merged->map(function ($game) {
            $platforms = $game->platform ? (is_string($game->platform) ? json_decode($game->platform, true) : $game->platform) : [];

            // Build gallery
            $gallery = [];
            if ($game->primary_video_id) {
                $gallery[] = ['id' => Str::random(8), 'type' => 'video', 'url' => $game->primary_video_id, 'source' => 'YouTube'];
            }
            if ($game->cover_url) {
                $gallery[] = ['id' => Str::random(8), 'type' => 'image', 'url' => $this->upscaleImage($game->cover_url, 't_720p'), 'source' => 'IGDB'];
            }
            if ($game->background_url) {
                $gallery[] = ['id' => Str::random(8), 'type' => 'image', 'url' => $this->upscaleImage($game->background_url, 't_1080p'), 'source' => 'IGDB'];
            }
            if ($game->artwork_url) {
                $gallery[] = ['id' => Str::random(8), 'type' => 'image', 'url' => $this->upscaleImage($game->artwork_url, 't_1080p'), 'source' => 'IGDB'];
            }

            // Spotlight score breakdown
            $rating = (float) ($game->rating ?? 0);
            $reviewScore = (float) ($game->review_score ?? 70);

            $verdict = match (true) {
                $reviewScore >= 90 => 'Masterpiece',
                $reviewScore >= 80 => 'Essential',
                $reviewScore >= 70 => 'Great',
                default => 'Mixed',
            };

            return [
                'id' => $game->id,
                'name' => $game->name,
                'slug' => $game->slug,
                'image' => $this->upscaleImage($game->cover_url ?: $game->image_url, 't_1080p'),
                'background' => $this->upscaleImage($game->background_url, 't_1080p'),
                'platform_labels' => is_array($platforms) ? $platforms : [$platforms],
                'region_codes' => ['US', 'EU', 'JP', 'GB'],
                'currencies' => ['USD', 'EUR', 'JPY', 'GBP'],
                'retailer_names' => ['Steam', 'PlayStation', 'Xbox'],
                'trailer_url' => $game->primary_video_id ? "https://www.youtube.com/watch?v={$game->primary_video_id}" : null,
                'spotlight_score' => [
                    'total' => round($reviewScore / 10, 1),
                    'grade' => $reviewScore >= 90 ? 'S+' : ($reviewScore >= 80 ? 'A' : 'B'),
                    'verdict' => $verdict,
                    'breakdown' => [
                        ['label' => 'Critical Reception', 'score' => (int) $rating, 'summary' => 'Aggregated rating from critics.', 'weight_percentage' => 40],
                        ['label' => 'Popularity', 'score' => (int) min($game->popularity_score ?? 0, 100), 'summary' => 'Community follows and hype.', 'weight_percentage' => 30],
                        ['label' => 'Ranking', 'score' => (int) min($reviewScore, 100), 'summary' => 'System weighted placement.', 'weight_percentage' => 30],
                    ],
                ],
                'spotlight_gallery' => ! empty($gallery) ? $gallery : [
                    ['id' => '1', 'type' => 'image', 'url' => $this->upscaleImage($game->cover_url ?: $game->image_url, 't_1080p'), 'source' => 'IGDB'],
                ],
            ];
        })->toArray();
    }

    private function getCrossReferenceStats(): array
    {
        $total = VideoGameTitleSource::count();

        // Mock distribution based on total count
        return [
            'total' => $total,
            'digital' => (int) ($total * 0.6),
            'physical' => (int) ($total * 0.4),
            'both' => (int) ($total * 0.3), // Overlap
            'generated_at' => now()->toIso8601String(),
            'displayed' => 0,
            'display_limit' => 100,
        ];
    }

    private function getAvailablePlatforms(): array
    {
        return ['Switch', 'PS5', 'PS4', 'Xbox Series X', 'Xbox One', 'PC'];
    }

    private function getGameComparisons(string $search = '', int $limit = 20): array
    {
        $query = DB::table('public.video_games_ranked_mv');

        if ($search) {
            $query->where(function ($q) use ($search) {
                $q->where('name', 'ilike', "%{$search}%")
                    ->orWhere('canonical_name', 'ilike', "%{$search}%");
            });
        }

        $results = $query->orderBy('release_date', 'desc')
            ->orderBy('review_score', 'desc')
            ->limit($limit)
            ->get();

        return $results->map(function ($game) {
            // Get price data - we pass an empty collection for sources as we don't need them here
            $priceData = $this->getPriceDataForGame($game->name, collect());

            $hasDigital = false; // Mock for now
            $hasPhysical = count($priceData['entries']) > 0;

            $platforms = $game->platform ? json_decode($game->platform, true) : ['Unknown'];
            if (! is_array($platforms)) {
                $platforms = [$platforms];
            }

            return [
                'product_id' => $game->id,
                'product_slug' => $game->slug,
                'name' => $game->name,
                'image' => $this->upscaleImage($game->cover_url ?: $game->image_url),
                'platforms' => $platforms,
                'has_digital' => $hasDigital,
                'has_physical' => $hasPhysical,
                'currencies' => ['USD', 'EUR'],
                'digital' => [
                    'best' => null,
                    'offers' => [],
                    'currencies' => [],
                ],
                'physical' => array_map(function ($entry) {
                    return [
                        'console' => $entry['console'],
                        'formatted_price' => '$'.number_format($entry['base_price'], 2),
                    ];
                }, $priceData['entries']),
                'best_digital' => null,
                'best_physical' => $priceData['best_price'],
                'rating' => $game->rating,
                'review_score' => $game->review_score,
                'normalized_key' => $game->slug,
                'updated_at' => now()->toIso8601String(),
            ];
        })->toArray();
    }

    private function getPriceDataForGame(string $gameName, $sources): array
    {
        // Simple memory cache
        static $priceCache = [];

        if (isset($priceCache[$gameName])) {
            return $priceCache[$gameName];
        }

        $priceData = $this->loadPriceChartingData($gameName);
        $currencies = ['USD' => 1.0, 'EUR' => 0.85, 'GBP' => 0.73, 'JPY' => 110.0];

        $processedPrices = [];
        foreach ($priceData as $price) {
            $basePrice = $this->parsePriceString($price['loose-price'] ?? '0');

            if ($basePrice > 0) {
                $convertedPrices = [];
                foreach ($currencies as $currency => $rate) {
                    $convertedPrices[$currency] = round($basePrice * $rate, 2);
                }

                $processedPrices[] = [
                    'console' => $price['console-name'] ?? 'Unknown',
                    'product' => $price['product-name'] ?? 'Unknown',
                    'base_price' => $basePrice,
                    'currencies' => $convertedPrices,
                ];
            }
        }

        $result = [
            'entries' => $processedPrices,
            'best_price' => $processedPrices ? min(array_column($processedPrices, 'base_price')) : null,
            'price_range' => $processedPrices ? [
                'min' => min(array_column($processedPrices, 'base_price')),
                'max' => max(array_column($processedPrices, 'base_price')),
            ] : null,
        ];

        $priceCache[$gameName] = $result;

        return $result;
    }

    private function loadPriceChartingData(string $gameName): array
    {
        $csvData = Cache::remember('compare:price-guide-sample', 3600, function () {
            $csvPath = base_path('price-guide.csv');
            if (! file_exists($csvPath)) {
                return [];
            }

            $rows = [];
            $handle = fopen($csvPath, 'r');
            if ($handle !== false) {
                $headers = fgetcsv($handle);
                $rowCount = 0;
                while (($row = fgetcsv($handle)) && $rowCount < 5000) {
                    if (count($row) >= 4 && is_array($headers)) {
                        if (count($row) === count($headers)) {
                            $rows[] = array_combine($headers, $row);
                        }
                    }
                    $rowCount++;
                }
                fclose($handle);
            }

            return $rows;
        });

        if (empty($csvData)) {
            return [];
        }

        $matches = [];
        $searchTerm = strtolower($gameName);

        foreach ($csvData as $row) {
            if (! empty($row['product-name'])) {
                $productName = strtolower($row['product-name']);
                if (str_contains($productName, $searchTerm) || str_contains($searchTerm, $productName)) {
                    $matches[] = $row;
                }
            }

            if (count($matches) >= 10) {
                break;
            }
        }

        return $matches;
    }

    private function parsePriceString(string $priceStr): float
    {
        $cleaned = preg_replace('/[^0-9.,]/', '', $priceStr);

        return (float) str_replace(',', '', $cleaned);
    }

    private function getTopLists(): array
    {
        $rows = DB::table('public.video_games_toplists_mv')
            ->orderByRaw("
                CASE 
                    WHEN list_key = 'trending' THEN 0 
                    WHEN list_key = 'upcoming' THEN 1 
                    WHEN list_key = 'popular' THEN 2
                    WHEN list_key = 'top_rated' THEN 3
                    ELSE 4 
                END
            ")
            ->orderBy('rank')
            ->get();

        $grouped = $rows->groupBy('list_key');

        return $grouped->map(function ($items, $key) {
            return [
                'key' => $key,
                'title' => $items->first()->list_name,
                'provider' => $items->first()->provider_key,
                'games' => $items->map(function ($item) {
                    $prices = $item->prices ? json_decode($item->prices, true) : [];

                    return [
                        'id' => $item->id,
                        'name' => $item->name,
                        'slug' => $item->slug,
                        'rank' => $item->rank,
                        'rating' => $item->rating,
                        'release_date' => $item->release_date,
                        'year' => $item->release_date ? date('Y', strtotime($item->release_date)) : null,
                        'platform' => $item->platform ? json_decode($item->platform, true) : [],
                        'image' => $this->upscaleImage($item->image_url),
                        'cover' => $this->upscaleImage($item->cover_url),
                        'background' => $this->upscaleImage($item->background_url, 't_720p'),
                        'artwork' => $this->upscaleImage($item->artwork_url, 't_720p'),
                        'video_id' => $item->primary_video_id,
                        'video_name' => $item->primary_video_name,
                        'review_score' => round((float) $item->review_score, 1),
                        'popularity_score' => $item->popularity_score,
                        'provider' => $item->provider_key,
                        'prices' => [
                            'usd' => isset($prices['USD']) ? $prices['USD'] / 100 : null,
                            'eur' => isset($prices['EUR']) ? $prices['EUR'] / 100 : null,
                            'gbp' => isset($prices['GBP']) ? $prices['GBP'] / 100 : null,
                        ],
                    ];
                })->toArray(),
            ];
        })->values()->toArray();
    }

    /**
     * Upscale IGDB image URLs to higher quality versions.
     */
    private function upscaleImage(?string $url, string $target = 't_cover_big'): ?string
    {
        if (! $url) {
            return null;
        }

        if (str_contains($url, 'igdb.com')) {
            $url = str_replace(['t_thumb', 't_cover_small', 't_logo_med'], $target, $url);
            if (str_starts_with($url, '//')) {
                $url = 'https:'.$url;
            }
        }

        return $url;
    }
}
