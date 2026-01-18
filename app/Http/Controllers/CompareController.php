<?php

declare(strict_types=1);

namespace App\Http\Controllers;

use App\Models\VideoGameTitleSource;
use Illuminate\Http\Request;
use Inertia\Inertia;
use Inertia\Response;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Str;

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

        // Get cross reference stats
        $crossReferenceStats = $this->getCrossReferenceStats();

        // Get matches using the optimized query
        $prioritizedMatches = $this->getGameComparisons($searchQuery, $limit);

        // Get platforms and currencies for filters
        $platforms = $this->getAvailablePlatforms();
        $currencies = ['USD', 'EUR', 'GBP', 'JPY'];

        // Get hero product (top rated)
        $hero = $this->getHeroProduct();

        return Inertia::render('Compare/Index', [
            'hero' => $hero,
            'spotlight' => $spotlight,
            'crossReferenceStats' => $crossReferenceStats,
            'prioritizedMatches' => $prioritizedMatches,
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

    private function getSpotlightProducts(): array
    {
        // Mock spotlight data for the "Apple TV" style carousel
        return [
            [
                'id' => 1,
                'name' => 'The Legend of Zelda: Breath of the Wild',
                'slug' => 'the-legend-of-zelda-breath-of-the-wild',
                'image' => 'https://images.igdb.com/igdb/image/upload/t_1080p/co1wyy.webp',
                'platform_labels' => ['Switch', 'Wii U'],
                'region_codes' => ['US', 'EU', 'JP'],
                'currencies' => ['USD', 'EUR', 'JPY'],
                'retailer_names' => ['Nintendo eShop', 'Amazon', 'GameStop'],
                'spotlight_score' => [
                    'total' => 9.7,
                    'grade' => 'S+',
                    'verdict' => 'Masterpiece',
                    'breakdown' => [
                        ['label' => 'Critical Reception', 'score' => 98, 'summary' => 'Universal acclaim from critics worldwide.', 'weight_percentage' => 40],
                        ['label' => 'Price Stability', 'score' => 85, 'summary' => 'Holds value exceptionally well.', 'weight_percentage' => 30],
                        ['label' => 'Availability', 'score' => 100, 'summary' => 'Widely available physically and digitally.', 'weight_percentage' => 30],
                    ],
                ],
                'spotlight_gallery' => [
                   ['id' => '1', 'type' => 'image', 'url' => 'https://images.igdb.com/igdb/image/upload/t_1080p/co1wyy.webp', 'source' => 'IGDB'],
                   ['id' => '2', 'type' => 'image', 'url' => 'https://images.igdb.com/igdb/image/upload/t_1080p/sc2.webp', 'source' => 'IGDB'],
                ],
            ],
            [
                'id' => 2,
                'name' => 'Elden Ring',
                'slug' => 'elden-ring',
                'image' => 'https://images.igdb.com/igdb/image/upload/t_1080p/co4jni.webp',
                'platform_labels' => ['PS5', 'XSX', 'PC'],
                'region_codes' => ['US', 'EU', 'GB'],
                'currencies' => ['USD', 'EUR', 'GBP'],
                'retailer_names' => ['Steam', 'PS Store', 'Xbox Store'],
                'spotlight_score' => [
                    'total' => 9.6,
                    'grade' => 'S+',
                    'verdict' => 'Essential',
                    'breakdown' => [
                        ['label' => 'Critical Reception', 'score' => 96, 'summary' => 'Critically acclaimed open world RPG.', 'weight_percentage' => 40],
                        ['label' => 'Price Stability', 'score' => 90, 'summary' => 'Frequent sales but high base value.', 'weight_percentage' => 30],
                        ['label' => 'Performance', 'score' => 95, 'summary' => 'Excellent performance on current gen consoles.', 'weight_percentage' => 30],
                    ],
                ],
                'spotlight_gallery' => [
                   ['id' => '3', 'type' => 'image', 'url' => 'https://images.igdb.com/igdb/image/upload/t_1080p/co4jni.webp', 'source' => 'IGDB'],
                ],
            ]
        ];
    }

    private function getHeroProduct(): ?array
    {
        // For now, let's use the first spotlight product as the hero
        // In the future, this could be dynamic, latest release, or editor's pick
        $spotlights = $this->getSpotlightProducts();
        
        return $spotlights[0] ?? null;
    }

    private function getCrossReferenceStats(): array
    {
        $total = VideoGameTitleSource::count();
        // Mock distribution based on total count
        return [
            'total' => $total,
            'digital' => (int)($total * 0.6),
            'physical' => (int)($total * 0.4),
            'both' => (int)($total * 0.3), // Overlap
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
        $query = VideoGameTitleSource::query()
            ->select([
                'video_game_title_sources.id',
                'video_game_title_sources.name as game_name',
                'video_game_title_sources.platform',
                'video_game_title_sources.provider',
                'video_game_title_sources.external_id',
                'video_game_title_sources.rating',
                'video_game_title_sources.release_date',
                'video_game_sources.display_name as source_name',
                'video_game_titles.name as canonical_name',
            ])
            ->join('video_game_sources', 'video_game_title_sources.video_game_source_id', '=', 'video_game_sources.id')
            ->join('video_game_titles', 'video_game_title_sources.video_game_title_id', '=', 'video_game_titles.id')
            ->whereNotNull('video_game_title_sources.name');

        if ($search) {
            $query->where(function ($q) use ($search) {
                $q->whereRaw('LOWER(video_game_title_sources.name) LIKE LOWER(?)', ["%{$search}%"])
                    ->orWhereRaw('LOWER(video_game_titles.canonical_name) LIKE LOWER(?)', ["%{$search}%"]);
            });
        }

        $results = $query->orderBy('video_game_title_sources.rating', 'desc')
            ->limit($limit)
            ->get();

        // Group by game title for comparison
        $grouped = $results->groupBy('canonical_name');

        return $grouped->map(function ($sources, $gameName) {
            // Get price data
            $priceData = $this->getPriceDataForGame($gameName, $sources);
            
            $hasDigital = false; // We would determine this from sources in a real scenario
            $hasPhysical = count($priceData['entries']) > 0;
            
            // Determine platforms from sources
            $platforms = $sources->pluck('platform')
                ->flatten()
                ->unique()
                ->filter()
                ->values()
                ->toArray();
                
            if (empty($platforms)) {
                $platforms = ['Unknown'];
            }

            // Placeholder image
            $image = 'https://images.igdb.com/igdb/image/upload/t_cover_big/co1wyy.webp'; 

            return [
                'product_id' => $sources->first()->id,
                'product_slug' => Str::slug($gameName),
                'name' => $gameName,
                'image' => $image,
                'platforms' => $platforms,
                'has_digital' => $hasDigital,
                'has_physical' => $hasPhysical,
                'currencies' => ['USD', 'EUR'],
                'digital' => [
                    'best' => null,
                    'offers' => [],
                    'currencies' => [],
                ],
                'physical' => array_map(function($entry) {
                    return [
                        'console' => $entry['console'],
                        'formatted_price' => '$' . number_format($entry['base_price'], 2),
                    ];
                }, $priceData['entries']),
                'best_digital' => null,
                'best_physical' => $priceData['best_price'],
                'rating' => $sources->max('rating'),
                'normalized_key' => Str::slug($gameName),
                'updated_at' => now()->toIso8601String(),
            ];
        })->values()->toArray();
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
        $csvData = Cache::remember('compare:price-guide-sample', 3600, function() {
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
             if (!empty($row['product-name'])) {
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
}
