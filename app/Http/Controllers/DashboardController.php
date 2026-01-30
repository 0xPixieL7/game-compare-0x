<?php

declare(strict_types=1);

namespace App\Http\Controllers;

use Illuminate\Http\Request;
use Illuminate\Support\Facades\Cache;
use Illuminate\Support\Facades\DB;
use Inertia\Inertia;
use Inertia\Response;

class DashboardController extends Controller
{
    public function show(Request $request, string $gameId): Response
    {
        $gameId = (int) $gameId;

        // Dispatch viewed event for background enrichment
        $videoGame = \App\Models\VideoGame::find($gameId);
        if ($videoGame) {
            \App\Events\VideoGameViewed::dispatch($videoGame);
        }

        $startTime = microtime(true);

        // Execute multiple queries in parallel using multiple connections
        $game = $this->getGameWithMedia($gameId);

        if (! $game) {
            abort(404);
        }

        // Use parallel execution for price and availability data
        $priceData = [];
        $availabilityData = [];

        if ($request->user() !== null) {
            // PERFORMANCE: Increased cache from 600s (10min) to 3600s (1hr) for better hit rate
            $priceData = Cache::remember("price_analysis_{$gameId}", 3600, function () use ($gameId) {
                return $this->getPriceAnalysis($gameId);
            });

            $availabilityData = Cache::remember("availability_data_{$gameId}", 3600, function () use ($gameId) {
                return $this->getAvailabilityData($gameId);
            });
        }

        return Inertia::render('Dashboard/Show', [
            'game' => $game,
            'priceData' => $priceData,
            'availabilityData' => $availabilityData,
            'meta' => [
                'query_time' => microtime(true) - $startTime,
                'cached' => [
                    'game' => Cache::has("game_with_media_{$gameId}"),
                    'prices' => $request->user() !== null && Cache::has("price_analysis_{$gameId}"),
                    'availability' => $request->user() !== null && Cache::has("availability_data_{$gameId}"),
                ],
            ],
        ]);
    }

    private function getGameWithMedia(int $gameId): ?array
    {
        // PERFORMANCE: Increased cache from 300s (5min) to 3600s (1hr) for better hit rate
        return Cache::remember("game_with_media_{$gameId}", 3600, function () use ($gameId) {
            // Get basic game info with optimized join
            $game = DB::table('video_games')
                ->select([
                    'video_games.id',
                    'video_games.name',
                    'video_games.rating',
                    'video_games.release_date',
                    'video_games.video_game_title_id',
                    'video_game_titles.name as canonical_name',
                    'video_game_titles.normalized_title',
                ])
                ->join('video_game_titles', 'video_games.video_game_title_id', '=', 'video_game_titles.id')
                ->where('video_games.id', $gameId)
                ->first();

            if (! $game) {
                return null;
            }

            // Get all metadata sources for this title
            $sources = DB::table('video_game_title_sources')
                ->select([
                    'provider',
                    'name',
                    'rating',
                    'external_id',
                ])
                ->where('video_game_title_id', $game->video_game_title_id)
                ->get()
                ->map(fn ($s) => [
                    'provider' => $s->provider,
                    'name' => $s->name,
                    'rating' => $s->rating,
                    'external_id' => $s->external_id,
                ]);

            // Get all platform variants for this title
            $variants = DB::table('video_games')
                ->select(['id', 'name', 'platform', 'rating'])
                ->where('video_game_title_id', $game->video_game_title_id)
                ->get();

            // Use the primary IGDB source for the main description/media
            $primarySource = DB::table('video_game_title_sources')
                ->where('video_game_title_id', $game->video_game_title_id)
                ->where('provider', 'igdb')
                ->first() ?? DB::table('video_game_title_sources')
                ->where('video_game_title_id', $game->video_game_title_id)
                ->first();

            // Parse media from raw_payload
            $rawPayload = $primarySource ? json_decode($primarySource->raw_payload, true) : [];
            $media = $this->extractMediaFromPayload($rawPayload);

            return [
                'id' => $game->id,
                'name' => $game->name,
                'canonical_name' => $game->canonical_name,
                'normalized_title' => $game->normalized_title,
                'rating' => $game->rating,
                'release_date' => $game->release_date,
                'description' => $primarySource?->description ?? 'No description available',
                'synopsis' => $rawPayload['summary'] ?? $rawPayload['storyline'] ?? '',
                'developer' => $primarySource?->developer ?? 'Unknown',
                'publisher' => $primarySource?->publisher ?? 'Unknown',
                'platforms' => json_decode($primarySource?->platform ?? '[]', true),
                'genres' => json_decode($primarySource?->genre ?? '[]', true),
                'media' => $media,
                'sources' => $sources,
                'variants' => $variants,
            ];
        });
    }

    private function extractMediaFromPayload(array $payload): array
    {
        $baseUrl = 'https://images.igdb.com/igdb/image/upload/';

        // Extract cover - IGDB stores cover as string ID, not object
        $cover = null;
        if (! empty($payload['cover'])) {
            $imageId = is_string($payload['cover']) ? $payload['cover'] : $payload['cover']['image_id'] ?? null;
            if ($imageId) {
                $cover = [
                    'url' => $baseUrl.'t_1080p/co'.base_convert($imageId, 10, 36).'.webp',
                    'width' => 1080,
                    'height' => 1440,
                    'external_id' => $imageId,
                ];
            }
        }

        // Extract screenshots - IGDB stores as array of string IDs
        $screenshots = [];
        if (! empty($payload['screenshots']) && is_array($payload['screenshots'])) {
            foreach (array_slice($payload['screenshots'], 0, 20) as $screenshotId) {
                if (is_string($screenshotId) || is_numeric($screenshotId)) {
                    $screenshots[] = [
                        'url' => $baseUrl.'t_1080p/sc'.base_convert($screenshotId, 10, 36).'.webp',
                        'width' => 1920,
                        'height' => 1080,
                        'external_id' => $screenshotId,
                    ];
                }
            }
        }

        // Extract artworks - IGDB stores as array of string IDs
        $artworks = [];
        if (! empty($payload['artworks']) && is_array($payload['artworks'])) {
            foreach (array_slice($payload['artworks'], 0, 10) as $artworkId) {
                if (is_string($artworkId) || is_numeric($artworkId)) {
                    $artworks[] = [
                        'url' => $baseUrl.'t_1080p/ar'.base_convert($artworkId, 10, 36).'.webp',
                        'width' => 1920,
                        'height' => 1080,
                        'external_id' => $artworkId,
                    ];
                }
            }
        }

        // Extract videos/trailers
        $trailers = [];
        if (! empty($payload['videos']) && is_array($payload['videos'])) {
            foreach (array_slice($payload['videos'], 0, 5) as $video) {
                if (! empty($video['video_id'])) {
                    $trailers[] = [
                        'video_id' => $video['video_id'],
                        'name' => $video['name'] ?? 'Trailer',
                        'checksum' => $video['checksum'] ?? null,
                    ];
                }
            }
        }

        // Determine the best Hero URL (Artwork > Screenshot > Cover)
        $heroUrl = null;
        if (! empty($artworks)) {
            $heroUrl = $baseUrl.'t_original/ar'.base_convert($artworks[0]['external_id'], 10, 36).'.webp';
        } elseif (! empty($screenshots)) {
            $heroUrl = $baseUrl.'t_original/sc'.base_convert($screenshots[0]['external_id'], 10, 36).'.webp';
        } elseif ($cover) {
            $heroUrl = $baseUrl.'t_original/co'.base_convert($cover['external_id'], 10, 36).'.webp';
        }

        return [
            'cover' => $cover,
            'screenshots' => $screenshots,
            'artworks' => $artworks,
            'trailers' => $trailers,
            'hero_url' => $heroUrl,
            'cover_url_high_res' => $cover ? $baseUrl.'t_1080p/co'.base_convert($cover['external_id'], 10, 36).'.webp' : null,
            'cover_url_mobile' => $cover ? $baseUrl.'t_cover_big/co'.base_convert($cover['external_id'], 10, 36).'.webp' : null,
            'summary' => [
                'images' => [
                    'has_cover' => ! empty($cover),
                    'has_screenshots' => ! empty($screenshots),
                    'has_artworks' => ! empty($artworks),
                    'total_count' => count($screenshots) + count($artworks) + ($cover ? 1 : 0),
                    'hero_url' => $heroUrl,
                ],
                'videos' => [
                    'has_trailers' => ! empty($trailers),
                    'total_count' => count($trailers),
                ],
            ],
        ];
    }

    private function getPriceAnalysis(int $gameId): array
    {
        // Optimized single query with CTE for better performance
        $results = DB::select("
            WITH latest_prices AS (
                SELECT
                    vgp.currency,
                    vgp.amount_minor,
                    vgp.recorded_at,
                    vgp.retailer,
                    c.name as country_name,
                    c.code as country_code,
                    ROW_NUMBER() OVER (PARTITION BY vgp.currency, c.code ORDER BY vgp.recorded_at DESC) as rn
                FROM video_game_prices vgp
                JOIN countries c ON vgp.region_code = c.code
                WHERE vgp.video_game_id = ? AND vgp.is_active = true
            ),
            currency_stats AS (
                SELECT
                    currency,
                    country_name,
                    country_code,
                    amount_minor,
                    retailer,
                    recorded_at,
                    MIN(amount_minor) OVER (PARTITION BY currency) as min_price,
                    MAX(amount_minor) OVER (PARTITION BY currency) as max_price,
                    AVG(amount_minor) OVER (PARTITION BY currency) as avg_price,
                    COUNT(*) OVER (PARTITION BY currency) as country_count
                FROM latest_prices
                WHERE rn = 1
            )
            SELECT
                currency,
                json_agg(
                    json_build_object(
                        'country', country_name,
                        'country_code', country_code,
                        'price', amount_minor::float / 100,
                        'retailer', retailer,
                        'recorded_at', recorded_at
                    )
                ) as countries,
                min_price::float / 100 as min_price,
                max_price::float / 100 as max_price,
                avg_price::float / 100 as avg_price,
                CASE
                    WHEN country_count > 1 THEN ((max_price - min_price)::float / min_price) * 100
                    ELSE 0
                END as disparity_percentage
            FROM currency_stats
            WHERE country_count >= 2
            GROUP BY currency, min_price, max_price, avg_price, country_count
        ", [$gameId]);

        return collect($results)->map(function ($row) {
            return [
                'currency' => $row->currency,
                'countries' => json_decode($row->countries, true),
                'min_price' => $row->min_price,
                'max_price' => $row->max_price,
                'avg_price' => $row->avg_price,
                'disparity_percentage' => $row->disparity_percentage,
            ];
        })->values()->toArray();
    }

    private function getAvailabilityData(int $gameId): array
    {
        // Get availability data by checking which countries have active prices
        $availability = DB::table('video_game_prices')
            ->select([
                'countries.name as country_name',
                'countries.code as country_code',
                DB::raw('COUNT(DISTINCT video_game_prices.retailer) as retailer_count'),
                DB::raw('COUNT(DISTINCT video_game_prices.currency) as currency_count'),
                DB::raw('MIN(video_game_prices.recorded_at) as first_seen'),
                DB::raw('MAX(video_game_prices.recorded_at) as last_updated'),
            ])
            ->join('countries', 'video_game_prices.region_code', '=', 'countries.code')
            ->where('video_game_prices.video_game_id', $gameId)
            ->where('video_game_prices.is_active', true)
            ->groupBy('countries.name', 'countries.code')
            ->get()
            ->map(function ($item) {
                return [
                    'country' => $item->country_name,
                    'country_code' => $item->country_code,
                    'retailer_count' => $item->retailer_count,
                    'currency_count' => $item->currency_count,
                    'first_seen' => $item->first_seen,
                    'last_updated' => $item->last_updated,
                    'availability_score' => min(($item->retailer_count * 20) + ($item->currency_count * 10), 100),
                ];
            })
            ->sortByDesc('availability_score')
            ->values()
            ->toArray();

        return $availability;
    }

    private function getMockPriceData(int $gameId): array
    {
        return [
            [
                'currency' => 'USD',
                'countries' => [
                    ['country' => 'United States', 'country_code' => 'US', 'price' => 59.99, 'retailer' => 'GameStop', 'recorded_at' => '2024-01-15T12:00:00Z'],
                    ['country' => 'Canada', 'country_code' => 'CA', 'price' => 79.99, 'retailer' => 'Best Buy', 'recorded_at' => '2024-01-15T12:00:00Z'],
                ],
                'min_price' => 59.99,
                'max_price' => 79.99,
                'avg_price' => 69.99,
                'disparity_percentage' => 33.3,
            ],
            [
                'currency' => 'EUR',
                'countries' => [
                    ['country' => 'Germany', 'country_code' => 'DE', 'price' => 49.99, 'retailer' => 'MediaMarkt', 'recorded_at' => '2024-01-15T12:00:00Z'],
                    ['country' => 'France', 'country_code' => 'FR', 'price' => 54.99, 'retailer' => 'Fnac', 'recorded_at' => '2024-01-15T12:00:00Z'],
                    ['country' => 'Spain', 'country_code' => 'ES', 'price' => 52.99, 'retailer' => 'GAME', 'recorded_at' => '2024-01-15T12:00:00Z'],
                ],
                'min_price' => 49.99,
                'max_price' => 54.99,
                'avg_price' => 52.65,
                'disparity_percentage' => 10.0,
            ],
            [
                'currency' => 'GBP',
                'countries' => [
                    ['country' => 'United Kingdom', 'country_code' => 'GB', 'price' => 44.99, 'retailer' => 'GAME', 'recorded_at' => '2024-01-15T12:00:00Z'],
                    ['country' => 'Ireland', 'country_code' => 'IE', 'price' => 47.99, 'retailer' => 'GameStop', 'recorded_at' => '2024-01-15T12:00:00Z'],
                ],
                'min_price' => 44.99,
                'max_price' => 47.99,
                'avg_price' => 46.49,
                'disparity_percentage' => 6.7,
            ],
        ];
    }

    private function getMockAvailabilityData(int $gameId): array
    {
        return [
            ['country' => 'United States', 'country_code' => 'US', 'retailer_count' => 5, 'currency_count' => 1, 'availability_score' => 100],
            ['country' => 'Germany', 'country_code' => 'DE', 'retailer_count' => 4, 'currency_count' => 1, 'availability_score' => 90],
            ['country' => 'United Kingdom', 'country_code' => 'GB', 'retailer_count' => 3, 'currency_count' => 1, 'availability_score' => 70],
            ['country' => 'France', 'country_code' => 'FR', 'retailer_count' => 3, 'currency_count' => 1, 'availability_score' => 70],
            ['country' => 'Canada', 'country_code' => 'CA', 'retailer_count' => 2, 'currency_count' => 1, 'availability_score' => 50],
            ['country' => 'Australia', 'country_code' => 'AU', 'retailer_count' => 2, 'currency_count' => 1, 'availability_score' => 50],
            ['country' => 'Japan', 'country_code' => 'JP', 'retailer_count' => 2, 'currency_count' => 1, 'availability_score' => 50],
            ['country' => 'Spain', 'country_code' => 'ES', 'retailer_count' => 1, 'currency_count' => 1, 'availability_score' => 30],
        ];
    }

    public function index(Request $request): Response
    {
        $searchQuery = $request->get('search', '');
        $user = $request->user();

        // Get spotlight and hero data for a premium feel
        $spotlightGames = $this->fetchSpotlightGamesForDashboard(12);

        // Personalize spotlight if user is logged in
        if ($user) {
            $likedGamesForSpotlight = $this->getUserLikedGamesForSpotlight($user);
            if (! empty($likedGamesForSpotlight)) {
                // Prepend liked games to spotlight, removing duplicates
                $spotlightIds = array_column($spotlightGames, 'id');
                $likedGamesForSpotlight = array_filter($likedGamesForSpotlight, function ($game) use ($spotlightIds) {
                    return ! in_array($game['id'], $spotlightIds);
                });
                $spotlightGames = array_merge($likedGamesForSpotlight, $spotlightGames);
                // Limit to 12 again
                $spotlightGames = array_slice($spotlightGames, 0, 12);
            }
        }

        $hero = $spotlightGames[0] ?? null;

        // Get genre-based carousel rows and user preferences
        $carouselRows = $this->getGenreBasedCarouselRows($user);

        // For search functionality, get filtered games
        $searchResults = [];
        if ($searchQuery) {
            $searchResults = $this->getSearchResults($searchQuery);
        }

        return Inertia::render('Dashboard/Index', [
            'hero' => $hero,
            'spotlightGames' => $spotlightGames,
            'carouselRows' => $carouselRows,
            'searchResults' => $searchResults,
            'search' => $searchQuery,
            'meta' => [
                'total_rows' => count($carouselRows),
                'query_time' => microtime(true) - (defined('LARAVEL_START') ? LARAVEL_START : microtime(true)),
            ],
        ]);
    }

    private function fetchSpotlightGamesForDashboard(int $limit = 12): array
    {
        return Cache::remember('dashboard:spotlight-games-v3', 3600, function () use ($limit) {
            // Pull top games from materialized view top lists
            // Priority: trending (3), upcoming (2), popular (4), top_rated (3)
            $topListGames = DB::table('video_games_toplists_mv')
                ->whereIn('list_key', ['trending', 'upcoming', 'popular', 'top_rated'])
                ->whereNotNull('cover_url')
                ->where('rating', '>=', 80)
                ->orderByRaw("
                    CASE 
                        WHEN list_key = 'trending' THEN 0 
                        WHEN list_key = 'popular' THEN 1 
                        WHEN list_key = 'top_rated' THEN 2
                        WHEN list_key = 'upcoming' THEN 3
                    END
                ")
                ->orderBy('rank')
                ->limit($limit)
                ->get();

            // If we have enough games from top lists, use them
            if ($topListGames->count() >= $limit) {
                return $topListGames->map(function ($game) {
                    return $this->mapTopListGameToSpotlight($game);
                })->toArray();
            }

            // Otherwise, fallback to marquee games
            $marqueeIds = [
                12026, 199224, 117836, 221441, 174739, 221545, 233062,
                220587, 235660, 260502, 92989, 142457, 220450, 274649, 257269,
            ];

            $games = DB::table('video_games')
                ->join('video_games_ranked_mv', 'video_games.id', '=', 'video_games_ranked_mv.id')
                ->leftJoin('video_game_titles', 'video_games.video_game_title_id', '=', 'video_game_titles.id')
                ->leftJoin('video_game_title_sources', function ($join) {
                    $join->on('video_game_title_sources.video_game_title_id', '=', 'video_game_titles.id')
                        ->where('video_game_title_sources.provider', '=', 'igdb');
                })
                ->select([
                    'video_games.id',
                    'video_games.name',
                    'video_games_ranked_mv.rating',
                    'video_games_ranked_mv.popularity_score',
                    'video_game_titles.name as canonical_name',
                    'video_game_title_sources.raw_payload',
                ])
                ->whereIn('video_games.id', $marqueeIds)
                ->where('video_games_ranked_mv.rating', '>=', 80)
                ->whereNotNull('video_game_title_sources.raw_payload')
                ->limit($limit)
                ->get();

            return $games->map(function ($game) {
                return $this->mapDashboardSpotlightGame($game);
            })->toArray();
        });
    }

    private function mapDashboardSpotlightGame(object $game): array
    {
        $rawPayload = $game->raw_payload ? json_decode($game->raw_payload, true) : [];
        if (is_string($rawPayload)) {
            $rawPayload = json_decode($rawPayload, true) ?? [];
        }

        $baseUrl = 'https://images.igdb.com/igdb/image/upload/';
        $coverUrl = null;
        if (! empty($rawPayload['cover'])) {
            $imageId = is_array($rawPayload['cover']) ? ($rawPayload['cover']['image_id'] ?? null) : null;
            if ($imageId) {
                $coverUrl = $baseUrl.'t_1080p/'.$imageId.'.webp';
            }
        }

        $screenshots = [];
        if (! empty($rawPayload['screenshots']) && is_array($rawPayload['screenshots'])) {
            foreach (array_slice($rawPayload['screenshots'], 0, 5) as $s) {
                $id = is_array($s) ? ($s['image_id'] ?? null) : null;
                if ($id) {
                    $screenshots[] = ['url' => $baseUrl.'t_1080p/'.$id.'.webp'];
                }
            }
        }

        $reviewScore = (float) ($game->rating ?? 85);

        $gallery = [];
        foreach ($screenshots as $s) {
            $gallery[] = ['id' => uniqid(), 'type' => 'image', 'url' => $s['url'], 'source' => 'IGDB'];
        }

        return [
            'id' => $game->id,
            'name' => $game->canonical_name ?? $game->name,
            'image' => $coverUrl ?: ($screenshots[0]['url'] ?? null),
            'background' => $screenshots[0]['url'] ?? $coverUrl,
            'spotlight_score' => [
                'total' => round($reviewScore / 10, 1),
                'grade' => $reviewScore >= 90 ? 'S' : ($reviewScore >= 80 ? 'A' : 'B'),
                'verdict' => $reviewScore >= 90 ? 'Masterpiece' : 'Essential',
                'breakdown' => [
                    ['label' => 'Critical Reception', 'score' => (int) $reviewScore, 'summary' => 'Aggregated rating.', 'weight_percentage' => 100],
                ],
            ],
            'spotlight_gallery' => $gallery,
        ];
    }

    private function mapTopListGameToSpotlight(object $game): array
    {
        $reviewScore = (float) ($game->rating ?? 85);
        $coverUrl = $this->upscaleIgdbImage($game->cover_url, 't_1080p');
        $backgroundUrl = $this->upscaleIgdbImage($game->background_url, 't_1080p');

        $gallery = [];
        if ($backgroundUrl) {
            $gallery[] = ['id' => uniqid(), 'type' => 'image', 'url' => $backgroundUrl, 'source' => 'IGDB'];
        }

        return [
            'id' => $game->id,
            'name' => $game->name,
            'image' => $coverUrl,
            'background' => $backgroundUrl ?? $coverUrl,
            'spotlight_score' => [
                'total' => round($reviewScore / 10, 1),
                'grade' => $reviewScore >= 90 ? 'S' : ($reviewScore >= 80 ? 'A' : 'B'),
                'verdict' => $reviewScore >= 90 ? 'Masterpiece' : 'Essential',
                'breakdown' => [
                    ['label' => 'Critical Reception', 'score' => (int) $reviewScore, 'summary' => 'Aggregated rating.', 'weight_percentage' => 100],
                ],
            ],
            'spotlight_gallery' => $gallery,
        ];
    }

    private function getGenreBasedCarouselRows(?\App\Models\User $user = null): array
    {
        // Define genre priority order starting with sports
        $genrePriority = [
            'Sport' => 'Sports Games',
            'Racing' => 'Racing Games',
            'Fighting' => 'Fighting Games',
            'Shooter' => 'Shooter Games',
            'Action' => 'Action Games',
            'Adventure' => 'Adventure Games',
            'Role-playing (RPG)' => 'RPG Games',
            'Strategy' => 'Strategy Games',
            'Simulation' => 'Simulation Games',
            'Puzzle' => 'Puzzle Games',
            'Platform' => 'Platform Games',
            'Indie' => 'Indie Games',
        ];

        $rows = [];

        // Add "My Likes" row if user is logged in
        if ($user) {
            $likedGames = $this->getUserLikedGames($user);
            if (count($likedGames) > 0) {
                $rows[] = [
                    'id' => 'my_likes',
                    'title' => 'My Likes',
                    'type' => 'user_list',
                    'games' => $likedGames,
                    'description' => 'Games you have liked',
                ];
            }
        }

        // Add "Recently Viewed" row
        $rows[] = [
            'id' => 'recently_viewed',
            'title' => 'Recently Viewed',
            'type' => 'recent',
            'games' => [], // Will be populated by frontend based on user activity
            'description' => 'Games you\'ve recently looked at',
        ];

        // Add top lists from materialized view (includes trending, upcoming, popular, top_rated, and all genre lists)
        $topLists = $this->getTopListsFromMaterializedView();
        foreach ($topLists as $list) {
            $rows[] = $list;
        }

        // Add "Highest Rated" row for all games
        $topRatedGames = $this->getTopRatedGames(25);
        if (count($topRatedGames) > 0) {
            $rows[] = [
                'id' => 'highest_rated',
                'title' => 'Highest Rated Games',
                'type' => 'top_rated',
                'games' => $topRatedGames,
                'description' => 'The highest rated games across all genres',
            ];
        }

        // Add "New Releases" row
        $newReleases = $this->getNewReleases(20);
        if (count($newReleases) > 0) {
            $rows[] = [
                'id' => 'new_releases',
                'title' => 'New Releases',
                'type' => 'new_releases',
                'games' => $newReleases,
                'description' => 'Recently released games',
            ];
        }

        return $rows;
    }

    private function getUserLikedGames(\App\Models\User $user, int $limit = 20): array
    {
        return $user->likes()
            ->with(['title', 'title.sources' => function ($q) {
                $q->where('provider', 'igdb');
            }])
            ->orderByPivot('created_at', 'desc')
            ->limit($limit)
            ->get()
            ->map(function ($game) {
                $primarySource = $game->title?->sources->first();
                $rawPayload = $primarySource?->raw_payload ?? [];
                if (is_string($rawPayload)) {
                    $rawPayload = json_decode($rawPayload, true) ?? [];
                }
                if (! is_array($rawPayload)) {
                    $rawPayload = [];
                }

                $cover = $this->getCoverFromPayload($rawPayload);

                return [
                    'id' => $game->id,
                    'name' => $game->name,
                    'canonical_name' => $game->title?->name ?? $game->name,
                    'rating' => $primarySource->rating ?? $game->rating,
                    'release_date' => $game->release_date?->format('Y-m-d'),
                    'media' => [
                        'cover_url' => $cover['cover_url'] ?? null,
                        'cover_url_thumb' => $cover['cover_url_thumb'] ?? null,
                        'screenshots' => [],
                        'trailers' => [],
                    ],
                ];
            })
            ->toArray();
    }

    private function getUserLikedGamesForSpotlight(\App\Models\User $user, int $limit = 5): array
    {
        return $user->likes()
            ->with(['title', 'title.sources' => function ($q) {
                $q->where('provider', 'igdb');
            }])
            ->orderByPivot('created_at', 'desc')
            ->limit($limit)
            ->get()
            ->map(function ($game) {
                $primarySource = $game->title?->sources->first();
                $game->raw_payload = $primarySource?->raw_payload;
                $game->canonical_name = $game->title?->name ?? $game->name;

                return $this->mapDashboardSpotlightGame($game);
            })
            ->toArray();
    }

    private function getGamesByGenre(string $genre, int $limit = 20): array
    {
        return DB::table('video_games')
            ->select([
                'video_games.id',
                'video_games.name',
                'video_games.rating',
                'video_games.release_date',
                'video_game_titles.name as canonical_name',
                'video_game_title_sources.raw_payload',
                'video_game_title_sources.rating as source_rating',
                'video_game_title_sources.genre',
            ])
            ->join('video_game_titles', 'video_games.video_game_title_id', '=', 'video_game_titles.id')
            ->leftJoin('video_game_title_sources', function ($join) {
                $join->on('video_game_title_sources.video_game_title_id', '=', 'video_game_titles.id')
                    ->where('video_game_title_sources.provider', '=', 'igdb');
            })
            ->whereNotNull('video_game_title_sources.rating')
            ->where('video_game_title_sources.rating', '>=', 75)
            ->where('video_game_title_sources.rating_count', '>=', 10)
            ->whereNotNull('video_game_title_sources.genre')
            ->whereRaw('LOWER(video_game_title_sources.genre::text) LIKE LOWER(?)', ["%{$genre}%"])
            ->orderBy('video_game_title_sources.rating', 'desc')
            ->limit($limit)
            ->get()
            ->map(function ($game) {
                $rawPayload = $game->raw_payload ? json_decode($game->raw_payload, true) : [];
                // Ensure $rawPayload is always an array (json_decode can return null on invalid JSON)
                if (! is_array($rawPayload)) {
                    $rawPayload = [];
                }
                $cover = $this->getCoverFromPayload($rawPayload);

                return [
                    'id' => $game->id,
                    'name' => $game->name,
                    'canonical_name' => $game->canonical_name,
                    'rating' => $game->source_rating ?? $game->rating,
                    'release_date' => $game->release_date,
                    'media' => [
                        'cover_url' => $cover['cover_url'] ?? null,
                        'cover_url_thumb' => $cover['cover_url_thumb'] ?? null,
                        'screenshots' => [],
                        'trailers' => [],
                    ],
                ];
            })
            ->toArray();
    }

    private function getTopRatedGames(int $limit = 25): array
    {
        return DB::table('video_games')
            ->select([
                'video_games.id',
                'video_games.name',
                'video_games.rating',
                'video_games.release_date',
                'video_game_titles.name as canonical_name',
                'video_game_title_sources.raw_payload',
                'video_game_title_sources.rating as source_rating',
            ])
            ->join('video_game_titles', 'video_games.video_game_title_id', '=', 'video_game_titles.id')
            ->leftJoin('video_game_title_sources', function ($join) {
                $join->on('video_game_title_sources.video_game_title_id', '=', 'video_game_titles.id')
                    ->where('video_game_title_sources.provider', '=', 'igdb');
            })
            ->whereNotNull('video_game_title_sources.rating')
            ->where('video_game_title_sources.rating', '>=', 85)
            ->where('video_game_title_sources.rating_count', '>=', 20)
            ->orderBy('video_game_title_sources.rating', 'desc')
            ->limit($limit)
            ->get()
            ->map(function ($game) {
                $rawPayload = $game->raw_payload ? json_decode($game->raw_payload, true) : [];
                // Ensure $rawPayload is always an array (json_decode can return null on invalid JSON)
                if (! is_array($rawPayload)) {
                    $rawPayload = [];
                }
                $cover = $this->getCoverFromPayload($rawPayload);

                return [
                    'id' => $game->id,
                    'name' => $game->name,
                    'canonical_name' => $game->canonical_name,
                    'rating' => $game->source_rating ?? $game->rating,
                    'release_date' => $game->release_date,
                    'media' => [
                        'cover_url' => $cover['cover_url'] ?? null,
                        'cover_url_thumb' => $cover['cover_url_thumb'] ?? null,
                        'screenshots' => [],
                        'trailers' => [],
                    ],
                ];
            })
            ->toArray();
    }

    private function getNewReleases(int $limit = 20): array
    {
        return DB::table('video_games')
            ->select([
                'video_games.id',
                'video_games.name',
                'video_games.rating',
                'video_games.release_date',
                'video_game_titles.name as canonical_name',
                'video_game_title_sources.raw_payload',
                'video_game_title_sources.rating as source_rating',
            ])
            ->join('video_game_titles', 'video_games.video_game_title_id', '=', 'video_game_titles.id')
            ->leftJoin('video_game_title_sources', function ($join) {
                $join->on('video_game_title_sources.video_game_title_id', '=', 'video_game_titles.id')
                    ->where('video_game_title_sources.provider', '=', 'igdb');
            })
            ->whereNotNull('video_games.release_date')
            ->whereNotNull('video_game_title_sources.rating')
            ->where('video_game_title_sources.rating', '>=', 75)
            ->where('video_game_title_sources.rating_count', '>=', 5)
            ->orderBy('video_games.release_date', 'desc')
            ->limit($limit)
            ->get()
            ->map(function ($game) {
                $rawPayload = $game->raw_payload ? json_decode($game->raw_payload, true) : [];
                // Ensure $rawPayload is always an array (json_decode can return null on invalid JSON)
                if (! is_array($rawPayload)) {
                    $rawPayload = [];
                }
                $cover = $this->getCoverFromPayload($rawPayload);

                return [
                    'id' => $game->id,
                    'name' => $game->name,
                    'canonical_name' => $game->canonical_name,
                    'rating' => $game->source_rating ?? $game->rating,
                    'release_date' => $game->release_date,
                    'media' => [
                        'cover_url' => $cover['cover_url'] ?? null,
                        'cover_url_thumb' => $cover['cover_url_thumb'] ?? null,
                        'screenshots' => [],
                        'trailers' => [],
                    ],
                ];
            })
            ->toArray();
    }

    private function getSearchResults(string $searchQuery): array
    {
        return DB::table('video_games')
            ->select([
                'video_games.id',
                'video_games.name',
                'video_games.rating',
                'video_games.release_date',
                'video_game_titles.name as canonical_name',
                'video_game_title_sources.raw_payload',
                'video_game_title_sources.rating as source_rating',
            ])
            ->join('video_game_titles', 'video_games.video_game_title_id', '=', 'video_game_titles.id')
            ->leftJoin('video_game_title_sources', function ($join) {
                $join->on('video_game_title_sources.video_game_title_id', '=', 'video_game_titles.id')
                    ->where('video_game_title_sources.provider', '=', 'igdb');
            })
            ->where(function ($query) use ($searchQuery) {
                $query->whereRaw('LOWER(video_games.name) LIKE LOWER(?)', ["%{$searchQuery}%"])
                    ->orWhereRaw('LOWER(video_game_titles.name) LIKE LOWER(?)', ["%{$searchQuery}%"]);
            })
            ->whereNotNull('video_game_title_sources.rating')
            ->where('video_game_title_sources.rating', '>=', 60)
            ->where('video_game_title_sources.rating_count', '>=', 5)
            ->orderBy('video_game_title_sources.rating', 'desc')
            ->limit(50)
            ->get()
            ->map(function ($game) {
                $rawPayload = $game->raw_payload ? json_decode($game->raw_payload, true) : [];
                // Ensure $rawPayload is always an array (json_decode can return null on invalid JSON)
                if (! is_array($rawPayload)) {
                    $rawPayload = [];
                }
                $cover = $this->getCoverFromPayload($rawPayload);

                return [
                    'id' => $game->id,
                    'name' => $game->name,
                    'canonical_name' => $game->canonical_name,
                    'rating' => $game->source_rating ?? $game->rating,
                    'release_date' => $game->release_date,
                    'media' => [
                        'cover_url' => $cover['cover_url'] ?? null,
                        'cover_url_thumb' => $cover['cover_url_thumb'] ?? null,
                        'screenshots' => [],
                        'trailers' => [],
                    ],
                ];
            })
            ->toArray();
    }

    private function getCoverFromPayload(array $payload): array
    {
        $baseUrl = 'https://images.igdb.com/igdb/image/upload/';
        $result = [
            'cover_url' => null,
            'cover_url_thumb' => null,
        ];

        if (! empty($payload['cover'])) {
            $imageId = is_string($payload['cover']) ? $payload['cover'] : $payload['cover']['image_id'] ?? null;
            if ($imageId) {
                // Use t_1080p for high quality covers
                $result['cover_url'] = $baseUrl.'t_1080p/co'.base_convert($imageId, 10, 36).'.webp';
                $result['cover_url_thumb'] = $baseUrl.'t_thumb/co'.base_convert($imageId, 10, 36).'.webp';
            }
        }

        return $result;
    }

    private function getMockGameData(int $gameId): array
    {
        $mockGames = [
            1 => [
                'name' => 'The Legend of Zelda: Breath of the Wild',
                'canonical_name' => 'The Legend of Zelda: Breath of the Wild',
                'rating' => 97,
                'release_date' => '2017-03-03',
                'description' => 'Step into a world of discovery, exploration, and adventure in The Legend of Zelda: Breath of the Wild, a boundary-breaking new game in the acclaimed series.',
                'synopsis' => 'An epic open-world adventure that revolutionized the Zelda series with unprecedented freedom and exploration.',
                'developer' => 'Nintendo EPD',
                'publisher' => 'Nintendo',
                'platforms' => ['Nintendo Switch', 'Wii U'],
                'genres' => ['Action', 'Adventure', 'Open World'],
                'cover_url_high_res' => 'https://images.igdb.com/igdb/image/upload/t_1080p/co1wyy.webp',
                'cover_url_mobile' => 'https://images.igdb.com/igdb/image/upload/t_cover_big/co1wyy.webp',
            ],
            2 => [
                'name' => 'Red Dead Redemption 2',
                'canonical_name' => 'Red Dead Redemption 2',
                'rating' => 93,
                'release_date' => '2018-10-26',
                'description' => 'Winner of over 175 Game of the Year Awards and recipient of over 250 perfect scores, RDR2 is the epic tale of outlaw Arthur Morgan.',
                'synopsis' => 'An immersive Wild West epic featuring stunning visuals and deep storytelling.',
                'developer' => 'Rockstar Games',
                'publisher' => 'Rockstar Games',
                'platforms' => ['PlayStation 4', 'Xbox One', 'PC', 'Stadia'],
                'genres' => ['Action', 'Adventure', 'Open World', 'Western'],
                'cover_url_high_res' => 'https://images.igdb.com/igdb/image/upload/t_1080p/co1q1f.webp',
                'cover_url_mobile' => 'https://images.igdb.com/igdb/image/upload/t_cover_big/co1q1f.webp',
            ],
            3 => [
                'name' => 'Cyberpunk 2077',
                'canonical_name' => 'Cyberpunk 2077',
                'rating' => 86,
                'release_date' => '2020-12-10',
                'description' => 'Cyberpunk 2077 is an open-world, action-adventure story set in Night City, a megalopolis obsessed with power, glamour and body modification.',
                'synopsis' => 'A futuristic RPG set in a dystopian world where technology and humanity collide.',
                'developer' => 'CD Projekt RED',
                'publisher' => 'CD Projekt',
                'platforms' => ['PC', 'PlayStation 4', 'PlayStation 5', 'Xbox One', 'Xbox Series X/S'],
                'genres' => ['Action', 'RPG', 'Open World', 'Cyberpunk'],
                'cover_url_high_res' => 'https://images.igdb.com/igdb/image/upload/t_1080p/co1wyy.webp',
                'cover_url_mobile' => 'https://images.igdb.com/igdb/image/upload/t_cover_big/co1wyy.webp',
            ],
        ];

        $game = $mockGames[$gameId] ?? $mockGames[1];

        return [
            'id' => $gameId,
            'name' => $game['name'],
            'canonical_name' => $game['canonical_name'],
            'normalized_title' => strtolower(str_replace(' ', '_', $game['canonical_name'])),
            'rating' => $game['rating'],
            'release_date' => $game['release_date'],
            'description' => $game['description'],
            'synopsis' => $game['synopsis'],
            'developer' => $game['developer'],
            'publisher' => $game['publisher'],
            'platforms' => $game['platforms'],
            'genres' => $game['genres'],
            'media' => [
                'cover' => [
                    'url' => $game['cover_url_high_res'],
                    'width' => 1080,
                    'height' => 1440,
                ],
                'screenshots' => [
                    ['url' => 'https://images.igdb.com/igdb/image/upload/t_1080p/sc1.webp', 'width' => 1920, 'height' => 1080],
                    ['url' => 'https://images.igdb.com/igdb/image/upload/t_1080p/sc2.webp', 'width' => 1920, 'height' => 1080],
                    ['url' => 'https://images.igdb.com/igdb/image/upload/t_1080p/sc3.webp', 'width' => 1920, 'height' => 1080],
                ],
                'artworks' => [
                    ['url' => 'https://images.igdb.com/igdb/image/upload/t_1080p/art1.webp', 'width' => 1920, 'height' => 1080],
                    ['url' => 'https://images.igdb.com/igdb/image/upload/t_1080p/art2.webp', 'width' => 1920, 'height' => 1080],
                ],
                'trailers' => [],
                'cover_url_high_res' => $game['cover_url_high_res'],
                'cover_url_mobile' => $game['cover_url_mobile'],
                'summary' => [
                    'images' => [
                        'has_cover' => true,
                        'has_screenshots' => true,
                        'has_artworks' => true,
                        'total_count' => 6,
                    ],
                    'videos' => [
                        'has_trailers' => false,
                        'total_count' => 0,
                    ],
                ],
            ],
        ];
    }

    private function getMockGamesIndex(string $search = '', int $limit = 20): array
    {
        $mockGames = [
            [
                'id' => 1,
                'name' => 'The Legend of Zelda: Breath of the Wild',
                'canonical_name' => 'The Legend of Zelda: Breath of the Wild',
                'rating' => 97,
                'release_date' => '2017-03-03',
                'cover_url' => 'https://images.igdb.com/igdb/image/upload/t_cover_big/co1wyy.webp',
                'cover_url_thumb' => 'https://images.igdb.com/igdb/image/upload/t_thumb/co1wyy.webp',
            ],
            [
                'id' => 2,
                'name' => 'Red Dead Redemption 2',
                'canonical_name' => 'Red Dead Redemption 2',
                'rating' => 93,
                'release_date' => '2018-10-26',
                'cover_url' => 'https://images.igdb.com/igdb/image/upload/t_cover_big/co1q1f.webp',
                'cover_url_thumb' => 'https://images.igdb.com/igdb/image/upload/t_thumb/co1q1f.webp',
            ],
            [
                'id' => 3,
                'name' => 'Cyberpunk 2077',
                'canonical_name' => 'Cyberpunk 2077',
                'rating' => 86,
                'release_date' => '2020-12-10',
                'cover_url' => 'https://images.igdb.com/igdb/image/upload/t_cover_big/co1wyy.webp',
                'cover_url_thumb' => 'https://images.igdb.com/igdb/image/upload/t_thumb/co1wyy.webp',
            ],
            [
                'id' => 4,
                'name' => 'The Witcher 3: Wild Hunt',
                'canonical_name' => 'The Witcher 3: Wild Hunt',
                'rating' => 91,
                'release_date' => '2015-05-19',
                'cover_url' => 'https://images.igdb.com/igdb/image/upload/t_cover_big/co1wyy.webp',
                'cover_url_thumb' => 'https://images.igdb.com/igdb/image/upload/t_thumb/co1wyy.webp',
            ],
            [
                'id' => 5,
                'name' => 'God of War',
                'canonical_name' => 'God of War (2018)',
                'rating' => 89,
                'release_date' => '2018-04-20',
                'cover_url' => 'https://images.igdb.com/igdb/image/upload/t_cover_big/co1wyy.webp',
                'cover_url_thumb' => 'https://images.igdb.com/igdb/image/upload/t_thumb/co1wyy.webp',
            ],
        ];

        if ($search) {
            $mockGames = array_filter($mockGames, function ($game) use ($search) {
                return stripos($game['name'], $search) !== false ||
                       stripos($game['canonical_name'], $search) !== false;
            });
        }

        return array_slice($mockGames, 0, $limit);
    }

    /**
     * Get all top lists from the materialized view.
     * Includes main lists (trending, upcoming, popular, top_rated) and all genre-specific lists.
     */
    private function getTopListsFromMaterializedView(): array
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
            ->orderBy('list_name')
            ->orderBy('rank')
            ->get();

        $grouped = $rows->groupBy('list_key');

        return $grouped->map(function ($items, $key) {
            return [
                'id' => $key,
                'title' => $items->first()->list_name,
                'type' => 'top_list',
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
                        'media' => [
                            'cover' => $this->upscaleIgdbImage($item->cover_url),
                            'cover_url' => $this->upscaleIgdbImage($item->image_url),
                            'cover_url_thumb' => $this->upscaleIgdbImage($item->image_url, 't_thumb'),
                            'background' => $this->upscaleIgdbImage($item->background_url, 't_720p'),
                            'screenshots' => [],
                            'trailers' => $item->primary_video_id ? [[
                                'url' => $item->primary_video_id,
                                'name' => $item->primary_video_name,
                            ]] : [],
                        ],
                        'review_score' => round((float) $item->review_score, 1),
                        'popularity_score' => $item->popularity_score,
                        'provider' => $item->provider_key,
                        'pricing' => [
                            'amount_major' => isset($prices['USD']) ? $prices['USD'] / 100 : null,
                            'currency' => 'USD',
                        ],
                    ];
                })->toArray(),
                'description' => $items->first()->list_name,
            ];
        })->values()->toArray();
    }

    /**
     * Upscale IGDB image URLs to higher quality versions.
     */
    private function upscaleIgdbImage(?string $url, string $target = 't_cover_big'): ?string
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
