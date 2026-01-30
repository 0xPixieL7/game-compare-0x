<?php

declare(strict_types=1);

namespace App\Http\Controllers;

use App\Models\Country;
use App\Services\ExchangeRates\TradingViewClient;
use Illuminate\Contracts\Cache\Repository as CacheRepository;
use Illuminate\Database\Query\Builder;
use Illuminate\Http\Request;
use Illuminate\Support\Arr;
use Illuminate\Support\Collection;
use Illuminate\Support\Facades\Cache;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Log;
use Illuminate\Support\Str;
use Inertia\Inertia;
use Inertia\Response;

class LandingController extends Controller
{
    private const ROW_LIMIT = 40;

    private const GENRE_POOL_LIMIT = 100;

    private const LANDING_FEATURED_YEAR_MIN = 2015;

    private const LANDING_FEATURED_YEAR_MAX = 2025;

    private const LANDING_FEATURED_MIN_RATING = 85;

    private const LANDING_FEATURED_FALLBACK_MIN_RATING = 80;

    private const LANDING_FEATURED_MIN_RATING_COUNT = 5;

    private const COUNTRY_CURRENCY_CACHE_TTL = 3600;

    private const ROW_CACHE_TTL = 14400; // 4 hours - Premium data is relatively stable

    public function __construct(private readonly TradingViewClient $tradingViewClient) {}

    public function index(Request $request): Response
    {
        set_time_limit(120); // Give it extra time if DB is under heavy load

        Log::info('Homepage hit', [
            'ip' => $request->ip(),
            'user_agent' => $request->userAgent(),
            'auth' => $request->user()?->id,
        ]);

        $isAuthenticated = $request->user() !== null;

        // Featured: highly-rated 2015-2025 picks with price data, by genre
        $genreRows = $this->fetchGenreRows(self::ROW_LIMIT);

        // Also fetch some of the original rows for variety
        $bestDealsData = $this->fetchBestDeals(self::ROW_LIMIT);

        // Convert to rows format
        $rows = [];
        $allGameIds = [];

        // Add genre spotlight rows next
        foreach ($genreRows as $row) {
            if (! empty($row['games'])) {
                $gameIds = $row['games']->pluck('id')->toArray();
                $allGameIds = array_merge($allGameIds, $gameIds);

                $rows[] = [
                    'id' => $row['key'],
                    'title' => $row['title'],
                    'games' => $row['games'],
                ];
            }
        }

        // Add best deals if we have them
        if (! empty($bestDealsData['games'])) {
            $dealIds = $bestDealsData['games']->pluck('id')->toArray();
            $allGameIds = array_merge($allGameIds, $dealIds);

            $rows[] = [
                'id' => 'best-deals',
                'title' => 'Best Deals',
                'games' => $bestDealsData['games'],
            ];
        }

        // Get unique game IDs for pricing
        $displayIds = array_unique($allGameIds);
        $pricingMap = $isAuthenticated ? $this->buildPricingMapForIds($displayIds) : [];
        $pricingMap = array_replace($pricingMap, $bestDealsData['pricing'] ?? []);

        // Map all games to the expected format
        foreach ($rows as &$row) {
            $row['games'] = $this->mapGames($row['games'], $pricingMap, $isAuthenticated);
        }

        // Get spotlight games for hero selection
        $spotlightGames = $this->fetchSpotlightGames(20);
        $zeldaHero = $this->fetchZeldaHero();

        if ($zeldaHero) {
            $spotlightGames = collect($spotlightGames)
                ->reject(fn ($game) => data_get($game, 'id') === $zeldaHero['id'])
                ->prepend($zeldaHero)
                ->values()
                ->all();
        }

        $hero = $spotlightGames[0] ?? null;

        Log::info('Homepage data fetched', [
            'genreRows' => count($genreRows),
            'totalRows' => count($rows),
            'heroLinked' => $hero !== null,
            'spotlightCount' => count($spotlightGames),
        ]);

        return Inertia::render('welcome', [
            'hero' => $hero,
            'spotlightGames' => $spotlightGames,
            'rows' => $rows,
            'cta' => [
                'pricing' => 'Join free for price data',
            ],
        ]);
    }

    public function debugSpotlight()
    {
        return response()->json($this->fetchSpotlightGames(20));
    }

    private function fetchSpotlightGames(int $limit = 6): array
    {
        return $this->cacheStore()->remember('landing:spotlight-games-v19', self::ROW_CACHE_TTL, function () use ($limit) {
            // These are prioritized "Marquee" titles the user expects to see
            // Mario Kart 8, EA FC, NBA 2K, Halo, COD, Pokemon, GTA V/VI, Fortnite, Minecraft, CS2, LoL, Valorant, F1 25, GT7, Tekken 8
            $marqueeIds = [
                12026, 199224, 117836, 221441, 174739, 221545, 233062,
                220587, 235660, 260502, 92989, 142457, 220450, 274649, 257269,
                37470, 53320,
                181474, // Madden NFL 26
                122588, // Pax Dei
                53981,  // Everspace 2
                175409, // Clair Obscur: Expedition 33
            ];

            // 1. Fetch Marquee Games first
            $marqueeQuery = DB::table('video_games')
                ->join('video_games_ranked_mv', 'video_games.id', '=', 'video_games_ranked_mv.id')
                ->leftJoin('video_game_titles', 'video_games.video_game_title_id', '=', 'video_game_titles.id')
                ->leftJoin('video_game_title_sources', function ($join) {
                    $join->on('video_game_title_sources.video_game_title_id', '=', 'video_game_titles.id')
                        ->where('video_game_title_sources.provider', '=', 'igdb');
                })
                ->select([
                    'video_games.*',
                    'video_games_ranked_mv.image_url',
                    'video_games_ranked_mv.image_urls',
                    'video_games_ranked_mv.media as mv_media',
                    'video_game_titles.name as canonical_name',
                    'video_game_title_sources.raw_payload',
                    'video_game_title_sources.platform as source_platform',
                    'video_game_title_sources.genre as source_genre',
                    DB::raw("(SELECT json_agg(json_build_object(
                        'video_id', video_id, 
                        'name', COALESCE(title, primary_collection, 'Trailer'),
                        'type', COALESCE(primary_collection, 'trailer'),
                        'duration', duration
                    )) FROM videos WHERE videos.video_game_id = video_games.id) as all_videos"),
                ])
                ->whereIn('video_games.id', $marqueeIds)
                ->whereNotNull('video_game_title_sources.raw_payload');

            $marqueeGames = $marqueeQuery->get();

            // 2. Fetch Dynamic "High Quality" Games to fill remaining slots
            $remainingLimit = max(0, $limit - $marqueeGames->count());
            $dynamicGames = collect();

            if ($remainingLimit > 0) {
                $dynamicQuery = DB::table('video_games')
                    ->join('video_games_ranked_mv', 'video_games.id', '=', 'video_games_ranked_mv.id')
                    ->leftJoin('video_game_titles', 'video_games.video_game_title_id', '=', 'video_game_titles.id')
                    ->leftJoin('video_game_title_sources', function ($join) {
                        $join->on('video_game_title_sources.video_game_title_id', '=', 'video_game_titles.id')
                            ->where('video_game_title_sources.provider', '=', 'igdb');
                    })
                    ->select([
                        'video_games.*',
                        'video_games_ranked_mv.image_url',
                        'video_games_ranked_mv.image_urls',
                        'video_games_ranked_mv.media as mv_media',
                        'video_game_titles.name as canonical_name',
                        'video_game_title_sources.raw_payload',
                        'video_game_title_sources.platform as source_platform',
                        'video_game_title_sources.genre as source_genre',
                        DB::raw("(SELECT json_agg(json_build_object(
                        'video_id', video_id, 
                        'name', COALESCE(title, primary_collection, 'Trailer'),
                        'type', COALESCE(primary_collection, 'trailer'),
                        'duration', duration
                    )) FROM videos WHERE videos.video_game_id = video_games.id) as all_videos"),
                    ])
                    ->where('video_games.release_date', '>=', now()->subMonths(18))
                    ->where('video_games.release_date', '<=', now())
                    ->whereNotIn('video_games.id', $marqueeIds)
                    ->whereNotNull('video_game_title_sources.raw_payload');

                $dynamicGames = $this->applyPremiumFilter($dynamicQuery);
                $dynamicGames = $this->applyLandingRanking($dynamicGames)
                    ->limit($remainingLimit)
                    ->get();
            }

            // Merge and preserve order: Marquee first, then Dynamic
            $allGames = $marqueeGames->concat($dynamicGames);

            return $allGames->map(function ($game) {
                $mapped = $this->mapSpotlightGame($game);
                $mapped['raw_payload_debug'] = $game->raw_payload;

                return $mapped;
            })->toArray();
        });
    }

    private function mapSpotlightGame(object $game): array
    {
        // Parse media from raw_payload for high-res screenshots/artworks
        $rawPayload = property_exists($game, 'raw_payload') && $game->raw_payload
            ? (is_array($game->raw_payload) ? $game->raw_payload : json_decode((string) $game->raw_payload, true))
            : [];

        // Handle double-encoded JSON (common implementation quirk)
        if (is_string($rawPayload)) {
            $rawPayload = json_decode($rawPayload, true) ?? [];
        }

        $baseUrl = 'https://images.igdb.com/igdb/image/upload/';

        $coverUrl = null;
        if (! empty($rawPayload['cover'])) {
            // Check for direct image_id found in IGDB objects
            if (is_array($rawPayload['cover']) && ! empty($rawPayload['cover']['image_id'])) {
                $coverUrl = $baseUrl.'t_1080p/'.$rawPayload['cover']['image_id'].'.webp';
            } else {
                // Fallback for ID-only references
                $coverId = is_array($rawPayload['cover']) ? ($rawPayload['cover']['id'] ?? $rawPayload['cover']) : $rawPayload['cover'];
                if (is_numeric($coverId)) {
                    $coverUrl = $baseUrl.'t_1080p/co'.base_convert((string) $coverId, 10, 36).'.webp';
                } elseif (is_string($coverId) && ! empty($coverId)) {
                    $coverUrl = $baseUrl.'t_1080p/'.(str_starts_with($coverId, 'co') ? '' : 'co').$coverId.'.webp';
                }
            }
        }

        $screenshots = [];
        if (! empty($rawPayload['screenshots'])) {
            $items = is_string($rawPayload['screenshots']) ? explode(',', str_replace(['{', '}'], '', $rawPayload['screenshots'])) : $rawPayload['screenshots'];
            // Normalize to array of items
            $normalizedItems = is_array($items) ? $items : [];

            foreach (array_slice($normalizedItems, 0, 8) as $item) {
                if (is_array($item) && ! empty($item['image_id'])) {
                    $screenshots[] = ['url' => $baseUrl.'t_1080p/'.$item['image_id'].'.webp'];
                } elseif (is_numeric($item)) {
                    $screenshots[] = ['url' => $baseUrl.'t_1080p/sc'.base_convert((string) $item, 10, 36).'.webp'];
                } elseif (is_string($item) && ! empty($item)) {
                    $screenshots[] = ['url' => $baseUrl.'t_1080p/'.$item.'.webp'];
                }
            }
        }

        $artworks = [];
        if (! empty($rawPayload['artworks'])) {
            $items = is_string($rawPayload['artworks']) ? explode(',', str_replace(['{', '}'], '', $rawPayload['artworks'])) : $rawPayload['artworks'];
            $normalizedItems = is_array($items) ? $items : [];

            foreach (array_slice($normalizedItems, 0, 8) as $item) {
                if (is_array($item) && ! empty($item['image_id'])) {
                    $artworks[] = ['url' => $baseUrl.'t_1080p/'.$item['image_id'].'.webp'];
                } elseif (is_numeric($item)) {
                    $artworks[] = ['url' => $baseUrl.'t_1080p/ar'.base_convert((string) $item, 10, 36).'.webp'];
                } elseif (is_string($item) && ! empty($item)) {
                    $artworks[] = ['url' => $baseUrl.'t_1080p/'.$item.'.webp'];
                }
            }
        }

        // Extract all videos (trailers, gameplay, etc.)
        $trailers = [];
        $allVideos = property_exists($game, 'all_videos') ? $game->all_videos : null;
        if ($allVideos) {
            $videoList = is_array($allVideos) ? $allVideos : json_decode((string) $allVideos, true);
            if (is_array($videoList)) {
                foreach ($videoList as $v) {
                    if (! empty($v['video_id'])) {
                        $trailers[] = [
                            'video_id' => $v['video_id'],
                            'name' => $v['name'] ?? 'Trailer',
                            'type' => $v['type'] ?? 'trailer',
                            'url' => 'https://www.youtube.com/watch?v='.$v['video_id'],
                        ];
                    }
                }
            }
        }

        // Manual fallback for specific games (like Witcher 3)
        if ($game->id == 1014215 && empty($trailers)) {
            $trailers[] = [
                'video_id' => 'rIoPrbzI5Z4',
                'name' => 'The Witcher 3: Wild Hunt Trailer',
                'type' => 'trailer',
                'url' => 'https://www.youtube.com/watch?v=rIoPrbzI5Z4',
            ];
        }

        $reviewScore = (float) ($game->review_score ?? $game->rating ?? $game->mv_rating ?? 85);
        $verdict = match (true) {
            $reviewScore >= 90 => 'Masterpiece',
            $reviewScore >= 80 => 'Essential',
            $reviewScore >= 70 => 'Great',
            default => 'Strong',
        };

        $gallery = [];
        foreach ($trailers as $t) {
            $gallery[] = [
                'id' => Str::random(8),
                'type' => 'video',
                'url' => $t['video_id'],
                'source' => 'YouTube',
                'title' => $t['name'],
                'video_type' => $t['type'],
                'duration' => $t['duration'] ?? null,
            ];
        }
        foreach ($artworks as $a) {
            $gallery[] = ['id' => Str::random(8), 'type' => 'image', 'url' => $a['url'], 'source' => 'IGDB'];
        }
        foreach ($screenshots as $s) {
            $gallery[] = ['id' => Str::random(8), 'type' => 'image', 'url' => $s['url'], 'source' => 'IGDB'];
        }

        $sourcePlatform = property_exists($game, 'source_platform') ? $game->source_platform : null;
        $platforms = $sourcePlatform ? (is_string($sourcePlatform) ? json_decode($sourcePlatform, true) : $sourcePlatform) : [];

        // Prioritize cover for background as well if requested, but falling back to others for variety
        $backdropUrl = $coverUrl ?: (! empty($artworks) ? $artworks[0]['url'] : (! empty($screenshots) ? $screenshots[0]['url'] : null));
        $mainImage = $coverUrl ?: $backdropUrl;

        return [
            'id' => $game->id,
            'name' => $game->canonical_name ?? $game->name,
            'slug' => property_exists($game, 'slug') ? $game->slug : Str::slug($game->name),
            'image' => $mainImage ? $this->upscaleImage($mainImage, 't_1080p') : null,
            'background' => $backdropUrl ? $this->upscaleImage($backdropUrl, 't_1080p') : null,
            'platform_labels' => is_array($platforms) ? $platforms : [$platforms],
            'spotlight_score' => [
                'total' => round($reviewScore / 10, 1),
                'grade' => $reviewScore >= 90 ? 'S' : ($reviewScore >= 80 ? 'A' : 'B'),
                'verdict' => $verdict,
                'breakdown' => [
                    ['label' => 'Critical Reception', 'score' => (int) $reviewScore, 'summary' => 'Aggregated rating.', 'weight_percentage' => 40],
                    ['label' => 'Popularity', 'score' => (int) min($game->popularity_score ?? $game->mv_popularity_score ?? 0, 100), 'summary' => 'Market demand.', 'weight_percentage' => 30],
                    ['label' => 'Quality Proxy', 'score' => (int) min($game->rating_count ?? $game->mv_rating_count ?? 0, 100), 'summary' => 'Sentiment signals.', 'weight_percentage' => 30],
                ],
            ],
            'spotlight_gallery' => ! empty($gallery) ? $gallery : [
                ['id' => '1', 'type' => 'image', 'url' => $this->upscaleImage($mainImage, 't_1080p'), 'source' => 'IGDB'],
            ],
        ];
    }

    private function fetchTopRated(int $limit): Collection
    {
        return $this->cacheStore()->remember('landing:top-rated-v4', self::ROW_CACHE_TTL, function () use ($limit) {
            $query = $this->applyPremiumFilter($this->baseGameQuery());

            return $this->applyLandingRanking($query)
                ->limit($limit)
                ->get();
        });
    }

    private function fetchUpcoming(int $limit): Collection
    {
        return $this->cacheStore()->remember('landing:upcoming-v4', self::ROW_CACHE_TTL, function () use ($limit) {
            $query = $this->applyPremiumFilter(
                DB::table('video_games_upcoming_mv')->select($this->mvColumns())
            );

            return $this->applyLandingRanking($query)
                ->limit($limit)
                ->get();
        });
    }

    private function fetchNewReleases(int $limit): Collection
    {
        return $this->cacheStore()->remember('landing:new-releases-v4', self::ROW_CACHE_TTL, function () use ($limit) {
            $query = $this->applyPremiumFilter($this->baseGameQuery())
                ->whereNotNull('release_date')
                ->where('release_date', '<=', now())
                ->where('release_date', '>=', now()->subMonths(6)); // Slightly wider window for premium results

            return $this->applyLandingRanking($query)
                ->limit($limit)
                ->get();
        });
    }

    private function fetchMostReviewed(int $limit): Collection
    {
        return $this->cacheStore()->remember('landing:most-reviewed-v4', self::ROW_CACHE_TTL, function () use ($limit) {
            $query = $this->applyPremiumFilter($this->baseGameQuery());

            return $this->applyLandingRanking($query)
                ->limit($limit)
                ->get();
        });
    }

    private function selectHero(Collection $topRated, Collection $newReleases, Collection $mostReviewed): ?object
    {
        $candidates = $topRated
            ->merge($newReleases)
            ->merge($mostReviewed)
            ->filter(function ($game) {
                // Ensure properties exist before checking them to avoid undefined property errors
                $imageUrl = $game->image_url ?? null;
                $imageUrls = $game->image_urls ?? null;

                return ! empty($imageUrl) || ! empty($imageUrls);
            })
            ->sortByDesc(fn ($game) => $game->rating ?? 0)
            ->values();

        $preferredHero = $candidates->first(function ($game) {
            $name = Str::lower((string) ($game->name ?? ''));

            return Str::contains($name, 'zelda');
        });

        return $preferredHero ?? $candidates->first();
    }

    private function fetchZeldaHero(): ?array
    {
        return $this->cacheStore()->remember('landing:hero-ac-shadows-v1', self::ROW_CACHE_TTL, function () {
            $acGame = DB::table('video_games')
                ->join('video_games_ranked_mv', 'video_games.id', '=', 'video_games_ranked_mv.id')
                ->leftJoin('video_game_titles', 'video_games.video_game_title_id', '=', 'video_game_titles.id')
                ->leftJoin('video_game_title_sources', function ($join) {
                    $join->on('video_game_title_sources.video_game_title_id', '=', 'video_game_titles.id')
                        ->where('video_game_title_sources.provider', '=', 'igdb');
                })
                ->select([
                    'video_games.*',
                    'video_games_ranked_mv.image_url',
                    'video_games_ranked_mv.image_urls',
                    'video_games_ranked_mv.media as mv_media',
                    'video_game_titles.name as canonical_name',
                    'video_game_title_sources.raw_payload',
                    'video_game_title_sources.platform as source_platform',
                    'video_game_title_sources.genre as source_genre',
                    DB::raw("(SELECT json_agg(json_build_object(
                        'video_id', video_id, 
                        'name', COALESCE(title, primary_collection, 'Trailer'),
                        'type', COALESCE(primary_collection, 'trailer'),
                        'duration', duration
                    )) FROM videos WHERE videos.video_game_id = video_games.id) as all_videos"),
                ])
                ->whereNotNull('video_game_title_sources.raw_payload')
                ->where('video_games.rating', '>=', 60)
                ->whereRaw("lower(video_games.name) like '%assassin%creed%shadows%'")
                ->orderByRaw("case
                    when lower(video_games.name) = 'assassin''s creed shadows' then 0
                    when lower(video_games.name) like '%assassin%creed%shadows%deluxe%' then 1
                    when lower(video_games.name) like '%assassin%creed%shadows%gold%' then 2
                    else 3
                end asc")
                ->orderByRaw('video_games.release_date desc nulls last')
                ->orderByDesc('video_games.rating')
                ->limit(1)
                ->first();

            return $acGame ? $this->mapSpotlightGame($acGame) : null;
        });
    }

    /**
     * @return array{games: Collection, pricing: array<int, array<string, mixed>>}
     */
    private function fetchBestDeals(int $limit): array
    {
        return $this->cacheStore()->remember('landing:best-deals-v3', self::ROW_CACHE_TTL, function () use ($limit) {
            $pricingMap = $this->buildPricingMapFromQuery($this->latestPriceQuery());

            $sorted = collect($pricingMap)
                ->filter(fn ($pricing) => $pricing['btc_price'] !== null)
                ->sortBy('btc_price');

            $ids = $sorted->keys()->all();

            $games = $ids === []
                ? collect()
                : $this->applyPremiumFilter($this->baseGameQuery())
                    ->whereIn('id', $ids)
                    ->get()
                    ->sortBy(fn ($game) => array_search($game->id, $ids, true))
                    ->take($limit)
                    ->values();

            $finalIds = $games->pluck('id')->toArray();
            $finalPricing = $sorted->only($finalIds)->toArray();

            return [
                'games' => $games,
                'pricing' => $finalPricing,
            ];
        });
    }

    /**
     * @return array<int, array{genre: string, title: string, games: Collection}>
     */
    private function fetchGenreRows(int $limit): array
    {
        return $this->cacheStore()->remember('landing:genre-rows-v7', self::ROW_CACHE_TTL, function () use ($limit) {
            $targetGenres = [
                ['genre' => "Hack and slash/Beat 'em up", 'key' => 'action-adventure', 'title' => 'Action & Adventure'],
                ['genre' => 'Role-playing (RPG)', 'key' => Str::slug('Role-playing (RPG)'), 'title' => 'Top RPGs'],
                ['genre' => 'Shooter', 'key' => Str::slug('Shooter'), 'title' => 'FPS & Shooters'],
                ['genre' => 'Strategy', 'key' => Str::slug('Strategy'), 'title' => 'Strategy Games'],
                ['genre' => 'Adventure', 'key' => Str::slug('Adventure'), 'title' => 'Story & Adventure'],
                ['genre' => 'Racing', 'key' => Str::slug('Racing'), 'title' => 'Racing & Speed'],
                ['genre' => 'Sport', 'key' => Str::slug('Sport'), 'title' => 'Sports'],
                ['genre' => 'Simulator', 'key' => Str::slug('Simulator'), 'title' => 'Simulators'],
                ['genre' => 'Fighting', 'key' => Str::slug('Fighting'), 'title' => 'Fighting Games'],
                ['genre' => 'Puzzle', 'key' => Str::slug('Puzzle'), 'title' => 'Puzzle & Brain'],
                ['genre' => 'Indie', 'key' => Str::slug('Indie'), 'title' => 'Indie Gems'],
                ['genre' => 'Arcade', 'key' => Str::slug('Arcade'), 'title' => 'Arcade Classics'],
            ];

            $rows = [];
            foreach ($targetGenres as $rowSpec) {
                $genreName = $rowSpec['genre'];
                $rowKey = $rowSpec['key'];
                $displayTitle = $rowSpec['title'];
                $query = $this->applyPremiumFilter(
                    DB::table('video_games_genre_ranked_mv')
                        ->select($this->mvColumns())
                        ->where('genre_name', $genreName)
                );

                $query
                    ->whereBetween('release_date', [
                        self::LANDING_FEATURED_YEAR_MIN.'-01-01',
                        self::LANDING_FEATURED_YEAR_MAX.'-12-31',
                    ])
                    // Prefer cover art. Some sources (e.g. RAWG) store covers only inside `media`.
                    ->where(function (Builder $q): void {
                        $q->where('image_url', 'like', '%/co%')
                            ->orWhere('image_urls', 'like', '%/co%')
                            ->orWhere('media', 'like', '%"role":"cover"%');
                    })
                    ->whereNotNull('rating')
                    ->where('rating', '>=', self::LANDING_FEATURED_MIN_RATING)
                    ->where('rating_count', '>=', self::LANDING_FEATURED_MIN_RATING_COUNT)
                    ->whereExists(function (Builder $sub) {
                        $sub->selectRaw('1')
                            ->from('video_game_prices as p')
                            ->whereColumn('p.video_game_id', 'video_games_genre_ranked_mv.id')
                            ->where('p.is_active', true)
                            ->whereNotNull('p.currency')
                            ->where('p.amount_minor', '>=', 0);
                    });

                // For landing rows: recency-first within genre.
                $games = $query
                    ->orderByDesc('release_date')
                    ->orderByDesc('rating')
                    ->orderByDesc('rating_count')
                    ->orderByDesc('popularity_score')
                    ->limit($limit)
                    ->get();

                // If the strict threshold is too sparse for a genre, widen slightly to fill the row.
                if ($games->count() < $limit && self::LANDING_FEATURED_FALLBACK_MIN_RATING < self::LANDING_FEATURED_MIN_RATING) {
                    $existingIds = $games->pluck('id')->all();

                    $fallback = $this->applyPremiumFilter(
                        DB::table('video_games_genre_ranked_mv')
                            ->select($this->mvColumns())
                            ->where('genre_name', $genreName)
                            ->whereBetween('release_date', [
                                self::LANDING_FEATURED_YEAR_MIN.'-01-01',
                                self::LANDING_FEATURED_YEAR_MAX.'-12-31',
                            ])
                            ->where(function (Builder $q): void {
                                $q->where('image_url', 'like', '%/co%')
                                    ->orWhere('image_urls', 'like', '%/co%')
                                    ->orWhere('media', 'like', '%"role":"cover"%');
                            })
                            ->whereNotNull('rating')
                            ->where('rating', '>=', self::LANDING_FEATURED_FALLBACK_MIN_RATING)
                            ->where('rating_count', '>=', self::LANDING_FEATURED_MIN_RATING_COUNT)
                            ->whereExists(function (Builder $sub) {
                                $sub->selectRaw('1')
                                    ->from('video_game_prices as p')
                                    ->whereColumn('p.video_game_id', 'video_games_genre_ranked_mv.id')
                                    ->where('p.is_active', true)
                                    ->whereNotNull('p.currency')
                                    ->where('p.amount_minor', '>=', 0);
                            })
                    )
                        ->when($existingIds !== [], fn (Builder $q) => $q->whereNotIn('id', $existingIds))
                        ->orderByDesc('release_date')
                        ->orderByDesc('rating')
                        ->orderByDesc('rating_count')
                        ->orderByDesc('popularity_score')
                        ->limit($limit - $games->count())
                        ->get();

                    if ($fallback->isNotEmpty()) {
                        $games = $games->concat($fallback);
                    }
                }

                if ($games->isEmpty()) {
                    continue;
                }

                $rows[] = [
                    'key' => $rowKey,
                    'title' => $displayTitle,
                    'games' => $games,
                ];
            }

            return $rows;
        });
    }

    private function mvColumns(): array
    {
        return [
            'id',
            'name',
            'rating',
            'release_date',
            'rating_count',
            'canonical_name',
            'media',
            'image_urls',
            'image_url',
            'review_score',
            'popularity_score',
        ];
    }

    private function baseGameQuery(): Builder
    {
        return DB::table('video_games_ranked_mv')
            ->select($this->mvColumns());
    }

    /**
     * Apply landing page ranking logic:
     * Popularity > Rating Count > Quality > Recency
     * Optimized version without expensive JSON operations
     */
    private function applyLandingRanking(Builder $query): Builder
    {
        // Determine table prefix more efficiently
        $from = $query->from ?? '';
        $prefix = match (true) {
            str_contains($from, 'video_games_upcoming_mv') => 'video_games_upcoming_mv.',
            str_contains($from, 'video_games_genre_ranked_mv') => 'video_games_genre_ranked_mv.',
            default => 'video_games_ranked_mv.'  // Default to ranked MV which is safer for joined queries
        };

        return $query
            ->orderByDesc($prefix.'popularity_score')
            ->orderByDesc($prefix.'rating_count')
            ->orderByDesc($prefix.'rating')
            ->orderByDesc($prefix.'release_date');
    }

    /**
     * Strict filters to ensure only high-quality "Premium" games are shown.
     * Optimized version with efficient table detection
     */
    private function applyPremiumFilter(Builder $query): Builder
    {
        // Determine table prefix efficiently
        $from = $query->from ?? '';
        $isUpcoming = str_contains($from, 'video_games_upcoming_mv');

        $prefix = match (true) {
            $isUpcoming => 'video_games_upcoming_mv.',
            str_contains($from, 'video_games_genre_ranked_mv') => 'video_games_genre_ranked_mv.',
            default => 'video_games_ranked_mv.' // Default to ranked MV which is safer for joined queries
        };

        if ($isUpcoming) {
            // For upcoming games, we can't filter by rating yet.
            // We ensure they have at least one image to maintain the premium visual.
            return $query->whereNotNull($prefix.'name')
                ->where(function ($q) use ($prefix) {
                    $q->whereNotNull($prefix.'image_url')
                        ->orWhereNotNull($prefix.'image_urls');
                });
        }

        return $query
            ->whereNotNull($prefix.'rating')
            ->where($prefix.'rating', '>=', 60)
            ->where($prefix.'rating_count', '>=', 5);
    }

    private function latestPriceQuery(): Builder
    {
        // Use PostgreSQL DISTINCT ON for much faster "latest per group" lookups than ROW_NUMBER()
        return DB::table('video_game_prices')
            ->select([
                'video_game_id',
                'currency',
                'amount_minor',
                DB::raw('COALESCE(country_code, region_code) as country_code'),
                'recorded_at',
                'retailer',
            ])
            ->distinct('video_game_id')
            ->whereNotNull('currency')
            ->where('amount_minor', '>=', 0)
            ->orderBy('video_game_id')
            ->orderByDesc('recorded_at');
    }

    /**
     * @param  Collection<int, mixed>  $topRated
     * @param  Collection<int, mixed>  $newReleases
     * @param  Collection<int, mixed>  $mostReviewed
     * @param  Collection<int, mixed>  $bestDeals
     * @param  array<int, array{genre: string, title: string, games: Collection}>  $genreRows
     * @return array<int>
     */
    private function collectDisplayIds(Collection $topRated, Collection $newReleases, Collection $mostReviewed, Collection $bestDeals, array $genreRows): array
    {
        $ids = collect()
            ->merge($topRated->pluck('id'))
            ->merge($newReleases->pluck('id'))
            ->merge($mostReviewed->pluck('id'))
            ->merge($bestDeals->pluck('id'));

        foreach ($genreRows as $row) {
            $ids = $ids->merge($row['games']->pluck('id'));
        }

        return $ids->unique()->values()->all();
    }

    /**
     * Fetch curated top lists from provider data
     *
     * @return array<int, array{key: string, title: string, games: Collection}>
     */
    private function fetchProviderTopLists(): array
    {
        return $this->cacheStore()->remember('landing:provider-top-lists-v1', self::ROW_CACHE_TTL, function () {
            // Get the most recent top lists
            $topLists = DB::table('provider_toplists')
                ->select('id', 'provider_key', 'list_key', 'name')
                ->whereIn('list_key', ['popular', 'top_rated', 'top-rated', 'new_releases', 'new-releases', 'upcoming'])
                ->orderByRaw("
                    CASE 
                        WHEN list_key = 'popular' THEN 0 
                        WHEN list_key IN ('top-rated', 'top_rated') THEN 1
                        WHEN list_key IN ('new-releases', 'new_releases') THEN 2
                        WHEN list_key = 'upcoming' THEN 3
                        ELSE 4 
                    END
                ")
                ->orderByDesc('snapshot_at')
                ->limit(5)
                ->get();

            $results = [];

            foreach ($topLists as $list) {
                // Get games for this list
                $games = DB::table('provider_toplist_items as pti')
                    ->join('video_games_ranked_mv as vg', 'pti.video_game_id', '=', 'vg.id')
                    ->where('pti.provider_toplist_id', $list->id)
                    ->where('vg.rating', '>=', 60)
                    ->select(array_map(fn ($col) => "vg.{$col}", $this->mvColumns()))
                    ->orderBy('pti.rank')
                    ->limit(self::ROW_LIMIT)
                    ->get();

                if ($games->isNotEmpty()) {
                    $results[] = [
                        'key' => $list->list_key,
                        'title' => $this->formatListTitle($list->name, $list->list_key),
                        'games' => $games,
                    ];
                }
            }

            // If we don't have enough lists, fall back to some genre-based lists
            if (count($results) < 3) {
                $genreRows = $this->fetchGenreRows(self::ROW_LIMIT);
                foreach ($genreRows as $genreRow) {
                    if (count($results) >= 5) {
                        break;
                    }
                    $results[] = $genreRow;
                }
            }

            return $results;
        });
    }

    /**
     * Format list title for display
     */
    private function formatListTitle(string $name, string $key): string
    {
        // Clean up provider-specific naming
        $title = str_replace(['IGDB ', 'RAWG ', 'Giant Bomb '], '', $name);

        // Use friendly names for common keys
        return match ($key) {
            'popular' => '⭐ Most Popular',
            'top-rated', 'top_rated' => '🏆 Top Rated',
            'new-releases', 'new_releases' => '🆕 New Releases',
            'upcoming' => '🎮 Coming Soon',
            default => $title
        };
    }

    /**
     * @param  array<int>  $gameIds
     * @return array<int, array<string, mixed>>
     */
    private function buildPricingMapForIds(array $gameIds): array
    {
        if ($gameIds === []) {
            return [];
        }

        $query = $this->latestPriceQuery()->whereIn('video_game_id', $gameIds);

        return $this->buildPricingMapFromQuery($query);
    }

    /**
     * @return array<int, array<string, mixed>>
     */
    private function buildPricingMapFromQuery(Builder $query): array
    {
        $prices = $query->get();
        $countryCurrencies = $this->countryCurrencyMap();
        $rateCache = [];
        $pricingMap = [];

        foreach ($prices as $price) {
            $gameId = (int) $price->video_game_id;
            $amountMinor = (int) $price->amount_minor;
            $localCurrency = $this->resolveLocalCurrency($price, $countryCurrencies);
            $rateResult = $this->getRateForCurrency($localCurrency, $rateCache);
            $primaryRate = $rateResult['rates'][0]['close'] ?? null;

            if ($primaryRate === null) {
                continue;
            }

            $amountMajor = $amountMinor / 100;
            $btcPrice = $primaryRate > 0 ? $amountMajor / $primaryRate : null;

            if (! isset($pricingMap[$gameId]) || ($btcPrice !== null && $btcPrice < ($pricingMap[$gameId]['btc_price'] ?? INF))) {
                $pricingMap[$gameId] = [
                    'amount_minor' => $amountMinor,
                    'amount_major' => $amountMajor,
                    'currency' => $price->currency,
                    'local_currency' => $localCurrency,
                    'country_code' => $price->country_code,
                    'retailer' => $price->retailer,
                    'recorded_at' => $price->recorded_at,
                    'btc_price' => $btcPrice,
                    'fallback' => $rateResult['fallback'],
                    'requested_currency' => $rateResult['requested_currency'],
                    'exchange_rates' => $rateResult['rates'],
                    'is_free' => $amountMinor === 0,
                ];
            }
        }

        return $pricingMap;
    }

    /**
     * @return array<string, string>
     */
    private function countryCurrencyMap(): array
    {
        return $this->cacheStore()->remember('landing:country-currency', self::COUNTRY_CURRENCY_CACHE_TTL, function () {
            return Country::query()
                ->with('currency:id,code')
                ->get()
                ->filter(fn (Country $country) => $country->currency)
                ->mapWithKeys(function (Country $country) {
                    return [strtoupper($country->code) => strtoupper($country->currency->code)];
                })
                ->toArray();
        });
    }

    /**
     * @param  array<string, string>  $countryCurrencies
     */
    private function resolveLocalCurrency(object $price, array $countryCurrencies): string
    {
        $countryCode = $price->country_code ? strtoupper((string) $price->country_code) : null;

        if ($countryCode && isset($countryCurrencies[$countryCode])) {
            return $countryCurrencies[$countryCode];
        }

        return strtoupper((string) $price->currency);
    }

    /**
     * @param  array<string, array<string, mixed>>  $rateCache
     * @return array{currency: string, requested_currency: string, fallback: bool, rates: array<int, array{symbol: string, exchange: string, close: float, volume_quote: float, currency: string}>}
     */
    private function getRateForCurrency(string $currency, array &$rateCache): array
    {
        $normalized = strtoupper($currency);

        if (! isset($rateCache[$normalized])) {
            $rateCache[$normalized] = $this->tradingViewClient->getBtcRates($normalized, false);
        }

        return $rateCache[$normalized];
    }

    /**
     * @param  array<int, mixed>  $pricingMap
     * @return array<int, array<string, mixed>>
     */
    private function mapGames(Collection $games, array $pricingMap, bool $includePricing): array
    {
        return $games->map(function ($game) use ($pricingMap, $includePricing) {
            return $this->mapGame($game, $pricingMap, $includePricing);
        })->values()->toArray();
    }

    /**
     * @param  array<int, mixed>  $pricingMap
     * @return array<string, mixed>
     */
    private function mapGame(object $game, array $pricingMap, bool $includePricing): array
    {
        $media = $this->normalizeMedia(
            $game->media ?? null,
            $game->image_urls ?? null,
            $game->image_url ?? null
        );
        $pricing = $includePricing ? ($pricingMap[$game->id] ?? null) : null;

        $genres = property_exists($game, 'genre')
            ? $this->normalizeGenres($game->genre)
            : (property_exists($game, 'genre_name') ? [$game->genre_name] : []);

        return [
            'id' => $game->id,
            'name' => $game->name,
            'canonical_name' => $game->canonical_name,
            'rating' => $game->rating,
            'release_date' => $game->release_date,
            'genres' => $genres,
            'media' => $media,
            'pricing' => $pricing,
        ];
    }

    /**
     * @return array<string, mixed>
     */
    private function normalizeMedia(?string $mediaJson, ?string $imageUrlsJson, ?string $imageUrl): array
    {
        $media = $mediaJson ? json_decode($mediaJson, true) : [];
        $media = is_array($media) ? $media : [];

        $imageUrls = $imageUrlsJson ? json_decode($imageUrlsJson, true) : [];
        $imageUrls = is_array($imageUrls) ? $imageUrls : [];

        // Prefer true cover images (co*) over screenshots/artworks.
        $coverUrl = $this->findCoverVariant($imageUrls, 't_1080p')
            ?? $this->findCoverVariant($imageUrls, 't_720p')
            ?? $this->findCoverVariant($imageUrls, 't_cover_big')
            ?? $this->findImageVariant($imageUrls, 't_1080p')
            ?? $this->findImageVariant($imageUrls, 't_720p')
            ?? $imageUrl;

        // RAWG/MV sometimes provides screenshots as primary image_url; prefer cover from media JSON if available.
        if (is_string($coverUrl) && str_contains($coverUrl, 'media.rawg.io/media/screenshots')) {
            $mediaCover = $this->coverFromMedia($media);
            if ($mediaCover) {
                $coverUrl = $mediaCover;
            }
        }

        $coverThumb = $this->findImageVariant($imageUrls, 't_cover_big')
            ?? $this->findImageVariant($imageUrls, 't_thumb')
            ?? $coverUrl;

        if (! $coverUrl) {
            $coverUrl = $this->coverFromMedia($media);
            $coverThumb = $coverThumb ?? $coverUrl;
        }

        // Upscale cover URL for better quality and ensure HTTPS
        $coverUrl = $this->upscaleImage($coverUrl, 't_1080p');
        $coverThumb = $this->upscaleImage($coverThumb, 't_cover_big');

        $screenshots = $this->screenshotsFromMedia($media);
        $artworks = $this->artworksFromMedia($media);
        $trailers = $this->videosFromMedia($media);

        return [
            'cover' => $coverUrl ? [
                'url' => $coverUrl,
                'width' => 1080,
                'height' => 1440,
            ] : null,
            'cover_url' => $coverUrl,
            'cover_url_high_res' => $coverUrl,
            'cover_url_thumb' => $coverThumb,
            'hero_url' => $artworks[0]['url'] ?? $screenshots[0]['url'] ?? $coverUrl,
            'screenshots' => $screenshots,
            'artworks' => $artworks,
            'trailers' => $trailers,
        ];
    }

    private function findCoverVariant(array $urls, string $size): ?string
    {
        foreach ($urls as $url) {
            if (! is_string($url)) {
                continue;
            }

            if (! str_contains($url, "/{$size}/")) {
                continue;
            }

            // IGDB covers use co* ids; screenshots use sc*; artworks use ar*.
            if (str_contains($url, '/co')) {
                return $url;
            }
        }

        return null;
    }

    /**
     * @return array<int, array{url: string, width: int, height: int}>
     */
    private function artworksFromMedia(array $media): array
    {
        $images = Arr::get($media, 'images', []);

        if (! is_array($images)) {
            return [];
        }

        $artworks = [];

        foreach ($images as $image) {
            if (! is_array($image)) {
                continue;
            }

            if (($image['role'] ?? '') !== 'artwork' || empty($image['url'])) {
                continue;
            }

            $artworks[] = [
                'url' => $image['url'],
                'width' => 1920,
                'height' => 1080,
            ];
        }

        return $artworks;
    }

    private function findImageVariant(array $urls, string $size): ?string
    {
        foreach ($urls as $url) {
            if (! is_string($url)) {
                continue;
            }

            if (str_contains($url, "/{$size}/")) {
                return $url;
            }
        }

        return null;
    }

    private function coverFromMedia(array $media): ?string
    {
        $images = Arr::get($media, 'images', []);

        foreach ($images as $image) {
            if (($image['role'] ?? null) === 'cover' && ! empty($image['url'])) {
                return $image['url'];
            }
        }

        $firstImage = $images[0]['url'] ?? null;

        return $firstImage ?: null;
    }

    /**
     * @return array<int, array{url: string, width: int, height: int}>
     */
    private function screenshotsFromMedia(array $media): array
    {
        $images = Arr::get($media, 'images', []);

        // Ensure $images is an array
        if (! is_array($images)) {
            return [];
        }

        $screenshots = [];

        foreach ($images as $image) {
            if (! is_array($image)) {
                continue;
            }

            if (($image['role'] ?? '') !== 'screenshot' || empty($image['url'])) {
                continue;
            }

            $screenshots[] = [
                'url' => $image['url'],
                'width' => 1920,
                'height' => 1080,
            ];
        }

        return $screenshots;
    }

    /**
     * @return array<int, array{url?: string, thumbnail?: string, name?: string, video_id?: string}>
     */
    private function videosFromMedia(array $media): array
    {
        $videos = Arr::get($media, 'videos', []);

        // Ensure $videos is an array
        if (! is_array($videos)) {
            return [];
        }

        $trailers = [];

        foreach ($videos as $video) {
            if (! is_array($video)) {
                continue;
            }

            if (empty($video['url']) && empty($video['video_id'])) {
                continue;
            }

            $trailers[] = [
                'url' => $video['url'] ?? null,
                'thumbnail' => $video['thumbnail'] ?? null,
                'name' => $video['name'] ?? 'Trailer',
                'video_id' => $video['video_id'] ?? null,
            ];
        }

        return $trailers;
    }

    /**
     * @return array<int, string>
     */
    private function normalizeGenres(mixed $genres): array
    {
        if ($genres === null) {
            return [];
        }

        if (is_string($genres)) {
            $decoded = json_decode($genres, true);
            $genres = is_array($decoded) ? $decoded : [$genres];
        }

        if (! is_array($genres)) {
            return [];
        }

        return collect($genres)
            ->map(fn ($genre) => is_array($genre) ? Arr::get($genre, 'name') : $genre)
            ->filter()
            ->map(fn ($genre) => trim((string) $genre))
            ->filter()
            ->unique()
            ->values()
            ->all();
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

    private function cacheStore(): CacheRepository
    {
        if (config('cache.default') === 'redis') {
            return Cache::store('redis');
        }

        return Cache::store();
    }
}
