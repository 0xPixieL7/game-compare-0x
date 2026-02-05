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
    private const ROW_LIMIT = 20;
    private const LANDING_FEATURED_YEAR_MIN = 2015;
    private const LANDING_FEATURED_YEAR_MAX = 2025;
    private const LANDING_FEATURED_MIN_RATING = 85;
    private const LANDING_FEATURED_FALLBACK_MIN_RATING = 80;
    private const LANDING_FEATURED_MIN_RATING_COUNT = 5;
    private const COUNTRY_CURRENCY_CACHE_TTL = 3600;
    private const ROW_CACHE_TTL = 14400;

    public function __construct(private readonly TradingViewClient $tradingViewClient) {}

    public function index(Request $request): Response
    {
        set_time_limit(120);
        $isAuthenticated = $request->user() !== null;
        $genreRows = $this->fetchGenreRows(self::ROW_LIMIT);
        $bestDealsData = $this->fetchBestDeals(self::ROW_LIMIT);

        $rows = [];
        $displayIds = [];

        foreach ($genreRows as $row) {
            $displayIds = array_merge($displayIds, $row['games']->pluck('id')->all());
            $rows[] = ['id' => $row['key'], 'title' => $row['title'], 'games' => $row['games']];
        }

        if ($deals = $bestDealsData['games'] ?? null) {
            $displayIds = array_merge($displayIds, $deals->pluck('id')->all());
            $rows[] = ['id' => 'best-deals', 'title' => 'Best Deals', 'games' => $deals];
        }

        $pricingMap = array_replace($isAuthenticated ? $this->buildPricingMapForIds(array_unique($displayIds)) : [], $bestDealsData['pricing'] ?? []);

        foreach ($rows as &$row) {
            $row['games'] = $this->mapGames($row['games'], $pricingMap, $isAuthenticated);
        }

        $spotlightGames = $this->fetchSpotlightGames(20);
        if ($zeldaHero = $this->fetchZeldaHero()) {
            $spotlightGames = collect($spotlightGames)->reject(fn ($g) => $g['id'] === $zeldaHero['id'])->prepend($zeldaHero)->values()->all();
        }

        return Inertia::render('welcome', [
            'hero' => $spotlightGames[0] ?? null,
            'spotlightGames' => $spotlightGames,
            'rows' => Inertia::defer(fn () => $rows),
            'landingStats' => Inertia::defer(fn () => $this->fetchLandingStats()),
            'cta' => ['pricing' => 'Join free for price data'],
        ]);
    }

    private function fetchLandingStats(): array
    {
        return $this->cacheStore()->remember('landing:stats-v1', now()->addMinutes(10), function (): array {
            $baseQuery = DB::table('video_game_prices')->where('is_active', true);

            return [
                'active_prices' => (int) $baseQuery->count(),
                'priced_games' => (int) $baseQuery->distinct('video_game_id')->count('video_game_id'),
                'markets' => (int) $baseQuery->distinct('country_code')->count('country_code'),
            ];
        });
    }

    public function debugSpotlight()
    {
        return response()->json($this->fetchSpotlightGames(20));
    }

    private function fetchSpotlightGames(int $limit = 6): array
    {
        return $this->cacheStore()->remember('landing:spotlight-games-v19', self::ROW_CACHE_TTL, function () use ($limit) {
            $marqueeIds = [12026, 199224, 117836, 221441, 174739, 221545, 233062, 220587, 235660, 260502, 92989, 142457, 220450, 274649, 257269, 37470, 53320, 181474, 122588, 53981, 175409];

            $baseQuery = fn () => DB::table('video_games')
                ->join('video_games_ranked_mv', 'video_games.id', '=', 'video_games_ranked_mv.id')
                ->leftJoin('video_game_titles', 'video_games.video_game_title_id', '=', 'video_game_titles.id')
                ->leftJoin('video_game_title_sources', fn ($j) => $j->on('video_game_title_sources.video_game_title_id', '=', 'video_game_titles.id')->where('video_game_title_sources.provider', 'igdb'))
                ->select(['video_games.*', 'video_games_ranked_mv.image_url', 'video_games_ranked_mv.image_urls', 'video_games_ranked_mv.media as mv_media', 'video_game_titles.name as canonical_name', 'video_game_title_sources.raw_payload', 'video_game_title_sources.platform as source_platform', 'video_game_title_sources.genre as source_genre', DB::raw("(SELECT json_agg(json_build_object('video_id', video_id, 'name', COALESCE(title, primary_collection, 'Trailer'), 'type', COALESCE(primary_collection, 'trailer'), 'duration', duration)) FROM videos WHERE videos.video_game_id = video_games.id) as all_videos")])
                ->whereNotNull('video_game_title_sources.raw_payload');

            $marqueeGames = $baseQuery()->whereIn('video_games.id', $marqueeIds)->get();
            $dynamicGames = collect();

            if (($rem = $limit - $marqueeGames->count()) > 0) {
                $query = $baseQuery()
                    ->where('video_games.release_date', '>=', now()->subMonths(18))
                    ->where('video_games.release_date', '<=', now())
                    ->whereNotIn('video_games.id', $marqueeIds);
                $dynamicGames = $this->applyLandingRanking($this->applyPremiumFilter($query))->limit($rem)->get();
            }

            return $marqueeGames->concat($dynamicGames)->map(function ($game) {
                return array_merge($this->mapSpotlightGame($game), ['raw_payload_debug' => $game->raw_payload]);
            })->toArray();
        });
    }

    private function mapSpotlightGame(object $game): array
    {
        $rawPayload = is_string($payload = $game->raw_payload ?? []) ? json_decode($payload, true) : $payload;
        if (is_string($rawPayload)) { $rawPayload = json_decode($rawPayload, true) ?? []; }

        $baseUrl = 'https://images.igdb.com/igdb/image/upload/';
        $coverUrl = null;

        if ($cover = $rawPayload['cover'] ?? null) {
            $coverId = is_array($cover) ? ($cover['image_id'] ?? $cover['id'] ?? $cover) : $cover;
            if (is_numeric($coverId)) {
                $coverUrl = $baseUrl.'t_1080p/co'.base_convert((string) $coverId, 10, 36).'.webp';
            } elseif (is_string($coverId) && ! empty($coverId)) {
                $coverUrl = $baseUrl.'t_1080p/'.(str_starts_with($coverId, 'co') ? '' : 'co').$coverId.'.webp';
            }
        }

        $screenshots = [];
        if ($ss = $rawPayload['screenshots'] ?? null) {
            $items = is_string($ss) ? explode(',', str_replace(['{', '}'], '', $ss)) : $ss;
            foreach (array_slice((array) $items, 0, 8) as $item) {
                $id = is_array($item) ? ($item['image_id'] ?? null) : $item;
                if ($id) {
                    $prefix = is_numeric($id) ? 'sc'.base_convert((string) $id, 10, 36) : $id;
                    $screenshots[] = ['url' => $baseUrl.'t_1080p/'.$prefix.'.webp'];
                }
            }
        }

        $artworks = [];
        if ($aw = $rawPayload['artworks'] ?? null) {
            $items = is_string($aw) ? explode(',', str_replace(['{', '}'], '', $aw)) : $aw;
            foreach (array_slice((array) $items, 0, 8) as $item) {
                $id = is_array($item) ? ($item['image_id'] ?? null) : $item;
                if ($id) {
                    $prefix = is_numeric($id) ? 'ar'.base_convert((string) $id, 10, 36) : $id;
                    $artworks[] = ['url' => $baseUrl.'t_1080p/'.$prefix.'.webp'];
                }
            }
        }

        $trailers = [];
        $videoList = is_string($vids = $game->all_videos ?? null) ? json_decode($vids, true) : $vids;
        foreach ((array) $videoList as $v) {
            if ($vid = $v['video_id'] ?? null) {
                $trailers[] = [
                    'video_id' => $vid,
                    'name' => $v['name'] ?? 'Trailer',
                    'type' => $v['type'] ?? 'trailer',
                    'url' => "https://www.youtube.com/watch?v={$vid}",
                ];
            }
        }

        if ($game->id == 1014215 && empty($trailers)) {
            $trailers[] = ['video_id' => 'rIoPrbzI5Z4', 'name' => 'The Witcher 3: Wild Hunt Trailer', 'type' => 'trailer', 'url' => 'https://www.youtube.com/watch?v=rIoPrbzI5Z4'];
        }

        $score = (float) ($game->review_score ?? $game->rating ?? $game->mv_rating ?? 85);
        $gallery = array_merge(
            collect($trailers)->map(fn ($t) => ['id' => Str::random(8), 'type' => 'video', 'url' => $t['video_id'], 'source' => 'YouTube', 'title' => $t['name'], 'video_type' => $t['type'], 'duration' => $t['duration'] ?? null])->all(),
            collect(array_merge($artworks, $screenshots))->map(fn ($item) => ['id' => Str::random(8), 'type' => 'image', 'url' => $item['url'], 'source' => 'IGDB'])->all()
        );

        $backdropUrl = $coverUrl ?: ($artworks[0]['url'] ?? $screenshots[0]['url'] ?? null);
        $mainImage = $coverUrl ?: $backdropUrl;
        $theme = (is_string($attr = $game->attributes ?? null) ? json_decode($attr, true) : $attr)['theme'] ?? null;

        return [
            'id' => $game->id,
            'name' => $game->canonical_name ?? $game->name,
            'slug' => $game->slug ?? Str::slug($game->name),
            'image' => $this->upscaleImage($mainImage, 't_1080p'),
            'background' => $this->upscaleImage($backdropUrl, 't_1080p'),
            'platform_labels' => (array) (is_string($sp = $game->source_platform ?? null) ? json_decode($sp, true) : $sp),
            'theme' => $theme,
            'media' => [
                'cover_url' => $coverUrl,
                'cover_url_high_res' => $mainImage,
                'screenshots' => array_column($screenshots, 'url'),
                'artworks' => array_column($artworks, 'url'),
                'trailers' => $trailers,
            ],
            'spotlight_score' => [
                'total' => round($score / 10, 1),
                'grade' => $score >= 90 ? 'S' : ($score >= 80 ? 'A' : 'B'),
                'verdict' => match (true) { $score >= 90 => 'Masterpiece', $score >= 80 => 'Essential', $score >= 70 => 'Great', default => 'Strong' },
                'breakdown' => [
                    ['label' => 'Critical Reception', 'score' => (int) $score, 'summary' => 'Aggregated rating.', 'weight_percentage' => 40],
                    ['label' => 'Popularity', 'score' => (int) min($game->popularity_score ?? $game->mv_popularity_score ?? 0, 100), 'summary' => 'Market demand.', 'weight_percentage' => 30],
                    ['label' => 'Quality Proxy', 'score' => (int) min($game->rating_count ?? $game->mv_rating_count ?? 0, 100), 'summary' => 'Sentiment signals.', 'weight_percentage' => 30],
                ],
            ],
            'spotlight_gallery' => $gallery ?: [['id' => '1', 'type' => 'image', 'url' => $this->upscaleImage($mainImage, 't_1080p'), 'source' => 'IGDB']],
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
        $candidates = $topRated->merge($newReleases)->merge($mostReviewed)
            ->filter(fn ($game) => ! empty($game->image_url) || ! empty($game->image_urls))
            ->sortByDesc(fn ($game) => $game->rating ?? 0)
            ->values();

        return $candidates->first(fn ($game) => Str::contains(Str::lower((string) ($game->name ?? '')), 'zelda'))
            ?? $candidates->first();
    }

    private function fetchZeldaHero(): ?array
    {
        return $this->cacheStore()->remember('landing:hero-ac-shadows-v1', self::ROW_CACHE_TTL, function () {
            $acGame = DB::table('video_games')
                ->join('video_games_ranked_mv', 'video_games.id', '=', 'video_games_ranked_mv.id')
                ->leftJoin('video_game_titles', 'video_games.video_game_title_id', '=', 'video_game_titles.id')
                ->leftJoin('video_game_title_sources', fn ($j) => $j->on('video_game_title_sources.video_game_title_id', '=', 'video_game_titles.id')->where('video_game_title_sources.provider', 'igdb'))
                ->select([
                    'video_games.*',
                    'video_games_ranked_mv.image_url',
                    'video_games_ranked_mv.image_urls',
                    'video_games_ranked_mv.media as mv_media',
                    'video_game_titles.name as canonical_name',
                    'video_game_title_sources.raw_payload',
                    'video_game_title_sources.platform as source_platform',
                    'video_game_title_sources.genre as source_genre',
                    DB::raw("(SELECT json_agg(json_build_object('video_id', video_id, 'name', COALESCE(title, primary_collection, 'Trailer'), 'type', COALESCE(primary_collection, 'trailer'), 'duration', duration)) FROM videos WHERE videos.video_game_id = video_games.id) as all_videos"),
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
            $sorted = collect($this->buildPricingMapFromQuery($this->latestPriceQuery()))
                ->filter(fn ($p) => $p['btc_price'] !== null)
                ->sortBy('btc_price');

            $ids = $sorted->keys()->all();
            $games = $ids === [] ? collect() : $this->applyPremiumFilter($this->baseGameQuery())
                ->whereIn('id', $ids)
                ->get()
                ->sortBy(fn ($g) => array_search($g->id, $ids, true))
                ->take($limit)->values();

            return ['games' => $games, 'pricing' => $sorted->only($games->pluck('id')->all())->toArray()];
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
                $baseQuery = fn ($minRating) => $this->applyPremiumFilter(DB::table('video_games_genre_ranked_mv')->select($this->mvColumns())->where('genre_name', $genreName))
                    ->whereBetween('release_date', [self::LANDING_FEATURED_YEAR_MIN.'-01-01', self::LANDING_FEATURED_YEAR_MAX.'-12-31'])
                    ->where(fn ($q) => $q->where('image_url', 'like', '%/co%')->orWhere('image_urls', 'like', '%/co%')->orWhere('media', 'like', '%"role":"cover"%'))
                    ->whereNotNull('rating')->where('rating', '>=', $minRating)->where('rating_count', '>=', self::LANDING_FEATURED_MIN_RATING_COUNT)
                    ->whereExists(fn ($sub) => $sub->selectRaw('1')->from('video_game_prices as p')->whereColumn('p.video_game_id', 'video_games_genre_ranked_mv.id')->where('p.is_active', true)->whereNotNull('p.currency')->where('p.amount_minor', '>=', 0));

                $games = $baseQuery(self::LANDING_FEATURED_MIN_RATING)->orderByDesc('release_date')->orderByDesc('rating')->orderByDesc('rating_count')->orderByDesc('popularity_score')->limit($limit)->get();

                if ($games->count() < $limit && self::LANDING_FEATURED_FALLBACK_MIN_RATING < self::LANDING_FEATURED_MIN_RATING) {
                    $fallback = $baseQuery(self::LANDING_FEATURED_FALLBACK_MIN_RATING)->when($games->isNotEmpty(), fn ($q) => $q->whereNotIn('id', $games->pluck('id')->all()))
                        ->orderByDesc('release_date')->orderByDesc('rating')->orderByDesc('rating_count')->orderByDesc('popularity_score')->limit($limit - $games->count())->get();
                    $games = $games->concat($fallback);
                }

                if ($games->isNotEmpty()) {
                    $rows[] = ['key' => $rowSpec['key'], 'title' => $rowSpec['title'], 'games' => $games];
                }
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
        $from = $query->from ?? '';
        $prefix = match (true) {
            str_contains($from, 'video_games_upcoming_mv') => 'video_games_upcoming_mv.',
            str_contains($from, 'video_games_genre_ranked_mv') => 'video_games_genre_ranked_mv.',
            default => 'video_games_ranked_mv.'
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
        $from = $query->from ?? '';
        $isUpcoming = str_contains($from, 'video_games_upcoming_mv');

        $prefix = match (true) {
            $isUpcoming => 'video_games_upcoming_mv.',
            str_contains($from, 'video_games_genre_ranked_mv') => 'video_games_genre_ranked_mv.',
            default => 'video_games_ranked_mv.'
        };

        if ($isUpcoming) {
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
        return DB::table('video_game_prices')
            ->select(['video_game_id', 'currency', 'amount_minor', DB::raw('COALESCE(country_code, region_code) as country_code'), 'recorded_at', 'retailer'])
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
        return collect()
            ->merge($topRated->pluck('id'))
            ->merge($newReleases->pluck('id'))
            ->merge($mostReviewed->pluck('id'))
            ->merge($bestDeals->pluck('id'))
            ->merge(collect($genreRows)->flatMap(fn ($row) => $row['games']->pluck('id')))
            ->unique()
            ->values()
            ->all();
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

            $results = $topLists->map(function ($list) {
                $games = DB::table('provider_toplist_items as pti')
                    ->join('video_games_ranked_mv as vg', 'pti.video_game_id', '=', 'vg.id')
                    ->where('pti.provider_toplist_id', $list->id)
                    ->where('vg.rating', '>=', 60)
                    ->select(array_map(fn ($col) => "vg.{$col}", $this->mvColumns()))
                    ->orderBy('pti.rank')
                    ->limit(self::ROW_LIMIT)
                    ->get();

                return $games->isNotEmpty() ? [
                    'key' => $list->list_key,
                    'title' => $this->formatListTitle($list->name, $list->list_key),
                    'games' => $games,
                ] : null;
            })->filter()->values()->all();

            // If we don't have enough lists, fall back to some genre-based lists
            if (count($results) < 3) {
                $genreRows = $this->fetchGenreRows(self::ROW_LIMIT);
                $results = array_merge($results, array_slice($genreRows, 0, 5 - count($results)));
            }

            return $results;
        });
    }

    /**
     * Format list title for display
     */
    private function formatListTitle(string $name, string $key): string
    {
        return match ($key) {
            'popular' => '⭐ Most Popular',
            'top-rated', 'top_rated' => '🏆 Top Rated',
            'new-releases', 'new_releases' => '🆕 New Releases',
            'upcoming' => '🎮 Coming Soon',
            default => str_replace(['IGDB ', 'RAWG ', 'Giant Bomb '], '', $name)
        };
    }

    /**
     * @param  array<int>  $gameIds
     * @return array<int, array<string, mixed>>
     */
    private function buildPricingMapForIds(array $gameIds): array
    {
        return $gameIds === [] ? [] : $this->buildPricingMapFromQuery($this->latestPriceQuery()->whereIn('video_game_id', $gameIds));
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
            $localCurrency = $this->resolveLocalCurrency($price, $countryCurrencies);
            $rateResult = $this->getRateForCurrency($localCurrency, $rateCache);
            $primaryRate = $rateResult['rates'][0]['close'] ?? null;

            if ($primaryRate === null) {
                continue;
            }

            $amountMinor = (int) $price->amount_minor;
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
            return Country::query()->with('currency:id,code')->get()->filter(fn ($c) => $c->currency)
                ->mapWithKeys(fn ($c) => [strtoupper($c->code) => strtoupper($c->currency->code)])->toArray();
        });
    }

    /**
     * @param  array<string, string>  $countryCurrencies
     */
    private function resolveLocalCurrency(object $price, array $countryCurrencies): string
    {
        $code = strtoupper((string) ($price->country_code ?? ''));

        return ($code && isset($countryCurrencies[$code])) ? $countryCurrencies[$code] : strtoupper((string) $price->currency);
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
        return $games->map(fn ($g) => $this->mapGame($g, $pricingMap, $includePricing))->values()->toArray();
    }

    private function mapGame(object $game, array $pricingMap, bool $includePricing): array
    {
        return [
            'id' => $game->id,
            'name' => $game->name,
            'canonical_name' => $game->canonical_name,
            'rating' => $game->rating,
            'release_date' => $game->release_date,
            'genres' => property_exists($game, 'genre') ? $this->normalizeGenres($game->genre) : (property_exists($game, 'genre_name') ? [$game->genre_name] : []),
            'media' => $this->normalizeMedia($game->media ?? null, $game->image_urls ?? null, $game->image_url ?? null),
            'pricing' => $includePricing ? ($pricingMap[$game->id] ?? null) : null,
        ];
    }

    /**
     * @return array<string, mixed>
     */
    private function normalizeMedia(?string $mediaJson, ?string $imageUrlsJson, ?string $imageUrl): array
    {
        $media = is_array($decodedMedia = json_decode($mediaJson ?? '', true)) ? $decodedMedia : [];
        $imageUrls = is_array($decodedUrls = json_decode($imageUrlsJson ?? '', true)) ? $decodedUrls : [];

        // Prefer true cover images (co*) over screenshots/artworks.
        $coverUrl = $this->findCoverVariant($imageUrls, 't_1080p')
            ?? $this->findCoverVariant($imageUrls, 't_720p')
            ?? $this->findCoverVariant($imageUrls, 't_cover_big')
            ?? $this->findImageVariant($imageUrls, 't_1080p')
            ?? $this->findImageVariant($imageUrls, 't_720p')
            ?? $imageUrl;

        // RAWG/MV sometimes provides screenshots as primary image_url; prefer cover from media JSON if available.
        if (is_string($coverUrl) && str_contains($coverUrl, 'media.rawg.io/media/screenshots')) {
            $coverUrl = $this->coverFromMedia($media) ?? $coverUrl;
        }

        $coverThumb = $this->findImageVariant($imageUrls, 't_cover_big')
            ?? $this->findImageVariant($imageUrls, 't_thumb')
            ?? $coverUrl;

        if (! $coverUrl) {
            $coverUrl = $this->coverFromMedia($media);
            $coverThumb ??= $coverUrl;
        }

        // Upscale cover URL for better quality and ensure HTTPS
        $coverUrl = $this->upscaleImage($coverUrl, 't_1080p');
        $coverThumb = $this->upscaleImage($coverThumb, 't_cover_big');

        $screenshots = $this->screenshotsFromMedia($media);
        $artworks = $this->artworksFromMedia($media);

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
            'trailers' => $this->videosFromMedia($media),
        ];
    }

    private function findCoverVariant(array $urls, string $size): ?string
    {
        return collect($urls)
            ->filter(fn ($url) => is_string($url) && str_contains($url, "/{$size}/") && str_contains($url, '/co'))
            ->first();
    }

    /**
     * @return array<int, array{url: string, width: int, height: int}>
     */
    private function artworksFromMedia(array $media): array
    {
        return collect(Arr::get($media, 'images', []))
            ->filter(fn ($image) => is_array($image) && ($image['role'] ?? '') === 'artwork' && ! empty($image['url']))
            ->map(fn ($image) => [
                'url' => $image['url'],
                'width' => 1920,
                'height' => 1080,
            ])
            ->values()
            ->all();
    }

    private function findImageVariant(array $urls, string $size): ?string
    {
        return collect($urls)
            ->filter(fn ($url) => is_string($url) && str_contains($url, "/{$size}/"))
            ->first();
    }

    private function coverFromMedia(array $media): ?string
    {
        $images = Arr::get($media, 'images', []);

        foreach ($images as $image) {
            if (($image['role'] ?? null) === 'cover' && ! empty($image['url'])) {
                return $image['url'];
            }
        }

        return $images[0]['url'] ?? null;
    }

    /**
     * @return array<int, array{url: string, width: int, height: int}>
     */
    private function screenshotsFromMedia(array $media): array
    {
        return collect(Arr::get($media, 'images', []))
            ->filter(fn ($image) => is_array($image) && ($image['role'] ?? '') === 'screenshot' && ! empty($image['url']))
            ->map(fn ($image) => [
                'url' => $image['url'],
                'width' => 1920,
                'height' => 1080,
            ])
            ->values()
            ->all();
    }

    /**
     * @return array<int, array{url?: string, thumbnail?: string, name?: string, video_id?: string}>
     */
    private function videosFromMedia(array $media): array
    {
        return collect(Arr::get($media, 'videos', []))
            ->filter(fn ($video) => is_array($video) && (! empty($video['url']) || ! empty($video['video_id'])))
            ->map(fn ($video) => [
                'url' => $video['url'] ?? null,
                'thumbnail' => $video['thumbnail'] ?? null,
                'name' => $video['name'] ?? 'Trailer',
                'video_id' => $video['video_id'] ?? null,
            ])
            ->values()
            ->all();
    }

    /**
     * @return array<int, string>
     */
    private function normalizeGenres(mixed $genres): array
    {
        if (is_string($genres)) {
            $decoded = json_decode($genres, true);
            $genres = is_array($decoded) ? $decoded : [$genres];
        }

        return collect($genres ?? [])
            ->map(fn ($genre) => is_array($genre) ? Arr::get($genre, 'name') : $genre)
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
        if (! $url || ! str_contains($url, 'igdb.com')) {
            return $url;
        }

        $url = str_replace(['t_thumb', 't_cover_small', 't_logo_med'], $target, $url);

        return str_starts_with($url, '//') ? 'https:'.$url : $url;
    }

    private function cacheStore(): CacheRepository
    {
        return Cache::store(config('cache.default') === 'redis' ? 'redis' : null);
    }
}
