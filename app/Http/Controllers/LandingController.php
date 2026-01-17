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
use Illuminate\Support\Str;
use Inertia\Inertia;
use Inertia\Response;

class LandingController extends Controller
{
    private const ROW_LIMIT = 12;

    private const GENRE_POOL_LIMIT = 100;

    private const COUNTRY_CURRENCY_CACHE_TTL = 3600;

    private const ROW_CACHE_TTL = 300;

    public function __construct(private readonly TradingViewClient $tradingViewClient) {}

    public function index(Request $request): Response
    {
        $isAuthenticated = $request->user() !== null;

        $topRated = $this->fetchTopRated(self::ROW_LIMIT);
        $newReleases = $this->fetchNewReleases(self::ROW_LIMIT);
        $mostReviewed = $this->fetchMostReviewed(self::ROW_LIMIT);
        $bestDealsData = $this->fetchBestDeals(self::ROW_LIMIT);
        $genreRows = $this->fetchGenreRows(self::ROW_LIMIT);

        $displayIds = $this->collectDisplayIds($topRated, $newReleases, $mostReviewed, $bestDealsData['games'], $genreRows);
        $pricingMap = $isAuthenticated ? $this->buildPricingMapForIds($displayIds) : [];
        $pricingMap = array_replace($pricingMap, $bestDealsData['pricing']);

        $rows = [
            [
                'id' => 'top-rated',
                'title' => 'Top Rated',
                'games' => $this->mapGames($topRated, $pricingMap, $isAuthenticated),
            ],
            [
                'id' => 'new-releases',
                'title' => 'New Releases',
                'games' => $this->mapGames($newReleases, $pricingMap, $isAuthenticated),
            ],
            [
                'id' => 'best-deals',
                'title' => 'Best Deals',
                'games' => $this->mapGames($bestDealsData['games'], $pricingMap, $isAuthenticated),
            ],
            [
                'id' => 'most-reviewed',
                'title' => 'Most Reviewed',
                'games' => $this->mapGames($mostReviewed, $pricingMap, $isAuthenticated),
            ],
        ];

        foreach ($genreRows as $genreRow) {
            $rows[] = [
                'id' => 'genre-'.Str::slug($genreRow['genre']),
                'title' => $genreRow['title'],
                'games' => $this->mapGames($genreRow['games'], $pricingMap, $isAuthenticated),
            ];
        }

        $hero = $topRated->first();

        return Inertia::render('welcome', [
            'hero' => $hero ? $this->mapGame($hero, $pricingMap, $isAuthenticated) : null,
            'rows' => $rows,
            'cta' => [
                'pricing' => 'Join free for price data',
            ],
        ]);
    }

    private function fetchTopRated(int $limit): Collection
    {
        return $this->cacheStore()->remember('landing:top-rated', self::ROW_CACHE_TTL, function () use ($limit) {
            return $this->baseGameQuery()
                ->whereNotNull('video_games.rating')
                ->orderByDesc('video_games.rating')
                ->limit($limit)
                ->get();
        });
    }

    private function fetchNewReleases(int $limit): Collection
    {
        return $this->cacheStore()->remember('landing:new-releases', self::ROW_CACHE_TTL, function () use ($limit) {
            return $this->baseGameQuery()
                ->whereNotNull('video_games.release_date')
                ->orderByDesc('video_games.release_date')
                ->limit($limit)
                ->get();
        });
    }

    private function fetchMostReviewed(int $limit): Collection
    {
        return $this->cacheStore()->remember('landing:most-reviewed', self::ROW_CACHE_TTL, function () use ($limit) {
            return $this->baseGameQuery()
                ->orderByDesc('review_score')
                ->limit($limit)
                ->get();
        });
    }

    /**
     * @return array{games: Collection, pricing: array<int, array<string, mixed>>}
     */
    private function fetchBestDeals(int $limit): array
    {
        return $this->cacheStore()->remember('landing:best-deals', self::ROW_CACHE_TTL, function () use ($limit) {
            $pricingMap = $this->buildPricingMapFromQuery($this->latestPriceQuery());

            $sorted = collect($pricingMap)
                ->filter(fn ($pricing) => $pricing['btc_price'] !== null)
                ->sortBy('btc_price')
                ->take($limit);

            $ids = $sorted->keys()->all();
            $games = $ids === []
                ? collect()
                : $this->baseGameQuery()
                    ->whereIn('video_games.id', $ids)
                    ->get()
                    ->sortBy(fn ($game) => array_search($game->id, $ids, true))
                    ->values();

            return [
                'games' => $games,
                'pricing' => $sorted->toArray(),
            ];
        });
    }

    /**
     * @return array<int, array{genre: string, title: string, games: Collection}>
     */
    private function fetchGenreRows(int $limit): array
    {
        return $this->cacheStore()->remember('landing:genre-rows', self::ROW_CACHE_TTL, function () use ($limit) {
            $genrePool = $this->baseGameQuery()
                ->whereNotNull('video_game_title_sources.genre')
                ->orderByDesc('video_games.rating')
                ->limit(self::GENRE_POOL_LIMIT)
                ->get();

            $genreCounts = [];

            foreach ($genrePool as $game) {
                $genres = $this->normalizeGenres($game->genre);
                foreach ($genres as $genre) {
                    $genreCounts[$genre] = ($genreCounts[$genre] ?? 0) + 1;
                }
            }

            arsort($genreCounts);
            $topGenres = array_slice(array_keys($genreCounts), 0, 4);

            $rows = [];
            foreach ($topGenres as $genre) {
                $games = $genrePool
                    ->filter(function ($game) use ($genre) {
                        return in_array($genre, $this->normalizeGenres($game->genre), true);
                    })
                    ->sortByDesc('rating')
                    ->take($limit)
                    ->values();

                if ($games->isEmpty()) {
                    continue;
                }

                $rows[] = [
                    'genre' => $genre,
                    'title' => $genre,
                    'games' => $games,
                ];
            }

            return $rows;
        });
    }

    private function baseGameQuery(): Builder
    {
        $reviewScore = $this->reviewScoreExpression();

        return DB::table('video_games')
            ->select([
                'video_games.id',
                'video_games.name',
                'video_games.rating',
                'video_games.release_date',
                'video_games.opencritic_review_count',
                'video_games.opencritic_user_count',
                'video_game_titles.name as canonical_name',
                'video_game_title_sources.genre',
                'video_game_title_sources.raw_payload as media',
                'video_game_title_sources.rating_count',
                'images.urls as image_urls',
                'images.url as image_url',
                DB::raw("{$reviewScore} as review_score"),
            ])
            ->join('video_game_titles', 'video_games.video_game_title_id', '=', 'video_game_titles.id')
            ->leftJoin('video_game_title_sources', 'video_game_titles.id', '=', 'video_game_title_sources.video_game_title_id')
            ->leftJoin('images', 'images.video_game_id', '=', 'video_games.id');
    }

    private function latestPriceQuery(): Builder
    {
        return DB::query()
            ->fromSub(function (Builder $query) {
                $query->from('video_game_prices')
                    ->select([
                        'video_game_prices.video_game_id',
                        'video_game_prices.currency',
                        'video_game_prices.amount_minor',
                        DB::raw('COALESCE(video_game_prices.country_code, video_game_prices.region_code) as country_code'),
                        'video_game_prices.recorded_at',
                        'video_game_prices.retailer',
                        DB::raw('ROW_NUMBER() OVER (PARTITION BY video_game_prices.video_game_id ORDER BY video_game_prices.recorded_at DESC) as rn'),
                    ])
                    ->whereNotNull('video_game_prices.currency')
                    ->where('video_game_prices.amount_minor', '>=', 0);
            }, 'latest_prices')
            ->where('rn', 1);
    }

    private function reviewScoreExpression(): string
    {
        $sum = 'COALESCE(video_game_title_sources.rating_count, 0)'
            .' + COALESCE(video_games.opencritic_review_count, 0)'
            .' + COALESCE(video_games.opencritic_user_count, 0)';

        $denominator = '('.
            'CASE WHEN video_game_title_sources.rating_count IS NULL THEN 0 ELSE 1 END + '
            .'CASE WHEN video_games.opencritic_review_count IS NULL THEN 0 ELSE 1 END + '
            .'CASE WHEN video_games.opencritic_user_count IS NULL THEN 0 ELSE 1 END'
            .')';

        return "({$sum}) / NULLIF({$denominator}, 0)";
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
        $media = $this->normalizeMedia($game->media, $game->image_urls, $game->image_url);
        $pricing = $includePricing ? ($pricingMap[$game->id] ?? null) : null;

        return [
            'id' => $game->id,
            'name' => $game->name,
            'canonical_name' => $game->canonical_name,
            'rating' => $game->rating,
            'release_date' => $game->release_date,
            'genres' => $this->normalizeGenres($game->genre),
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

        $coverUrl = $this->findImageVariant($imageUrls, 't_1080p')
            ?? $this->findImageVariant($imageUrls, 't_720p')
            ?? $imageUrl;

        $coverThumb = $this->findImageVariant($imageUrls, 't_cover_big')
            ?? $this->findImageVariant($imageUrls, 't_thumb')
            ?? $coverUrl;

        if (! $coverUrl) {
            $coverUrl = $this->coverFromMedia($media);
            $coverThumb = $coverThumb ?? $coverUrl;
        }

        $screenshots = $this->screenshotsFromMedia($media);
        $trailers = $this->videosFromMedia($media);

        return [
            'cover' => $coverUrl ? [
                'url' => $coverUrl,
                'width' => 1080,
                'height' => 1440,
            ] : null,
            'cover_url' => $coverUrl,
            'cover_url_thumb' => $coverThumb,
            'screenshots' => $screenshots,
            'trailers' => $trailers,
        ];
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

    private function cacheStore(): CacheRepository
    {
        if (config('cache.default') === 'redis') {
            return Cache::store('redis');
        }

        return Cache::store();
    }
}
