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
    private const ROW_LIMIT = 12;

    private const GENRE_POOL_LIMIT = 100;

    private const COUNTRY_CURRENCY_CACHE_TTL = 3600;

    private const ROW_CACHE_TTL = 3600; // Increased to 1 hour to prevent timeouts during imports

    public function __construct(private readonly TradingViewClient $tradingViewClient) {}

    public function index(Request $request): Response
    {
        set_time_limit(120); // Give it extra time if DB is under heavy load

        Log::info('Homepage hit', [
            'ip' => $request->ip(),
            'user_agent' => $request->userAgent(),
            'auth' => $request->user()?->id
        ]);

        $isAuthenticated = $request->user() !== null;

        $topRated = $this->fetchTopRated(self::ROW_LIMIT);
        $newReleases = $this->fetchNewReleases(self::ROW_LIMIT);
        $upcoming = $this->fetchUpcoming(self::ROW_LIMIT);
        $mostReviewed = $this->fetchMostReviewed(self::ROW_LIMIT);
        $bestDealsData = $this->fetchBestDeals(self::ROW_LIMIT);
        $genreRows = $this->fetchGenreRows(self::ROW_LIMIT);

        $displayIds = $this->collectDisplayIds($topRated, $newReleases, $mostReviewed, $bestDealsData['games'], $genreRows);
        $displayIds = array_unique(array_merge($displayIds, $upcoming->pluck('id')->all()));
        
        $pricingMap = $isAuthenticated ? $this->buildPricingMapForIds($displayIds) : [];
        $pricingMap = array_replace($pricingMap, $bestDealsData['pricing']);

        $rows = [
            [
                'id' => 'top-rated',
                'title' => 'Top Rated',
                'games' => $this->mapGames($topRated, $pricingMap, $isAuthenticated),
            ],
            [
                'id' => 'upcoming',
                'title' => 'Upcoming Games',
                'games' => $this->mapGames($upcoming, $pricingMap, $isAuthenticated),
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

        $hero = $this->selectHero($topRated, $newReleases, $mostReviewed);
        $spotlightGames = $this->fetchSpotlightGames(6);

        Log::info('Homepage data fetched', [
            'topRated' => $topRated->count(),
            'upcoming' => $upcoming->count(),
            'newReleases' => $newReleases->count(),
            'mostReviewed' => $mostReviewed->count(),
            'bestDeals' => $bestDealsData['games']->count(),
            'genreRows' => count($genreRows),
            'heroLinked' => $hero !== null,
            'spotlightCount' => count($spotlightGames)
        ]);

        return Inertia::render('welcome', [
            'hero' => $hero ? $this->mapGame($hero, $pricingMap, $isAuthenticated) : null,
            'spotlightGames' => $spotlightGames,
            'rows' => $rows,
            'cta' => [
                'pricing' => 'Join free for price data',
            ],
        ]);
    }

    private function fetchSpotlightGames(int $limit = 6): array
    {
        return $this->cacheStore()->remember('landing:spotlight-games-v7', self::ROW_CACHE_TTL, function () use ($limit) {
            // Target IDs: GTA VI, EA FC 26, NBA 2K26, Witcher 3, Cloud Cutter, Skyrim Anniversary
            $forcedIds = [121815, 741167, 741505, 1014215, 1018, 195630];
            
            // Use base video_games table but select columns required for mapping
            $games = DB::table('video_games')
                ->leftJoin('video_game_titles', 'video_games.video_game_title_id', '=', 'video_game_titles.id')
                ->leftJoin('video_game_title_sources', function ($join) {
                    $join->on('video_game_title_sources.video_game_title_id', '=', 'video_game_titles.id')
                        ->where('video_game_title_sources.provider', '=', 'igdb');
                })
                ->leftJoin('videos', 'video_games.id', '=', 'videos.video_game_id')
                ->select([
                    'video_games.*',
                    'video_game_titles.name as canonical_name',
                    'video_game_title_sources.raw_payload',
                    'video_game_title_sources.platform as source_platform',
                    'video_game_title_sources.genre as source_genre',
                    'videos.video_id',
                    'videos.metadata as video_metadata'
                ])
                ->whereIn('video_games.id', $forcedIds)
                ->get();

            // Fetch enriched images for backdrop selection
            $images = DB::table('images')
                ->whereIn('imageable_id', $forcedIds)
                ->where('imageable_type', 'App\Models\VideoGame')
                ->get()
                ->keyBy('imageable_id');

            // Sort to match requested order and reset keys to ensure sequential array
            $games = $games->sortBy(function($game) use ($forcedIds) {
                return array_search($game->id, $forcedIds);
            })->values();

            return $games->map(function ($game) use ($images) {
                // Parse media from raw_payload for high-res screenshots/artworks
                $rawPayload = property_exists($game, 'raw_payload') && $game->raw_payload 
                    ? json_decode($game->raw_payload, true) 
                    : [];
                $baseUrl = 'https://images.igdb.com/igdb/image/upload/';
                
                $screenshots = [];
                if (!empty($rawPayload['screenshots'])) {
                    $ids = is_string($rawPayload['screenshots']) ? explode(',', str_replace(['{', '}'], '', $rawPayload['screenshots'])) : $rawPayload['screenshots'];
                    foreach (array_slice($ids, 0, 5) as $id) {
                        if ($id) $screenshots[] = ['url' => $baseUrl . 't_1080p/sc' . base_convert((string)$id, 10, 36) . '.webp'];
                    }
                }

                $artworks = [];
                if (!empty($rawPayload['artworks'])) {
                    $ids = is_string($rawPayload['artworks']) ? explode(',', str_replace(['{', '}'], '', $rawPayload['artworks'])) : $rawPayload['artworks'];
                    foreach (array_slice($ids, 0, 5) as $id) {
                        if ($id) $artworks[] = ['url' => $baseUrl . 't_1080p/ar' . base_convert((string)$id, 10, 36) . '.webp'];
                    }
                }
                
                // Extract trailers
                $trailers = [];
                if ($game->video_metadata) {
                    $metadata = json_decode($game->video_metadata, true);
                    if (is_array($metadata)) {
                        foreach ($metadata as $v) {
                            if (!empty($v['video_id'])) {
                                $trailers[] = [
                                    'video_id' => $v['video_id'],
                                    'name' => $v['name'] ?? 'Trailer',
                                    'url' => 'https://www.youtube.com/watch?v=' . $v['video_id']
                                ];
                            }
                        }
                    }
                }

                if (empty($trailers) && $game->video_id) {
                    $trailers[] = [
                        'video_id' => $game->video_id,
                        'name' => 'Trailer',
                        'url' => 'https://www.youtube.com/watch?v=' . $game->video_id
                    ];
                }

                // Manual fallback for Witcher 3 if no media found
                if ($game->id == 1014215 && empty($trailers)) {
                    $trailers[] = [
                        'video_id' => 'rIoPrbzI5Z4',
                        'name' => 'The Witcher 3: Wild Hunt Trailer',
                        'url' => 'https://www.youtube.com/watch?v=rIoPrbzI5Z4'
                    ];
                }

                $mapped = $this->mapGame($game, [], false);
                $mapped['media']['trailers'] = $trailers;
                
                // Prioritize high-res screenshots/artworks from raw_payload if available
                if (!empty($screenshots)) {
                    $mapped['media']['screenshots'] = array_merge($screenshots, $mapped['media']['screenshots'] ?? []);
                }
                if (!empty($artworks)) {
                    $mapped['media']['artworks'] = array_merge($artworks, $mapped['media']['artworks'] ?? []);
                }
                
                // Determine Backdrop URL
                // Strategy: Check local images table first (metadata > details), then fall back to raw_payload
                $backdropUrl = null;
                $dbImage = $images->get($game->id);

                if ($dbImage && $dbImage->metadata) {
                    $metadata = json_decode($dbImage->metadata, true);
                    $details = $metadata['details'] ?? $metadata['all_details'] ?? [];
                    
                    if (!empty($details)) {
                        // Filter for artworks and screenshots
                        $artworksList = array_filter($details, fn($d) => ($d['collection'] ?? '') === 'artworks');
                        $screenshotsList = array_filter($details, fn($d) => ($d['collection'] ?? '') === 'screenshots');

                        // Sort by resolution (width * height) descending
                        $sortByRes = function($a, $b) {
                            $resA = ($a['width'] ?? 0) * ($a['height'] ?? 0);
                            $resB = ($b['width'] ?? 0) * ($b['height'] ?? 0);
                            return $resB <=> $resA;
                        };

                        usort($artworksList, $sortByRes);
                        usort($screenshotsList, $sortByRes);

                        // Pick best candidate: Best Artwork > Best Screenshot
                        $bestCandidate = $artworksList[0] ?? $screenshotsList[0] ?? null;

                        if ($bestCandidate && isset($bestCandidate['url'])) {
                            $url = $bestCandidate['url'];
                            // If it's an IGDB URL, upgrade to original size if possible
                            // IGDB usually puts size variants in 'size_variants' or we can replace the size in URL
                            // Check 'size_variants' first if available
                            $foundOriginal = false;
                            if (isset($bestCandidate['size_variants']) && is_array($bestCandidate['size_variants'])) {
                                foreach ($bestCandidate['size_variants'] as $variant) {
                                    if (str_contains($variant, 't_original') || str_contains($variant, 't_1080p')) {
                                        // Prefer original if found, or 1080p
                                        if (str_contains($variant, 't_original')) {
                                            $backdropUrl = $variant;
                                            $foundOriginal = true;
                                            break;
                                        }
                                        $backdropUrl = $variant; // Keep 1080p as fallback
                                    }
                                }
                            }
                            
                            if (!$foundOriginal && str_contains($url, 'images.igdb.com')) {
                                // Manual upgrade if variants didn't yield 't_original'
                                $backdropUrl = preg_replace('/\/t_[a-zA-Z0-9_]+\//', '/t_original/', $url);
                            } elseif (!$backdropUrl) {
                                $backdropUrl = $url;
                            }
                        }
                    }
                }

                // Fallback to raw_payload logic if DB image yielded nothing
                if (!$backdropUrl) {
                    // 1. Try first Artwork (usually landscape/high-res)
                    if (!empty($artworks) && isset($artworks[0]['url'])) {
                        $backdropUrl = str_replace('t_1080p', 't_original', $artworks[0]['url']);
                    }
                    // 2. Fallback to first Screenshot
                    elseif (!empty($screenshots) && isset($screenshots[0]['url'])) {
                        $backdropUrl = str_replace('t_1080p', 't_original', $screenshots[0]['url']);
                    }
                }

                $mapped['backdrop_url'] = $backdropUrl;
                
                // Fallback: Check images table if no cover found in payload
                if (empty($mapped['media']['cover_url']) && $dbImage) {
                    $mapped['media']['cover_url'] = $dbImage->url;
                    // Use higher res variant if available in urls array
                    if ($dbImage->urls) {
                        $urls = json_decode($dbImage->urls, true);
                        foreach ($urls as $url) {
                            if (str_contains($url, 't_1080p') || str_contains($url, 't_cover_big')) {
                                $mapped['media']['cover_url_high_res'] = $url;
                                break;
                            }
                        }
                    }
                }
                
                return $mapped;
            })->toArray();
        });
    }

    private function fetchTopRated(int $limit): Collection
    {
        return $this->cacheStore()->remember('landing:top-rated', self::ROW_CACHE_TTL, function () use ($limit) {
            return $this->baseGameQuery()
                ->whereNotNull('rating')
                ->orderByDesc('rating')
                ->limit($limit)
                ->get();
        });
    }

    private function fetchUpcoming(int $limit): Collection
    {
        return $this->cacheStore()->remember('landing:upcoming', self::ROW_CACHE_TTL, function () use ($limit) {
            return DB::table('video_games_upcoming_mv')
                ->select($this->mvColumns())
                ->limit($limit)
                ->get();
        });
    }

    private function fetchNewReleases(int $limit): Collection
    {
        return $this->cacheStore()->remember('landing:new-releases', self::ROW_CACHE_TTL, function () use ($limit) {
            return $this->baseGameQuery()
                ->whereNotNull('release_date')
                ->where('release_date', '<=', now())
                ->orderByDesc('release_date')
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

    private function selectHero(Collection $topRated, Collection $newReleases, Collection $mostReviewed): ?object
    {
        $candidates = $topRated
            ->merge($newReleases)
            ->merge($mostReviewed)
            ->filter(fn ($game) => ! empty($game->image_url) || ! empty($game->image_urls))
            ->sortByDesc(fn ($game) => $game->rating ?? 0)
            ->values();

        return $candidates->first();
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
                    ->whereIn('id', $ids)
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
            $targetGenres = [
                'Action' => 'Action & Adventure',
                'Role-playing (RPG)' => 'Top RPGs',
                'Shooter' => 'FPS & Shooters',
                'Strategy' => 'Strategy Games',
                'Adventure' => 'Story & Adventure',
                'Racing' => 'Racing & Speed',
                'Sport' => 'Sports',
                'Simulator' => 'Simulators',
                'Fighting' => 'Fighting Games',
                'Puzzle' => 'Puzzle & Brain',
                'Indie' => 'Indie Gems',
                'Arcade' => 'Arcade Classics',
            ];

            $rows = [];
            foreach ($targetGenres as $genreName => $displayTitle) {
                $games = DB::table('video_games_genre_ranked_mv')
                    ->select($this->mvColumns())
                    ->where('genre_name', $genreName)
                    ->limit($limit)
                    ->get();

                if ($games->isEmpty()) {
                    continue;
                }

                $rows[] = [
                    'genre' => $genreName,
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
        ];
    }

    private function baseGameQuery(): Builder
    {
        return DB::table('video_games_ranked_mv')
            ->select($this->mvColumns());
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
        $artworks = $this->artworksFromMedia($media);
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
            'artworks' => $artworks,
            'trailers' => $trailers,
        ];
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

    private function cacheStore(): CacheRepository
    {
        if (config('cache.default') === 'redis') {
            return Cache::store('redis');
        }

        return Cache::store();
    }
}
