<?php

namespace App\Http\Controllers;

use App\Models\VideoGame;
use App\Models\VideoGamePrice;
use App\Services\ExchangeRates\ExchangeRateService;
use Illuminate\Http\Request;
use Illuminate\Support\Collection;
use Inertia\Inertia;

class VideoGameController extends Controller
{
    public function __construct(private readonly ExchangeRateService $exchangeRates) {}

    public function index(Request $request)
    {
        $sort = $request->input('sort', 'top_rated');

        // Fetch featured game (highest rated with a video)
        $featuredGame = VideoGame::query()
            ->has('videos')
            ->where('rating', '>=', 60)
            ->orderByDesc('rating')
            ->with(['images', 'videos'])
            ->first();

        // Transform featured game
        $featuredData = $featuredGame ? [
            'id' => $featuredGame->id,
            'name' => $featuredGame->name,
            'rating' => $featuredGame->rating,
            'cover_url' => $featuredGame->getCoverUrl('t_1080p'),
            'trailer_url' => $featuredGame->getFirstTrailer()['youtube_embed_url'] ?? null,
            'description' => $featuredGame->attributes['summary'] ?? $featuredGame->attributes['storyline'] ?? 'No description available.',
        ] : null;

        $query = VideoGame::query()
            ->where('rating', '>=', 60)
            ->with(['images', 'videos', 'latestPrice']);

        // Sorting logic
        if ($sort === 'top_rated') {
            $query->orderByDesc('rating');
        } elseif ($sort === 'newest') {
            $query->orderByDesc('release_date');
        }

        // Opportunistically sync market data in the background if needed
        \App\Jobs\SynchronizeGlobalMarketDataJob::dispatchAfterResponse();

        return Inertia::render('VideoGames/Index', [
            'featuredGame' => $featuredData,
            'games' => Inertia::defer(function () use ($query) {

                $games = $query->paginate(24)->withQueryString();

                // Transform collection to include media URLs
                $games->getCollection()->transform(function ($game) {
                    return [
                        'id' => $game->id,
                        'name' => $game->name,
                        'rating' => $game->rating,
                        'release_date' => $game->release_date?->format('Y-m-d'),
                        'cover_url' => $game->getCoverUrl(),
                        'cover_url_high_res' => $game->getCoverUrl('t_1080p'),
                        'latest_price' => $game->latestPrice?->price,
                        'currency' => $game->latestPrice?->currency,
                    ];
                });

                return $games;
            }),
            'filters' => $request->only(['sort']),
        ]);
    }

    public function show(VideoGame $game)
    {
        $game->load(['images', 'videos']);

        // Fetch prices sorted by amount (cheapest first)
        $prices = VideoGamePrice::query()
            ->where('video_game_id', $game->id)
            ->where('is_active', true)
            ->orderBy('amount_minor', 'asc')
            ->get();

        $prices = $this->mapPricesWithBtc($prices);

        // Get all screenshots and trailers
        $allScreenshots = $game->getScreenshots();
        $allTrailers = $game->getTrailers();
        $allArtworks = $game->getArtworks();

        // Organize media for the frontend
        $media = [
            'hero' => $game->getHeroImageUrl(),
            'logo' => $game->getFirstMediaUrl('clear_logo'),
            'poster' => $game->getFirstMediaUrl('posters'),
            'background' => $game->getFirstMediaUrl('backgrounds'),
            'cover' => $game->getCoverUrl('t_1080p'),
            'screenshots' => $allScreenshots->pluck('url')->values()->all(),
            'artworks' => $allArtworks->pluck('url')->values()->all(),
            'trailers' => $allTrailers->map(fn ($t) => [
                'url' => $t['youtube_watch_url'] ?? $t['youtube_embed_url'] ?? null,
                'embed_url' => $t['youtube_embed_url'] ?? null,
                'name' => $t['name'] ?? 'Trailer',
                'video_id' => $t['video_id'] ?? null,
            ])->filter(fn ($t) => $t['url'] !== null)->values()->all(),
        ];

        // Get unique countries from prices
        $uniqueCountries = count(array_unique(array_column($prices, 'country_code')));

        // Extract ratings from various sources
        $ratings = [
            'igdb' => round($game->rating ?? 0),
            'rating_count' => $game->rating_count ?? 0,
            'hypes' => $game->hypes ?? 0,
            'follows' => $game->follows ?? 0,
        ];

        return Inertia::render('VideoGames/Show', [
            'game' => [
                'id' => $game->id,
                'name' => $game->name,
                'summary' => $game->attributes['summary'] ?? $game->description,
                'storyline' => $game->attributes['storyline'] ?? null,
                'release_date' => $game->release_date?->format('F j, Y'),
                'rating' => round($game->rating ?? 0),
                'genres' => $game->attributes['genres'] ?? [],
                'platforms' => $game->platform ?? [],
                'developer' => $game->developer,
                'publisher' => $game->publisher,
                'theme' => $game->attributes['theme'] ?? null,
                'url' => $game->url ?? null,
                'slug' => $game->slug ?? null,
            ],

            'prices' => $prices,
            'media' => $media,
            'statistics' => [
                'unique_countries' => $uniqueCountries,
                'total_prices' => count($prices),
                'total_screenshots' => $allScreenshots->count(),
                'total_videos' => $allTrailers->count(),
                'total_artworks' => $allArtworks->count(),
            ],
            'ratings' => $ratings,
        ]);
    }

    /**
     * @param  Collection<int, VideoGamePrice>  $prices
     * @return array<int, array<string, mixed>>
     */
    private function mapPricesWithBtc(Collection $prices): array
    {
        $rates = [];

        return $prices->map(function (VideoGamePrice $price) use (&$rates) {
            $meta = $price->metadata ?? [];
            $currency = strtoupper($price->currency);

            if (! array_key_exists($currency, $rates)) {
                $rates[$currency] = $this->exchangeRates->getBtcRateForCurrency($currency);
            }

            $rate = $rates[$currency];
            $amountMajor = $price->amount_minor / 100;
            $amountBtc = $rate && $rate > 0 ? $amountMajor / $rate : null;

            return [
                'id' => $price->id,
                'retailer' => $price->retailer,
                'country_code' => $price->country_code,
                'currency' => $price->currency,
                'amount' => $amountMajor,
                'btc_amount' => $amountBtc,
                'btc_rate' => $rate,
                'url' => $price->url,
                'discount_percent' => $meta['discount_percent'] ?? 0,
                'initial_amount' => isset($meta['initial_amount_minor'])
                    ? ($meta['initial_amount_minor'] / 100)
                    : null,
            ];
        })->all();
    }
}
