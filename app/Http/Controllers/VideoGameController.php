<?php

namespace App\Http\Controllers;

use App\Models\VideoGame;
use Illuminate\Http\Request;
use Inertia\Inertia;

class VideoGameController extends Controller
{
    public function index(Request $request)
    {
        $sort = $request->input('sort', 'top_rated');

        // Fetch featured game (highest rated with a video)
        $featuredGame = VideoGame::query()
            ->has('videos')
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
        $prices = \App\Models\VideoGamePrice::query()
            ->where('video_game_id', $game->id)
            ->where('is_active', true)
            ->orderBy('amount_minor', 'asc')
            ->get()
            ->map(function ($price) {
                $meta = $price->metadata ?? [];

                return [
                    'id' => $price->id,
                    'retailer' => $price->retailer,
                    'country_code' => $price->country_code,
                    'currency' => $price->currency,
                    'amount' => $price->amount_minor / 100,
                    'url' => $price->url,
                    'discount_percent' => $meta['discount_percent'] ?? 0,
                    'initial_amount' => isset($meta['initial_amount_minor']) ? ($meta['initial_amount_minor'] / 100) : null,
                ];
            });

        // Organize media for the frontend
        $media = [
            'hero' => $game->getHeroImageUrl(),
            'logo' => $game->getFirstMediaUrl('clear_logo'),
            'poster' => $game->getFirstMediaUrl('posters'),
            'background' => $game->getFirstMediaUrl('backgrounds'),
            'cover' => $game->getCoverUrl('t_1080p'),
            'screenshots' => $game->getScreenshots()->pluck('url')->take(6)->values()->all(),
            'trailers' => $game->getTrailers()->pluck('youtube_watch_url')->filter()->values()->all(),
        ];

        return Inertia::render('VideoGames/Show', [
            'game' => [
                'id' => $game->id,
                'name' => $game->name,
                'summary' => $game->attributes['summary'] ?? $game->description,
                'release_date' => $game->release_date?->format('F j, Y'),
                'rating' => round($game->rating ?? 0),
                'genres' => $game->attributes['genres'] ?? [],
                'platforms' => $game->platform ?? [],
                'developer' => $game->developer,
                'publisher' => $game->publisher,
                'theme' => $game->attributes['theme'] ?? null,
            ],

            'prices' => $prices,
            'media' => $media,
        ]);
    }
}
