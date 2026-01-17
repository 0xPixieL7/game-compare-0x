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
        $prices = \Illuminate\Support\Facades\DB::table('video_game_prices')
            ->where('video_game_id', $game->id)
            ->get();

        return Inertia::render('VideoGames/Show', [
            'game' => $game,
            'prices' => $prices,
            'media' => $game->getMediaSummary(),
        ]);
    }
}
