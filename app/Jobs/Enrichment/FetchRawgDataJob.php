<?php

declare(strict_types=1);

namespace App\Jobs\Enrichment;

use App\Jobs\Enrichment\Traits\CategorizesVideoTypes;
use App\Models\Image;
use App\Models\Video;
use App\Models\VideoGame;
use Illuminate\Bus\Queueable;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Bus\Dispatchable;
use Illuminate\Queue\InteractsWithQueue;
use Illuminate\Queue\Middleware\RateLimited;
use Illuminate\Queue\SerializesModels;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Http;
use Illuminate\Support\Facades\Log;

/**
 * Fetch media from RAWG.io API.
 *
 * RAWG provides screenshots and short video clips.
 * This job runs as a tertiary media source alongside IGDB and TGDB.
 */
class FetchRawgDataJob implements ShouldQueue
{
    use CategorizesVideoTypes, Dispatchable, InteractsWithQueue, Queueable, SerializesModels;

    private const BASE_URL = 'https://api.rawg.io/api';

    public int $tries = 3;

    /** @var array<int, int> */
    public array $backoff = [30, 120, 300];

    public function __construct(
        public int $videoGameId,
        public int $rawgId
    ) {}

    /**
     * @return array<int, object>
     */
    public function middleware(): array
    {
        return [new RateLimited('rawg')];
    }

    public function handle(): void
    {
        $game = VideoGame::find($this->videoGameId);

        if (! $game) {
            Log::warning('FetchRawgDataJob: Game not found', ['game_id' => $this->videoGameId]);

            return;
        }

        $apiKey = config('services.rawg.api_key');

        if (! $apiKey) {
            Log::debug('FetchRawgDataJob: No RAWG API key configured');

            return;
        }

        // Check if we already have RAWG media (don't overwrite)
        $existingRawgMedia = Image::where('video_game_id', $game->id)
            ->where('provider', 'rawg')
            ->exists();

        if ($existingRawgMedia) {
            Log::debug('FetchRawgDataJob: RAWG media already exists', [
                'game_id' => $this->videoGameId,
            ]);

            return;
        }

        // Fetch game details including screenshots
        $gameData = $this->fetchGameDetails($apiKey);

        if (! $gameData) {
            return;
        }

        // Fetch additional screenshots
        $screenshots = $this->fetchScreenshots($apiKey);

        // Fetch video clips if available
        $movies = $this->fetchMovies($apiKey);

        DB::transaction(function () use ($game, $gameData, $screenshots, $movies) {
            $this->storeMedia($game, $gameData, $screenshots, $movies);
        });

        Log::info('FetchRawgDataJob: Complete', [
            'game_id' => $this->videoGameId,
            'rawg_id' => $this->rawgId,
            'screenshots' => count($screenshots),
            'movies' => count($movies),
        ]);
    }

    /**
     * Fetch game details from RAWG API.
     */
    private function fetchGameDetails(string $apiKey): ?array
    {
        $response = Http::get(self::BASE_URL."/games/{$this->rawgId}", [
            'key' => $apiKey,
        ]);

        if (! $response->successful()) {
            Log::warning('FetchRawgDataJob: Failed to fetch game details', [
                'rawg_id' => $this->rawgId,
                'status' => $response->status(),
            ]);

            return null;
        }

        return $response->json();
    }

    /**
     * Fetch screenshots from RAWG API.
     */
    private function fetchScreenshots(string $apiKey): array
    {
        $response = Http::get(self::BASE_URL."/games/{$this->rawgId}/screenshots", [
            'key' => $apiKey,
            'page_size' => 20,
        ]);

        if (! $response->successful()) {
            return [];
        }

        return $response->json('results') ?? [];
    }

    /**
     * Fetch video clips from RAWG API.
     */
    private function fetchMovies(string $apiKey): array
    {
        $response = Http::get(self::BASE_URL."/games/{$this->rawgId}/movies", [
            'key' => $apiKey,
        ]);

        if (! $response->successful()) {
            return [];
        }

        return $response->json('results') ?? [];
    }

    /**
     * Store RAWG media in the database.
     */
    private function storeMedia(VideoGame $game, array $gameData, array $screenshots, array $movies): void
    {
        $urls = [];
        $collections = [];
        $details = [];

        // Background image as cover/header
        $backgroundImage = $gameData['background_image'] ?? null;
        $backgroundImageAdditional = $gameData['background_image_additional'] ?? null;

        if ($backgroundImage) {
            $collections[] = 'cover_images';
            $urls[] = $backgroundImage;
            $details[] = [
                'collection' => 'cover_images',
                'url' => $backgroundImage,
                'type' => 'background_image',
            ];
        }

        if ($backgroundImageAdditional) {
            $collections[] = 'artworks';
            $urls[] = $backgroundImageAdditional;
            $details[] = [
                'collection' => 'artworks',
                'url' => $backgroundImageAdditional,
                'type' => 'background_image_additional',
            ];
        }

        // Screenshots
        foreach ($screenshots as $screenshot) {
            $url = $screenshot['image'] ?? null;

            if (! $url) {
                continue;
            }

            $collections[] = 'screenshots';
            $urls[] = $url;
            $details[] = [
                'collection' => 'screenshots',
                'url' => $url,
                'rawg_id' => $screenshot['id'] ?? null,
                'width' => $screenshot['width'] ?? null,
                'height' => $screenshot['height'] ?? null,
            ];
        }

        $collections = array_values(array_unique($collections));

        // Store images
        if (! empty($urls)) {
            Image::updateOrCreate(
                [
                    'video_game_id' => $game->id,
                    'imageable_type' => VideoGame::class,
                    'imageable_id' => $game->id,
                    'provider' => 'rawg',
                ],
                [
                    'primary_collection' => in_array('cover_images', $collections, true)
                        ? 'cover_images'
                        : ($collections[0] ?? 'misc'),
                    'collection_names' => $collections,
                    'url' => $urls[0] ?? null,
                    'urls' => $urls,
                    'metadata' => [
                        'source' => 'rawg_enrichment',
                        'rawg_id' => $this->rawgId,
                        'total_images' => count($urls),
                        'details' => $details,
                    ],
                ]
            );
        }

        // Store videos
        if (! empty($movies)) {
            $videoUrls = [];
            $videoDetails = [];
            $videoCollections = [];

            foreach ($movies as $movie) {
                $videoData = $movie['data'] ?? [];

                // RAWG provides multiple quality versions
                $videoUrl = $videoData['max'] ?? $videoData['480'] ?? null;

                if (! $videoUrl) {
                    continue;
                }

                $name = $movie['name'] ?? '';
                $videoType = $this->categorizeVideoType($name);
                $videoCollections[] = $videoType;

                $videoUrls[] = $videoUrl;
                $videoDetails[] = [
                    'rawg_id' => $movie['id'] ?? null,
                    'name' => $name,
                    'type' => $videoType,
                    'preview' => $movie['preview'] ?? null,
                    'qualities' => array_keys(array_filter($videoData)),
                ];
            }

            if (! empty($videoUrls)) {
                $videoCollections = array_values(array_unique($videoCollections));

                Video::updateOrCreate(
                    [
                        'video_game_id' => $game->id,
                        'videoable_type' => VideoGame::class,
                        'videoable_id' => $game->id,
                        'provider' => 'rawg',
                    ],
                    [
                        'primary_collection' => $videoCollections[0] ?? 'trailers',
                        'collection_names' => $videoCollections,
                        'url' => $videoUrls[0] ?? null,
                        'urls' => $videoUrls,
                        'video_id' => (string) ($movies[0]['id'] ?? ''),
                        'thumbnail_url' => $movies[0]['preview'] ?? null,
                        'title' => $movies[0]['name'] ?? null,
                        'metadata' => [
                            'source' => 'rawg_enrichment',
                            'rawg_id' => $this->rawgId,
                            'total_videos' => count($movies),
                            'videos' => $videoDetails,
                        ],
                    ]
                );
            }
        }
    }
}
