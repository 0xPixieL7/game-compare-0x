<?php

declare(strict_types=1);

namespace App\Jobs\Enrichment;

use App\Models\Image;
use App\Models\Video;
use App\Models\VideoGame;
use App\Services\Igdb\IgdbMediaService;
use App\Services\Provider\ProviderDiscoveryService;
use App\Services\Tgdb\TgdbClient;
use Illuminate\Bus\Queueable;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Bus\Dispatchable;
use Illuminate\Queue\InteractsWithQueue;
use Illuminate\Queue\Middleware\RateLimited;
use Illuminate\Queue\SerializesModels;
use Illuminate\Support\Facades\Log;

/**
 * Enrich VideoGame with media from IGDB (primary) or TGDB (fallback).
 *
 * Uses ProviderDiscoveryService to check for IGDB mapping first,
 * falls back to TGDB if no IGDB mapping exists.
 */
class EnrichGameMediaJob implements ShouldQueue
{
    use Dispatchable, InteractsWithQueue, Queueable, SerializesModels;

    public int $tries = 3;

    /** @var array<int, int> */
    public array $backoff = [60, 300, 900];

    public function __construct(
        public int $videoGameId
    ) {}

    /**
     * Rate limit based on which provider we use.
     * IGDB is tried first, so we rate-limit for IGDB.
     */
    public function middleware(): array
    {
        return [new RateLimited('igdb')];
    }

    public function handle(
        ProviderDiscoveryService $discovery,
        IgdbMediaService $igdb,
        TgdbClient $tgdb
    ): void {
        $game = VideoGame::with(['images', 'title.sources'])->find($this->videoGameId);

        if (! $game) {
            Log::warning('EnrichGameMediaJob: Game not found', ['game_id' => $this->videoGameId]);

            return;
        }

        // Skip if we already have a cover image
        if ($game->images && $game->images->primary_collection === 'cover_images') {
            Log::info('EnrichGameMediaJob: Game already has cover', ['game_id' => $game->id]);

            return;
        }

        // Strategy 1: Try IGDB first (highest quality media)
        $igdbId = $discovery->getExternalId($game, 'igdb');

        if ($igdbId) {
            $enriched = $this->enrichFromIgdb($game, $igdbId, $igdb);
            if ($enriched) {
                return;
            }
        }

        // Strategy 2: Fall back to TGDB
        $tgdbId = $discovery->getExternalId($game, 'tgdb');

        if ($tgdbId) {
            $this->enrichFromTgdb($game, $tgdbId, $tgdb);

            return;
        }

        // Strategy 3: Search TGDB by name
        $this->searchAndEnrichFromTgdb($game, $tgdb);
    }

    /**
     * Enrich media from IGDB API.
     */
    private function enrichFromIgdb(VideoGame $game, int $igdbId, IgdbMediaService $igdb): bool
    {
        $media = $igdb->fetchMediaForGame($igdbId);

        if (! $media['cover'] && empty($media['screenshots']) && empty($media['artworks'])) {
            Log::info('EnrichGameMediaJob: No IGDB media found', [
                'game_id' => $game->id,
                'igdb_id' => $igdbId,
            ]);

            return false;
        }

        $this->storeIgdbMedia($game, $media);

        Log::info('EnrichGameMediaJob: Enriched from IGDB', [
            'game_id' => $game->id,
            'igdb_id' => $igdbId,
            'has_cover' => $media['cover'] !== null,
            'screenshots' => count($media['screenshots']),
            'artworks' => count($media['artworks']),
            'videos' => count($media['videos']),
        ]);

        return true;
    }

    /**
     * Store IGDB media in the database.
     */
    private function storeIgdbMedia(VideoGame $game, array $media): void
    {
        $urls = [];
        $collections = [];
        $details = [];

        // Process cover
        if ($media['cover']) {
            $collections[] = 'cover_images';
            $urls[] = $media['cover']['url'];
            $details[] = [
                'collection' => 'cover_images',
                'url' => $media['cover']['url'],
                'size_variants' => $media['cover']['size_variants'],
                'width' => $media['cover']['width'],
                'height' => $media['cover']['height'],
                'image_id' => $media['cover']['image_id'],
                'checksum' => $media['cover']['checksum'],
            ];
        }

        // Process screenshots
        foreach ($media['screenshots'] as $screenshot) {
            $collections[] = 'screenshots';
            $urls[] = $screenshot['url'];
            $details[] = [
                'collection' => 'screenshots',
                'url' => $screenshot['url'],
                'size_variants' => $screenshot['size_variants'],
                'width' => $screenshot['width'],
                'height' => $screenshot['height'],
                'image_id' => $screenshot['image_id'],
                'checksum' => $screenshot['checksum'],
            ];
        }

        // Process artworks
        foreach ($media['artworks'] as $artwork) {
            $collections[] = 'artworks';
            $urls[] = $artwork['url'];
            $details[] = [
                'collection' => 'artworks',
                'url' => $artwork['url'],
                'size_variants' => $artwork['size_variants'],
                'width' => $artwork['width'],
                'height' => $artwork['height'],
                'image_id' => $artwork['image_id'],
                'checksum' => $artwork['checksum'],
            ];
        }

        $collections = array_values(array_unique($collections));

        // Upsert Image record
        if (! empty($urls)) {
            Image::updateOrCreate(
                [
                    'video_game_id' => $game->id,
                    'imageable_type' => VideoGame::class,
                    'imageable_id' => $game->id,
                ],
                [
                    'provider' => 'igdb',
                    'primary_collection' => in_array('cover_images', $collections, true) ? 'cover_images' : ($collections[0] ?? 'misc'),
                    'collection_names' => $collections,
                    'url' => $urls[0] ?? null,
                    'urls' => $urls,
                    'metadata' => [
                        'source' => 'igdb_enrichment',
                        'details' => $details,
                    ],
                ]
            );
        }

        // Process videos
        if (! empty($media['videos'])) {
            $videoUrls = array_map(fn ($v) => $v['url'], $media['videos']);

            Video::updateOrCreate(
                [
                    'video_game_id' => $game->id,
                    'videoable_type' => VideoGame::class,
                    'videoable_id' => $game->id,
                ],
                [
                    'provider' => 'youtube',
                    'primary_collection' => 'trailers',
                    'collection_names' => ['trailers'],
                    'url' => $videoUrls[0] ?? null,
                    'urls' => $videoUrls,
                    'video_id' => $media['videos'][0]['video_id'] ?? null,
                    'thumbnail_url' => $media['videos'][0]['thumbnail_url'] ?? null,
                    'title' => $media['videos'][0]['name'] ?? null,
                    'metadata' => [
                        'source' => 'igdb_enrichment',
                        'videos' => $media['videos'],
                    ],
                ]
            );
        }
    }

    /**
     * Enrich media from TGDB API.
     */
    private function enrichFromTgdb(VideoGame $game, int $tgdbId, TgdbClient $tgdb): void
    {
        $response = $tgdb->getImages([$tgdbId]);
        $data = $response['data'] ?? [];

        $baseUrl = $data['base_url']['original'] ?? '';
        $images = $data['images'][$tgdbId] ?? [];

        if (empty($images)) {
            Log::info('EnrichGameMediaJob: No TGDB media found', [
                'game_id' => $game->id,
                'tgdb_id' => $tgdbId,
            ]);

            return;
        }

        $this->storeTgdbImages($game, $images, $baseUrl);

        Log::info('EnrichGameMediaJob: Enriched from TGDB', [
            'game_id' => $game->id,
            'tgdb_id' => $tgdbId,
            'image_count' => count($images),
        ]);
    }

    /**
     * Search TGDB by name and enrich if found.
     */
    private function searchAndEnrichFromTgdb(VideoGame $game, TgdbClient $tgdb): void
    {
        if (! $game->name) {
            return;
        }

        $results = $tgdb->getGamesByName($game->name);
        $games = $results['data']['games'] ?? [];

        if (empty($games)) {
            Log::info('EnrichGameMediaJob: No TGDB search results', [
                'game_id' => $game->id,
                'game_name' => $game->name,
            ]);

            return;
        }

        $tgdbId = (int) $games[0]['id'];
        $this->enrichFromTgdb($game, $tgdbId, $tgdb);
    }

    /**
     * Store TGDB images in the database.
     */
    private function storeTgdbImages(VideoGame $game, array $images, string $baseUrl): void
    {
        $processed = [];
        $urls = [];
        $collections = [];

        foreach ($images as $img) {
            $type = $img['type'] ?? 'unknown';
            $collection = match ($type) {
                'boxart' => 'cover_images',
                'screenshot' => 'screenshots',
                'fanart' => 'artworks',
                'clearlogo' => 'logos',
                default => 'misc'
            };

            $filename = $img['filename'] ?? '';
            $url = $baseUrl.$filename;

            $collections[] = $collection;
            $urls[] = $url;

            $resolution = $img['resolution'] ?? null;
            $width = null;
            $height = null;

            if ($resolution && str_contains($resolution, 'x')) {
                [$width, $height] = explode('x', $resolution);
            }

            $processed[] = [
                'collection' => $collection,
                'url' => $url,
                'width' => $width,
                'height' => $height,
                'image_id' => $img['id'] ?? null,
                'source' => 'tgdb',
            ];
        }

        $collections = array_values(array_unique($collections));

        Image::updateOrCreate(
            [
                'video_game_id' => $game->id,
                'imageable_type' => VideoGame::class,
                'imageable_id' => $game->id,
            ],
            [
                'provider' => 'tgdb',
                'primary_collection' => in_array('cover_images', $collections, true) ? 'cover_images' : ($collections[0] ?? 'misc'),
                'collection_names' => $collections,
                'url' => $processed[0]['url'] ?? null,
                'urls' => $urls,
                'metadata' => [
                    'source' => 'tgdb_enrichment',
                    'details' => $processed,
                ],
            ]
        );
    }
}
