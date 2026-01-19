<?php

declare(strict_types=1);

namespace App\Jobs\Enrichment;

use App\Jobs\Enrichment\Traits\CategorizesVideoTypes;
use App\Models\Image;
use App\Models\VideoGame;
use App\Services\Tgdb\TgdbClient;
use Illuminate\Bus\Queueable;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Bus\Dispatchable;
use Illuminate\Queue\InteractsWithQueue;
use Illuminate\Queue\Middleware\RateLimited;
use Illuminate\Queue\SerializesModels;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Log;

/**
 * Fetch media from TheGamesDB (TGDB).
 *
 * TGDB provides high-quality boxart, screenshots, fanart, and logos.
 * This job runs as a secondary media source alongside IGDB.
 */
class FetchTgdbDataJob implements ShouldQueue
{
    use CategorizesVideoTypes, Dispatchable, InteractsWithQueue, Queueable, SerializesModels;

    public int $tries = 3;

    /** @var array<int, int> */
    public array $backoff = [30, 120, 300];

    public function __construct(
        public int $videoGameId,
        public int $tgdbId
    ) {}

    /**
     * @return array<int, object>
     */
    public function middleware(): array
    {
        return [new RateLimited('tgdb')];
    }

    public function handle(TgdbClient $tgdb): void
    {
        $game = VideoGame::find($this->videoGameId);

        if (! $game) {
            Log::warning('FetchTgdbDataJob: Game not found', ['game_id' => $this->videoGameId]);

            return;
        }

        // Check if we already have TGDB media (don't overwrite)
        $existingTgdbMedia = Image::where('video_game_id', $game->id)
            ->where('provider', 'tgdb')
            ->exists();

        if ($existingTgdbMedia) {
            Log::debug('FetchTgdbDataJob: TGDB media already exists', [
                'game_id' => $this->videoGameId,
            ]);

            return;
        }

        // Fetch images from TGDB
        $response = $tgdb->getImages([$this->tgdbId]);
        $data = $response['data'] ?? [];

        $baseUrl = $data['base_url']['original'] ?? '';
        $images = $data['images'][$this->tgdbId] ?? [];

        if (empty($images)) {
            Log::info('FetchTgdbDataJob: No TGDB media found', [
                'game_id' => $this->videoGameId,
                'tgdb_id' => $this->tgdbId,
            ]);

            return;
        }

        DB::transaction(function () use ($game, $images, $baseUrl) {
            $this->storeMedia($game, $images, $baseUrl);
        });

        Log::info('FetchTgdbDataJob: Complete', [
            'game_id' => $this->videoGameId,
            'tgdb_id' => $this->tgdbId,
            'images_count' => count($images),
        ]);
    }

    /**
     * Store TGDB images in the database.
     */
    private function storeMedia(VideoGame $game, array $images, string $baseUrl): void
    {
        $urls = [];
        $collections = [];
        $details = [];

        foreach ($images as $img) {
            $type = $img['type'] ?? 'unknown';
            $collection = match ($type) {
                'boxart' => 'cover_images',
                'screenshot' => 'screenshots',
                'fanart' => 'artworks',
                'clearlogo' => 'logos',
                'banner' => 'banners',
                'titlescreen' => 'titlescreens',
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

            $details[] = [
                'collection' => $collection,
                'url' => $url,
                'type' => $type,
                'side' => $img['side'] ?? null,
                'width' => $width ? (int) $width : null,
                'height' => $height ? (int) $height : null,
                'tgdb_id' => $img['id'] ?? null,
            ];
        }

        $collections = array_values(array_unique($collections));

        Image::updateOrCreate(
            [
                'video_game_id' => $game->id,
                'imageable_type' => VideoGame::class,
                'imageable_id' => $game->id,
                'provider' => 'tgdb',
            ],
            [
                'primary_collection' => in_array('cover_images', $collections, true)
                    ? 'cover_images'
                    : ($collections[0] ?? 'misc'),
                'collection_names' => $collections,
                'url' => $urls[0] ?? null,
                'urls' => $urls,
                'metadata' => [
                    'source' => 'tgdb_enrichment',
                    'tgdb_id' => $this->tgdbId,
                    'total_images' => count($images),
                    'details' => $details,
                ],
            ]
        );
    }
}
