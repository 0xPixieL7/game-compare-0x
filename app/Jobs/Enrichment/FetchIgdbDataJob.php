<?php

declare(strict_types=1);

namespace App\Jobs\Enrichment;

use App\Jobs\Enrichment\Traits\CategorizesVideoTypes;
use App\Jobs\Enrichment\Traits\ExtractsStoreUrls;
use App\Models\Image;
use App\Models\Video;
use App\Models\VideoGame;
use App\Models\VideoGameTitleSource;
use App\Services\Igdb\IgdbMediaService;
use Illuminate\Bus\Queueable;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Bus\Dispatchable;
use Illuminate\Queue\InteractsWithQueue;
use Illuminate\Queue\Middleware\RateLimited;
use Illuminate\Queue\SerializesModels;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Log;

/**
 * Fetch comprehensive data from IGDB: media + store URLs for price discovery.
 *
 * IGDB is a goldmine for:
 * 1. High-quality media (covers, screenshots, artworks, YouTube videos)
 * 2. Store URLs (Steam, GOG, Epic, Itch) that enable cross-retailer price fetching
 *
 * When store URLs are discovered, this job dispatches price fetch jobs for each retailer.
 */
class FetchIgdbDataJob implements ShouldQueue
{
    use CategorizesVideoTypes, Dispatchable, ExtractsStoreUrls, InteractsWithQueue, Queueable, SerializesModels;

    public int $tries = 3;

    /** @var array<int, int> */
    public array $backoff = [30, 120, 300];

    public function __construct(
        public int $videoGameId,
        public int $igdbId
    ) {
    }

    /**
     * @return array<int, object>
     */
    public function middleware(): array
    {
        return [new RateLimited('igdb')];
    }

    public function handle(IgdbMediaService $igdb): void
    {
        $game = VideoGame::with('title')->find($this->videoGameId);

        if (! $game) {
            Log::warning('FetchIgdbDataJob: Game not found', ['game_id' => $this->videoGameId]);

            return;
        }

        // Fetch full data: media + store URLs
        $data = $igdb->fetchFullDataForGame($this->igdbId);

        if (empty($data['cover']) && empty($data['screenshots']) && empty($data['videos']) && empty($data['stores'])) {
            Log::info('FetchIgdbDataJob: No data returned from IGDB', [
                'game_id' => $this->videoGameId,
                'igdb_id' => $this->igdbId,
            ]);

            return;
        }

        DB::transaction(function () use ($game, $data) {
            // Store media
            $this->storeMedia($game, $data);

            // Store discovered store URLs as title sources for price discovery
            $this->storeDiscoveredSources($game, $data['stores'] ?? []);
        });

        // Dispatch price jobs for discovered stores
        $this->dispatchPriceJobs($game, $data['stores'] ?? []);

        Log::info('FetchIgdbDataJob: Complete', [
            'game_id' => $this->videoGameId,
            'igdb_id' => $this->igdbId,
            'has_cover' => $data['cover'] !== null,
            'screenshots' => count($data['screenshots'] ?? []),
            'artworks' => count($data['artworks'] ?? []),
            'videos' => count($data['videos'] ?? []),
            'stores_discovered' => count($data['stores'] ?? []),
        ]);
    }

    /**
     * Store media (images + videos) from IGDB.
     */
    private function storeMedia(VideoGame $game, array $data): void
    {
        $cover = $data['cover'] ?? null;
        $screenshots = $data['screenshots'] ?? [];
        $artworks = $data['artworks'] ?? [];
        $videos = $data['videos'] ?? [];

        // Build comprehensive image data
        if ($cover || ! empty($screenshots) || ! empty($artworks)) {
            $urls = [];
            $details = [];
            $collections = [];

            // Cover image
            if ($cover) {
                $collections[] = 'cover_images';
                $urls[] = $cover['url'];
                $details[] = [
                    'collection' => 'cover_images',
                    'image_id' => $cover['image_id'],
                    'url' => $cover['url'],
                    'size_variants' => $cover['size_variants'] ?? [],
                    'width' => $cover['width'] ?? null,
                    'height' => $cover['height'] ?? null,
                ];
            }

            // Screenshots
            foreach ($screenshots as $screenshot) {
                $collections[] = 'screenshots';
                $urls[] = $screenshot['url'];
                $details[] = [
                    'collection' => 'screenshots',
                    'image_id' => $screenshot['image_id'],
                    'url' => $screenshot['url'],
                    'size_variants' => $screenshot['size_variants'] ?? [],
                    'width' => $screenshot['width'] ?? null,
                    'height' => $screenshot['height'] ?? null,
                ];
            }

            // Artworks (high-quality promotional images)
            foreach ($artworks as $artwork) {
                $collections[] = 'artworks';
                $urls[] = $artwork['url'];
                $details[] = [
                    'collection' => 'artworks',
                    'image_id' => $artwork['image_id'],
                    'url' => $artwork['url'],
                    'size_variants' => $artwork['size_variants'] ?? [],
                    'width' => $artwork['width'] ?? null,
                    'height' => $artwork['height'] ?? null,
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
                    'provider' => 'igdb',
                    'primary_collection' => in_array('cover_images', $collections, true) ? 'cover_images' : ($collections[0] ?? 'misc'),
                    'collection_names' => $collections,
                    'url' => $urls[0] ?? null,
                    'urls' => $urls,
                    'metadata' => [
                        'source' => 'igdb_enrichment',
                        'igdb_id' => $this->igdbId,
                        'details' => $details,
                    ],
                ]
            );
        }

        // Store videos (YouTube links from IGDB)
        if (! empty($videos)) {
            $videoUrls = [];
            $videoDetails = [];
            $collections = [];

            foreach ($videos as $video) {
                $videoType = $this->categorizeVideoType($video['name'] ?? '');
                $collections[] = $videoType;

                $videoUrls[] = $video['url'];
                $videoDetails[] = [
                    'video_id' => $video['video_id'],
                    'name' => $video['name'] ?? null,
                    'type' => $videoType,
                    'provider' => $video['provider'] ?? 'youtube',
                    'url' => $video['url'],
                    'thumbnail_url' => $video['thumbnail_url'] ?? null,
                ];
            }

            $collections = array_values(array_unique($collections));

            Video::updateOrCreate(
                [
                    'video_game_id' => $game->id,
                    'videoable_type' => VideoGame::class,
                    'videoable_id' => $game->id,
                ],
                [
                    'provider' => 'igdb',
                    'primary_collection' => $collections[0] ?? 'trailers',
                    'collection_names' => $collections,
                    'url' => $videoUrls[0] ?? null,
                    'urls' => $videoUrls,
                    'video_id' => $videos[0]['video_id'] ?? null,
                    'thumbnail_url' => $videos[0]['thumbnail_url'] ?? null,
                    'title' => $videos[0]['name'] ?? null,
                    'metadata' => [
                        'source' => 'igdb_enrichment',
                        'igdb_id' => $this->igdbId,
                        'total_videos' => count($videos),
                        'videos' => $videoDetails,
                    ],
                ]
            );
        }
    }

    /**
     * Store discovered store URLs as title sources for future price lookups.
     *
     * This is the key value-add of IGDB: discovering retailer links we didn't know about.
     */
    private function storeDiscoveredSources(VideoGame $game, array $stores): void
    {
        if (empty($stores) || ! $game->title) {
            return;
        }

        foreach ($stores as $store) {
            $provider = $this->normalizeStoreProvider($store['store']);
            $externalId = $store['app_id'] ?? null;
            $url = $store['url'] ?? null;

            if (! $provider || ! $url) {
                continue;
            }

            // Only create if we don't already have this provider mapping
            VideoGameTitleSource::firstOrCreate(
                [
                    'video_game_title_id' => $game->title->id,
                    'provider' => $provider,
                ],
                [
                    'external_id' => $externalId,
                    'provider_item_id' => $externalId,
                    'provider_url' => $url,
                    'raw_payload' => [
                        'discovered_via' => 'igdb',
                        'igdb_id' => $this->igdbId,
                        'original_url' => $url,
                    ],
                ]
            );
        }
    }

    /**
     * Dispatch price fetch jobs for discovered stores.
     */
    private function dispatchPriceJobs(VideoGame $game, array $stores): void
    {
        foreach ($stores as $store) {
            $storeName = $store['store'] ?? '';
            $appId = $store['app_id'] ?? null;

            if (! $appId) {
                continue;
            }

            match ($storeName) {
                'steam' => ConcurrentFetchSteamDataJob::dispatch($game->id, (int) $appId),
                // Future: GOG, Epic, Itch price jobs
                // 'gog' => FetchGogPriceJob::dispatch($game->id, $appId),
                // 'epicgames' => FetchEpicPriceJob::dispatch($game->id, $appId),
                // 'itch' => FetchItchPriceJob::dispatch($game->id, $appId),
                default => null,
            };
        }
    }

}
