<?php

declare(strict_types=1);

namespace App\Jobs\Propagation;

use App\Models\Image;
use App\Models\Video;
use App\Models\VideoGame;
use Illuminate\Bus\Queueable;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Bus\Dispatchable;
use Illuminate\Queue\InteractsWithQueue;
use Illuminate\Queue\SerializesModels;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Log;

/**
 * Propagate media (images/videos) from VideoGameTitleSource to VideoGame.
 *
 * This job consolidates media from all provider sources into the canonical
 * VideoGame's aggregated Image/Video records.
 *
 * Strategy: Merge all sources, deduplicate by checksum/external_id
 * Idempotent: Safe to run multiple times
 * Atomic: Uses transactions to prevent partial updates
 */
class PropagateMediaToVideoGameJob implements ShouldQueue
{
    use Dispatchable;
    use InteractsWithQueue;
    use Queueable;
    use SerializesModels;

    public function __construct(
        public int $videoGameId
    ) {
        //
    }

    /**
     * Execute the job.
     */
    public function handle(): void
    {
        $videoGame = VideoGame::with('title.titleSources.images')->find($this->videoGameId);

        if (! $videoGame) {
            Log::warning('VideoGame not found for media propagation', ['id' => $this->videoGameId]);

            return;
        }

        DB::transaction(function () use ($videoGame) {
            // Collect all images from all title sources
            $allImages = $videoGame->title->titleSources()
                ->with('images')
                ->get()
                ->flatMap(fn ($source) => $source->images);

            if ($allImages->isNotEmpty()) {
                $this->propagateImages($videoGame, $allImages);
            }

            // TODO: Propagate videos (similar pattern)
            // $this->propagateVideos($videoGame, $allVideos);

            Log::info('Media propagated to VideoGame', [
                'video_game_id' => $videoGame->id,
                'images_count' => $allImages->count(),
            ]);
        });
    }

    /**
     * Propagate images to VideoGame's aggregated Image record.
     */
    private function propagateImages(VideoGame $videoGame, $sourceImages): void
    {
        // Group by collection
        $grouped = $sourceImages->groupBy(fn ($img) => $img->primary_collection ?? 'screenshots');

        // Build aggregated metadata
        $allDetails = [];
        $collections = [];
        $urls = [];

        foreach ($grouped as $collection => $images) {
            $collections[] = $collection;

            foreach ($images as $img) {
                $allDetails[] = [
                    'collection' => $collection,
                    'url' => $img->url,
                    'size_variants' => $img->urls ?? [$img->url],
                    'width' => $img->width,
                    'height' => $img->height,
                    'image_id' => $img->external_id,
                    'checksum' => $img->metadata['checksum'] ?? null,
                ];

                if ($img->url) {
                    $urls[] = $img->url;
                }
                if ($img->urls) {
                    $urls = array_merge($urls, $img->urls);
                }
            }
        }

        // Deduplicate URLs
        $urls = array_values(array_unique($urls));

        // Create or update aggregated Image record
        Image::updateOrCreate(
            [
                'video_game_id' => $videoGame->id,
                'imageable_type' => VideoGame::class,
                'imageable_id' => $videoGame->id,
            ],
            [
                'collection_names' => array_values(array_unique($collections)),
                'primary_collection' => $collections[0] ?? 'screenshots',
                'url' => $allDetails[0]['url'] ?? null,
                'urls' => $urls,
                'metadata' => [
                    'all_details' => $allDetails,
                    'collections' => array_values(array_unique($collections)),
                ],
                'provider' => 'aggregated',
            ]
        );
    }

    /**
     * Propagate videos to VideoGame's aggregated Video record.
     */
    private function propagateVideos(VideoGame $videoGame, $sourceVideos): void
    {
        // TODO: Implement video propagation
        // Similar pattern to images
    }

    /**
     * Get the tags that should be assigned to the job.
     *
     * @return array<int, string>
     */
    public function tags(): array
    {
        return [
            'propagation',
            'media',
            "video_game:{$this->videoGameId}",
        ];
    }
}
