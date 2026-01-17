<?php

declare(strict_types=1);

namespace App\Observers;

use App\Jobs\Propagation\PropagateSourceMetadataJob;
// use App\Jobs\Propagation\UpdateSourceItemCountJob;
use App\Models\VideoGameTitleSource;
use Illuminate\Support\Facades\Log;

/**
 * VideoGameTitleSource Observer - Automatic Data Propagation.
 *
 * This observer ensures that whenever a provider source is created or updated,
 * its data automatically propagates to:
 * - VideoGame (canonical metadata)
 * - Images/Videos (aggregated media)
 * - VideoGameSource (item counts)
 *
 * Pattern: Event → Queue Job → Propagate
 * Idempotent: All jobs are safe to retry
 * Atomic: Uses database transactions
 */
class VideoGameTitleSourceObserver
{
    /**
     * Handle the VideoGameTitleSource "created" event.
     *
     * Triggers:
     * - Metadata propagation to VideoGame
     * - Media propagation to Images/Videos
     * - Source item count increment
     */
    public function created(VideoGameTitleSource $videoGameTitleSource): void
    {
        Log::info('VideoGameTitleSource created, dispatching propagation jobs', [
            'source_id' => $videoGameTitleSource->id,
            'provider' => $videoGameTitleSource->provider,
            'title_id' => $videoGameTitleSource->video_game_title_id,
        ]);

        // Dispatch metadata propagation (will also dispatch media propagation)
        PropagateSourceMetadataJob::dispatch($videoGameTitleSource->id);

        // Update source item count
        if ($videoGameTitleSource->video_game_source_id) {
            // UpdateSourceItemCountJob::dispatch(
            //     $videoGameTitleSource->video_game_source_id,
            //     'increment'
            // );
        }
    }

    /**
     * Handle the VideoGameTitleSource "updated" event.
     *
     * Re-triggers propagation to ensure canonical data stays fresh.
     */
    public function updated(VideoGameTitleSource $videoGameTitleSource): void
    {
        // Only re-propagate if meaningful fields changed
        $watchedFields = [
            'name', 'description', 'rating', 'release_date',
            'developer', 'publisher', 'genre', 'platform',
        ];

        if ($videoGameTitleSource->wasChanged($watchedFields)) {
            Log::info('VideoGameTitleSource updated, re-dispatching propagation', [
                'source_id' => $videoGameTitleSource->id,
                'changed' => array_keys($videoGameTitleSource->getChanges()),
            ]);

            PropagateSourceMetadataJob::dispatch($videoGameTitleSource->id);
        }
    }

    /**
     * Handle the VideoGameTitleSource "deleted" event.
     *
     * Decrements source item count.
     * Note: Cascade deletes handle related records automatically.
     */
    public function deleted(VideoGameTitleSource $videoGameTitleSource): void
    {
        if ($videoGameTitleSource->video_game_source_id) {
            // UpdateSourceItemCountJob::dispatch(
            //     $videoGameTitleSource->video_game_source_id,
            //     'decrement'
            // );
        }

        Log::info('VideoGameTitleSource deleted', [
            'source_id' => $videoGameTitleSource->id,
            'provider' => $videoGameTitleSource->provider,
        ]);
    }

    /**
     * Handle the VideoGameTitleSource "restored" event.
     *
     * Re-triggers propagation after soft delete restoration.
     */
    public function restored(VideoGameTitleSource $videoGameTitleSource): void
    {
        PropagateSourceMetadataJob::dispatch($videoGameTitleSource->id);

        if ($videoGameTitleSource->video_game_source_id) {
            UpdateSourceItemCountJob::dispatch(
                $videoGameTitleSource->video_game_source_id,
                'increment'
            );
        }
    }

    /**
     * Handle the VideoGameTitleSource "force deleted" event.
     */
    public function forceDeleted(VideoGameTitleSource $videoGameTitleSource): void
    {
        // Same as deleted - cascade handles cleanup
    }
}
