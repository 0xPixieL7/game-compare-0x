<?php

declare(strict_types=1);

namespace App\Observers;

use App\Jobs\Enrichment\EnrichVideoGameJob;
use App\Models\VideoGame;

/**
 * VideoGame Observer.
 *
 * Handles lifecycle events for the canonical VideoGame model.
 * Dispatches enrichment jobs when games are created or critical fields change.
 *
 * Performance Note:
 * Logic here must remain minimal. No DB queries, no external API calls.
 * Job dispatching is acceptable as it's async.
 */
class VideoGameObserver
{
    /**
     * Identity fields that trigger enrichment when changed.
     */
    private const IDENTITY_FIELDS = ['name', 'provider', 'external_id'];

    /**
     * Handle the VideoGame "created" event.
     */
    public function created(VideoGame $videoGame): void
    {
        EnrichVideoGameJob::dispatch($videoGame->id);
    }

    /**
     * Handle the VideoGame "updated" event.
     */
    public function updated(VideoGame $videoGame): void
    {
        // Debounce logic: Only enrich if critical identity fields changed.
        // This prevents infinite loops and unnecessary enrichment runs.
        if ($videoGame->wasChanged(self::IDENTITY_FIELDS)) {
            EnrichVideoGameJob::dispatch($videoGame->id);
        }
    }
}
