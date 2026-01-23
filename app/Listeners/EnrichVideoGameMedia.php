<?php

namespace App\Listeners;

use App\Events\VideoGameViewed;
use App\Jobs\EnrichVideoGameMediaJob;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Queue\InteractsWithQueue;
use Illuminate\Support\Facades\Cache;
use Illuminate\Support\Facades\Log;

class EnrichVideoGameMedia implements ShouldQueue
{
    use InteractsWithQueue;

    /**
     * The number of times the listener may be attempted.
     */
    public int $tries = 3;

    /**
     * The time (seconds) before the listener should timeout.
     */
    public int $timeout = 300;

    /**
     * Handle the event.
     */
    public function handle(VideoGameViewed $event): void
    {
        $videoGame = $event->videoGame;
        $cacheKey = "video_game_enriched_{$videoGame->id}";

        // Skip if recently enriched (within last 24 hours) unless force refresh
        if (!$event->forceRefresh && Cache::has($cacheKey)) {
            Log::info("Skipping enrichment for video game {$videoGame->id} - recently enriched");
            return;
        }

        // Dispatch enrichment job to queue
        EnrichVideoGameMediaJob::dispatch($videoGame)
            ->onQueue('media-enrichment');

        // Mark as enriched for 24 hours
        Cache::put($cacheKey, true, now()->addDay());

        Log::info("Queued media enrichment for video game {$videoGame->id}");
    }

    /**
     * Handle a job failure.
     */
    public function failed(VideoGameViewed $event, \Throwable $exception): void
    {
        Log::error("Failed to enrich video game {$event->videoGame->id}: {$exception->getMessage()}");
    }
}
