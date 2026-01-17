<?php

declare(strict_types=1);

namespace App\Jobs\Enrichment;

use Illuminate\Bus\Queueable;
use Illuminate\Contracts\Queue\ShouldBeUnique;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Bus\Dispatchable;
use Illuminate\Queue\InteractsWithQueue;
use Illuminate\Queue\SerializesModels;
use Illuminate\Support\Facades\Cache;
use Illuminate\Support\Facades\Log;

/**
 * Enrichment Coordinator Job.
 *
 * This job acts as a Saga/Director for game enrichment.
 * It is triggered when a VideoGame is created or significantly updated.
 *
 * Responsibilities:
 * 1. Concurrency control (Locking)
 * 2. Dispatching sub-jobs (Prices, Media)
 *
 * Performance:
 * - Uses atomic locks to prevent duplicate processing within a window
 * - Dispatches parallel sub-jobs to maximize throughput
 */
class EnrichVideoGameJob implements ShouldBeUnique, ShouldQueue
{
    use Dispatchable, InteractsWithQueue, Queueable, SerializesModels;

    /**
     * The unique ID of the job.
     */
    public function uniqueId(): string
    {
        return (string) $this->videoGameId;
    }

    public function __construct(
        public int $videoGameId
    ) {
    }

    public function handle(): void
    {
        // 1. Atomic Lock to prevent race conditions
        // Prevents multiple workers from enriching the same game simultaneously
        $lockKey = "enrich_game:{$this->videoGameId}";

        $lock = Cache::lock($lockKey, 60); // 60s lock

        if (! $lock->get()) {
            Log::info("Enrichment skipped for game {$this->videoGameId}: Locked");

            return;
        }

        try {
            Log::info("Starting enrichment for game {$this->videoGameId}");

            // 2. Dispatch Parallel Sub-Jobs
            // We fire-and-forget these; they run on their own queues/workers

            // A. Prices (Steam, PSN, Xbox, etc.)
            EnrichGamePricesJob::dispatch($this->videoGameId);

            // B. Media (IGDB images/videos)
            EnrichGameMediaJob::dispatch($this->videoGameId);

        } finally {
            $lock->release();
        }
    }
}
