<?php

declare(strict_types=1);

namespace App\Jobs;

use Illuminate\Bus\Queueable;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Bus\Dispatchable;
use Illuminate\Queue\InteractsWithQueue;
use Illuminate\Queue\SerializesModels;

class PropagateMediaAndEnrichmentJob implements ShouldQueue
{
    use Dispatchable, InteractsWithQueue, Queueable, SerializesModels;

    public array $gameIds;

    public bool $enrichMissing;

    /**
     * Create a new job instance.
     */
    public function __construct(array $gameIds, bool $enrichMissing = true)
    {
        $this->gameIds = $gameIds;
        $this->enrichMissing = $enrichMissing;
    }

    /**
     * Execute the job.
     */
    public function handle(): void
    {
        // TODO: Implement logic to propagate media (images/videos) and enrichment lookups
        // for each game in $this->gameIds. Fill in missing data from other providers.
    }
}
