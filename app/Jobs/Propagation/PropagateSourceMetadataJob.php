<?php

declare(strict_types=1);

namespace App\Jobs\Propagation;

use App\Models\VideoGame;
use App\Models\VideoGameTitleSource;
use Illuminate\Bus\Queueable;
use Illuminate\Contracts\Queue\ShouldBeUnique;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Bus\Dispatchable;
use Illuminate\Queue\InteractsWithQueue;
use Illuminate\Queue\SerializesModels;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Log;

/**
 * Propagate metadata from VideoGameTitleSource to VideoGame (canonical).
 *
 * This job handles the core data propagation pattern: source → canonical.
 * It merges metadata from provider-specific sources into the canonical VideoGame record.
 *
 * Strategy: Priority-based merge (IGDB > Steam > Others)
 * Idempotent: Safe to run multiple times
 * Chunked: Processes in batches for large datasets
 *
 * Performance Requirements:
 * - NO N+1 queries (uses selective eager loading)
 * - Transactional writes (DB::transaction)
 * - Implements ShouldBeUnique for deduplication
 */
class PropagateSourceMetadataJob implements ShouldQueue, ShouldBeUnique
{
    use Dispatchable;
    use InteractsWithQueue;
    use Queueable;
    use SerializesModels;

    /**
     * The number of seconds after which the job's unique lock will be released.
     */
    public int $uniqueFor = 60;

    /**
     * The number of times the job may be attempted.
     */
    public int $tries = 3;

    /**
     * The maximum number of seconds the job can run before timing out.
     */
    public int $timeout = 120;

    public function __construct(
        public readonly int $sourceId
    ) {
        $this->onQueue('enrichment');
    }

    /**
     * Get the unique ID for the job.
     */
    public function uniqueId(): string
    {
        return "propagate-source-{$this->sourceId}";
    }

    /**
     * Execute the job.
     *
     * Performance Optimizations:
     * - Uses select() for selective column loading
     * - Eager loads only required relationships
     * - Wrapped in transaction for atomicity
     */
    public function handle(): void
    {
        // Selective column fetching to avoid loading large raw_payload unnecessarily
        $source = VideoGameTitleSource::query()
            ->select([
                'id', 'video_game_title_id', 'provider', 'external_id',
                'name', 'rating', 'release_date', 'description',
                'developer', 'publisher', 'genre', 'platform', 'rating_count',
            ])
            ->with(['title:id,name'])
            ->find($this->sourceId);

        if (! $source) {
            Log::warning('VideoGameTitleSource not found', ['id' => $this->sourceId]);

            return;
        }

        DB::transaction(function () use ($source) {
            // Get or create canonical VideoGame record for this title
            $videoGame = VideoGame::query()->firstOrCreate(
                [
                    'video_game_title_id' => $source->video_game_title_id,
                    'provider' => $source->provider,
                    'external_id' => $source->external_id,
                ],
                ['name' => $source->name]
            );

            // Priority-based merge strategy
            $priority = $this->getProviderPriority($source->provider);
            $currentPriority = $this->getProviderPriority($videoGame->provider ?? '');

            // Merge metadata (only if source has higher or equal priority)
            if ($priority >= $currentPriority) {
                $videoGame->fill([
                    'name' => $source->name ?? $videoGame->name,
                    'rating' => $source->rating ?? $videoGame->rating,
                    'release_date' => $source->release_date ?? $videoGame->release_date,
                    'attributes' => array_merge(
                        $videoGame->attributes ?? [],
                        array_filter([
                            'description' => $source->description,
                            'developer' => $source->developer,
                            'publisher' => $source->publisher,
                            'genre' => $source->genre,
                            'platform' => $source->platform,
                            'rating_count' => $source->rating_count,
                        ])
                    ),
                ]);

                $videoGame->save();

                Log::info('Metadata propagated to VideoGame', [
                    'source_id' => $source->id,
                    'video_game_id' => $videoGame->id,
                    'provider' => $source->provider,
                ]);
            }

            // Dispatch media propagation job
            PropagateMediaToVideoGameJob::dispatch($videoGame->id);
        });
    }

    /**
     * Get provider priority for merge strategy.
     * Higher number = higher priority (wins conflicts).
     */
    private function getProviderPriority(string $provider): int
    {
        return match (strtolower($provider)) {
            'igdb' => 100,
            'steam' => 80,
            'playstation' => 70,
            'xbox' => 70,
            'gog' => 60,
            'epic' => 60,
            default => 50,
        };
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
            'metadata',
            "source:{$this->sourceId}",
        ];
    }
}
