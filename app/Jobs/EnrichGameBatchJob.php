<?php

declare(strict_types=1);

namespace App\Jobs;

use App\Models\VideoGame;
use App\Services\Price\GameDataAggregatorService;
use Illuminate\Bus\Queueable;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Bus\Dispatchable;
use Illuminate\Queue\InteractsWithQueue;
use Illuminate\Queue\SerializesModels;
use Illuminate\Support\Facades\Log;

class EnrichGameBatchJob implements ShouldQueue
{
    use \App\Console\Commands\Enrich\Concerns\InteractsWithEnrichmentProviders;
    use Dispatchable, InteractsWithQueue, Queueable, SerializesModels;

    public $timeout = 300; // 5 minutes per batch

    public $tries = 2;

    /**
     * Create a new job instance.
     */
    public function __construct(
        public array $gameIds,
        public bool $skipPrices = false,
        public bool $skipMedia = false,
    ) {
        //
    }

    /**
     * Execute the job.
     */
    public function handle(
        GameDataAggregatorService $aggregator,
        \App\Services\Price\Steam\SteamStoreService $steam,
        \App\Services\Price\Xbox\XboxStoreService $xbox,
        \App\Services\Price\PlayStation\PlayStationStoreService $playstation
    ): void {
        // Optimization: Fetch all games in one query
        $games = VideoGame::whereIn('id', $this->gameIds)->get();

        foreach ($games as $game) {
            try {
                // Resolve external IDs
                $attributes = match (true) {
                    is_array($game->attributes) => $game->attributes,
                    is_string($game->attributes) => json_decode($game->attributes, true) ?? [],
                    default => [],
                };

                $attributes = $this->resolveIdsFromExternalLinks($game, $attributes);
                $game->attributes = $attributes;
                $game->save();

                // Enrich with providers
                if (! $this->skipPrices) {
                    // Pass false for search to avoid slow scraping in bulk jobs
                    $this->enrichWithSteam($steam, $aggregator, $game, false, false);
                    $this->enrichWithXbox($xbox, $aggregator, $game, false, false);
                    $this->enrichWithPlayStation($playstation, $aggregator, $game, false, false);
                }

                // Media enrichment
                if (! $this->skipMedia) {
                    EnrichVideoGameMediaJob::dispatch($game);
                }
            } catch (\Exception $e) {
                Log::error("Batch job failed for game {$game->id}: ".$e->getMessage());
            }
        }

        Log::info('Batch completed: '.count($this->gameIds).' games processed.');
    }
}
