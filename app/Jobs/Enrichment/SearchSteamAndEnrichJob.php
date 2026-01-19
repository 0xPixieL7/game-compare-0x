<?php

declare(strict_types=1);

namespace App\Jobs\Enrichment;

use App\Models\VideoGame;
use App\Models\VideoGameTitleSource;
use App\Services\Price\Steam\SteamStoreService;
use Illuminate\Bus\Queueable;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Bus\Dispatchable;
use Illuminate\Queue\InteractsWithQueue;
use Illuminate\Queue\Middleware\RateLimited;
use Illuminate\Queue\SerializesModels;
use Illuminate\Support\Facades\Log;

/**
 * Search Steam by name and dispatch enrichment if found.
 *
 * This job:
 * 1. Searches Steam Store API by game name (uses local JSON first)
 * 2. If a match is found, creates a VideoGameTitleSource mapping
 * 3. Dispatches ConcurrentFetchSteamDataJob for prices + media
 *
 * Used for games that don't have a Steam mapping yet.
 */
class SearchSteamAndEnrichJob implements ShouldQueue
{
    use Dispatchable, InteractsWithQueue, Queueable, SerializesModels;

    public int $tries = 2;

    /** @var array<int, int> */
    public array $backoff = [30, 120];

    public function __construct(
        public int $videoGameId,
        public string $gameName
    ) {}

    /**
     * @return array<int, object>
     */
    public function middleware(): array
    {
        return [new RateLimited('steam')];
    }

    public function handle(SteamStoreService $steam): void
    {
        $game = VideoGame::with('title')->find($this->videoGameId);

        if (! $game || ! $game->title) {
            return;
        }

        // Check if we already have a Steam mapping (race condition protection)
        $existingSource = $game->title->sources()
            ->whereIn('provider', ['steam', 'steam_store'])
            ->exists();

        if ($existingSource) {
            Log::debug('SearchSteamAndEnrichJob: Steam mapping already exists', [
                'game_id' => $this->videoGameId,
            ]);

            return;
        }

        // Search Steam (local JSON first, then API)
        $steamId = $steam->search($this->gameName);

        if (! $steamId) {
            Log::info('SearchSteamAndEnrichJob: No Steam match found', [
                'game_id' => $this->videoGameId,
                'game_name' => $this->gameName,
            ]);

            return;
        }

        // Create the mapping
        VideoGameTitleSource::firstOrCreate(
            [
                'video_game_title_id' => $game->title->id,
                'provider' => 'steam_store',
            ],
            [
                'external_id' => (string) $steamId,
                'provider_item_id' => (string) $steamId,
                'provider_url' => "https://store.steampowered.com/app/{$steamId}/",
                'raw_payload' => [
                    'discovered_via' => 'search',
                    'search_term' => $this->gameName,
                ],
            ]
        );

        // Dispatch the enrichment job
        ConcurrentFetchSteamDataJob::dispatch($this->videoGameId, $steamId)
            ->onQueue('prices-steam');

        Log::info('SearchSteamAndEnrichJob: Found and dispatched', [
            'game_id' => $this->videoGameId,
            'game_name' => $this->gameName,
            'steam_id' => $steamId,
        ]);
    }
}
