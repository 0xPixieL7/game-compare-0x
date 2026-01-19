<?php

declare(strict_types=1);

namespace App\Jobs\Enrichment;

use App\Models\VideoGame;
use App\Models\VideoGameTitleSource;
use Illuminate\Bus\Queueable;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Bus\Dispatchable;
use Illuminate\Queue\InteractsWithQueue;
use Illuminate\Queue\Middleware\RateLimited;
use Illuminate\Queue\SerializesModels;
use Illuminate\Support\Facades\Cache;
use Illuminate\Support\Facades\Http;
use Illuminate\Support\Facades\Log;

/**
 * Search IGDB by name and dispatch enrichment if found.
 *
 * This job:
 * 1. Searches IGDB API by game name
 * 2. If a match is found, creates a VideoGameTitleSource mapping
 * 3. Dispatches FetchIgdbDataJob for the discovered IGDB ID
 *
 * Used for games that don't have an IGDB mapping yet.
 */
class SearchIgdbAndEnrichJob implements ShouldQueue
{
    use Dispatchable, InteractsWithQueue, Queueable, SerializesModels;

    private const BASE_URL = 'https://api.igdb.com/v4';

    private const TOKEN_CACHE_KEY = 'igdb_oauth_token';

    public int $tries = 2;

    /** @var array<int, int> */
    public array $backoff = [60, 300];

    public function __construct(
        public int $videoGameId,
        public string $gameName
    ) {}

    /**
     * @return array<int, object>
     */
    public function middleware(): array
    {
        return [new RateLimited('igdb')];
    }

    public function handle(): void
    {
        $game = VideoGame::with('title')->find($this->videoGameId);

        if (! $game || ! $game->title) {
            return;
        }

        // Check if we already have an IGDB mapping (race condition protection)
        $existingSource = $game->title->sources()
            ->where('provider', 'igdb')
            ->exists();

        if ($existingSource) {
            Log::debug('SearchIgdbAndEnrichJob: IGDB mapping already exists', [
                'game_id' => $this->videoGameId,
            ]);

            return;
        }

        $token = $this->getAccessToken();

        if (! $token) {
            Log::warning('SearchIgdbAndEnrichJob: Failed to get access token');

            return;
        }

        // Search IGDB
        $igdbId = $this->searchIgdb($token);

        if (! $igdbId) {
            Log::info('SearchIgdbAndEnrichJob: No IGDB match found', [
                'game_id' => $this->videoGameId,
                'game_name' => $this->gameName,
            ]);

            return;
        }

        // Create the mapping
        VideoGameTitleSource::firstOrCreate(
            [
                'video_game_title_id' => $game->title->id,
                'provider' => 'igdb',
            ],
            [
                'external_id' => (string) $igdbId,
                'provider_item_id' => (string) $igdbId,
                'raw_payload' => [
                    'discovered_via' => 'search',
                    'search_term' => $this->gameName,
                ],
            ]
        );

        // Dispatch the enrichment job
        FetchIgdbDataJob::dispatch($this->videoGameId, $igdbId)
            ->onQueue('media-igdb');

        Log::info('SearchIgdbAndEnrichJob: Found and dispatched', [
            'game_id' => $this->videoGameId,
            'game_name' => $this->gameName,
            'igdb_id' => $igdbId,
        ]);
    }

    /**
     * Search IGDB by name.
     */
    private function searchIgdb(string $token): ?int
    {
        // Escape special characters in search query
        $escapedName = addslashes($this->gameName);

        $query = "search \"{$escapedName}\"; fields id, name; limit 1;";

        $response = Http::withHeaders([
            'Client-ID' => config('services.igdb.client_id'),
            'Authorization' => "Bearer {$token}",
        ])->withBody($query, 'text/plain')->post(self::BASE_URL.'/games');

        if (! $response->successful()) {
            return null;
        }

        $results = $response->json();

        return $results[0]['id'] ?? null;
    }

    /**
     * Get OAuth access token from cache or fetch new one.
     */
    private function getAccessToken(): ?string
    {
        $cached = Cache::get(self::TOKEN_CACHE_KEY);

        if ($cached) {
            return $cached;
        }

        $clientId = config('services.igdb.client_id');
        $clientSecret = config('services.igdb.client_secret');

        if (! $clientId || ! $clientSecret) {
            return null;
        }

        $response = Http::asForm()->post('https://id.twitch.tv/oauth2/token', [
            'client_id' => $clientId,
            'client_secret' => $clientSecret,
            'grant_type' => 'client_credentials',
        ]);

        if (! $response->successful()) {
            return null;
        }

        $token = $response->json('access_token');

        if ($token) {
            $expiresIn = $response->json('expires_in', 3600) - 300;
            Cache::put(self::TOKEN_CACHE_KEY, $token, max(60, $expiresIn));
        }

        return $token;
    }
}
