<?php

declare(strict_types=1);

namespace App\Jobs\Enrichment;

use App\Jobs\FetchGamePriceJob;
use App\Models\Retailer;
use App\Models\VideoGame;
use App\Models\VideoGameTitleSource;
use App\Services\Price\Steam\SteamStoreService;
use App\Services\Provider\ProviderDiscoveryService;
use Illuminate\Bus\Queueable;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Bus\Dispatchable;
use Illuminate\Queue\InteractsWithQueue;
use Illuminate\Queue\Middleware\RateLimited;
use Illuminate\Queue\SerializesModels;
use Illuminate\Support\Facades\Log;

/**
 * Enrichment job that discovers provider mappings and dispatches price fetch jobs.
 *
 * Uses ProviderDiscoveryService to query VideoGameTitleSource for all known
 * provider mappings, then dispatches appropriate price fetch jobs for each.
 */
class EnrichGamePricesJob implements ShouldQueue
{
    use Dispatchable, InteractsWithQueue, Queueable, SerializesModels;

    /**
     * Target regions for multi-region pricing.
     */
    private const TARGET_REGIONS = ['US', 'GB', 'DE', 'JP', 'BR', 'CA', 'AU'];

    public int $tries = 3;

    /** @var array<int, int> */
    public array $backoff = [60, 300, 900];

    public function __construct(
        public int $videoGameId
    ) {
    }

    /**
     * Get the middleware the job should pass through.
     *
     * @return array<int, object>
     */
    public function middleware(): array
    {
        return [
            new RateLimited('steam'),
        ];
    }

    public function handle(ProviderDiscoveryService $discovery, SteamStoreService $steam): void
    {
        $game = VideoGame::with('title.sources')->find($this->videoGameId);

        if (! $game) {
            Log::warning('EnrichGamePricesJob: Game not found', ['game_id' => $this->videoGameId]);

            return;
        }

        $dispatchedCount = 0;

        // Strategy 1: Use ProviderDiscoveryService to find all known provider mappings
        $mappings = $discovery->getPriceProviderMappings($game);

        foreach ($mappings as $source) {
            $dispatched = $this->dispatchForProvider($game, $source);
            $dispatchedCount += $dispatched;
        }

        // Strategy 2: If no mappings found and game has a direct retailer provider, use that
        if ($dispatchedCount === 0 && $this->isRetailerProvider($game->provider)) {
            $dispatched = $this->dispatchForDirectProvider($game);
            $dispatchedCount += $dispatched;
        }

        // Strategy 3: Search fallback for games without mappings (e.g., IGDB-only games)
        if ($dispatchedCount === 0) {
            $dispatched = $this->searchAndDispatch($game, $steam);
            $dispatchedCount += $dispatched;
        }

        Log::info('EnrichGamePricesJob: Completed', [
            'game_id' => $game->id,
            'game_name' => $game->name,
            'jobs_dispatched' => $dispatchedCount,
        ]);
    }

    /**
     * Dispatch price fetch jobs based on a provider mapping.
     */
    private function dispatchForProvider(VideoGame $game, VideoGameTitleSource $source): int
    {
        $provider = $source->provider;
        $externalId = $source->external_id;

        return match ($provider) {
            'steam', 'steam_store' => $this->dispatchSteamJobs($game, $externalId),
            'playstation_store' => $this->dispatchPsnJobs($game, $source->provider_item_id),
            'xbox' => $this->dispatchXboxJobs($game, $externalId),
            default => 0,
        };
    }

    /**
     * Dispatch Steam price fetch jobs.
     */
    private function dispatchSteamJobs(VideoGame $game, int $steamAppId): int
    {
        $retailer = Retailer::where('slug', 'steam')->first();

        if (! $retailer) {
            return 0;
        }

        $url = "https://store.steampowered.com/app/{$steamAppId}/";

        foreach (self::TARGET_REGIONS as $country) {
            FetchGamePriceJob::dispatch($game->id, $retailer->id, $url, $country);
        }

        Log::info('EnrichGamePricesJob: Dispatched Steam jobs', [
            'game_id' => $game->id,
            'steam_app_id' => $steamAppId,
            'regions' => count(self::TARGET_REGIONS),
        ]);

        return count(self::TARGET_REGIONS);
    }

    /**
     * Dispatch PSN price fetch jobs.
     */
    private function dispatchPsnJobs(VideoGame $game, int $psnProductId): int
    {
        // PSN job will be created separately
        // For now, log and return 0
        Log::info('EnrichGamePricesJob: PSN mapping found (job not implemented yet)', [
            'game_id' => $game->id,
            'psn_product_id' => $psnProductId,
        ]);

        return 0;
    }

    /**
     * Dispatch Xbox price fetch jobs.
     */
    private function dispatchXboxJobs(VideoGame $game, int $xboxProductId): int
    {
        // Xbox job will be created separately
        // For now, log and return 0
        Log::info('EnrichGamePricesJob: Xbox mapping found (job not implemented yet)', [
            'game_id' => $game->id,
            'xbox_product_id' => $xboxProductId,
        ]);

        return 0;
    }

    /**
     * Dispatch jobs when game has a direct retailer provider.
     */
    private function dispatchForDirectProvider(VideoGame $game): int
    {
        $url = $this->constructStoreUrl($game->provider, $game->external_id);

        if (! $url) {
            return 0;
        }

        $retailer = Retailer::where('slug', $game->provider)->first();

        if (! $retailer) {
            return 0;
        }

        foreach (self::TARGET_REGIONS as $country) {
            FetchGamePriceJob::dispatch($game->id, $retailer->id, $url, $country);
        }

        return count(self::TARGET_REGIONS);
    }

    /**
     * Search for the game on Steam and dispatch if found.
     */
    private function searchAndDispatch(VideoGame $game, SteamStoreService $steam): int
    {
        if (! $game->name) {
            return 0;
        }

        $steamId = $steam->search($game->name);

        if (! $steamId) {
            return 0;
        }

        Log::info('EnrichGamePricesJob: Found Steam match via search', [
            'game_id' => $game->id,
            'game_name' => $game->name,
            'steam_id' => $steamId,
        ]);

        return $this->dispatchSteamJobs($game, (int) $steamId);
    }

    private function isRetailerProvider(?string $provider): bool
    {
        return in_array($provider, ['steam', 'playstation', 'xbox', 'nintendo'], true);
    }

    private function constructStoreUrl(?string $provider, int|string|null $externalId): ?string
    {
        if (! $provider || ! $externalId) {
            return null;
        }

        return match ($provider) {
            'steam' => "https://store.steampowered.com/app/{$externalId}/",
            default => null,
        };
    }
}
