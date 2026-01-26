<?php

namespace App\Jobs;

use App\Console\Commands\Enrich\Concerns\InteractsWithEnrichmentProviders;
use App\Services\Media\TGDB\TGDBService;
use App\Services\Price\Amazon\AmazonScraperService;
use App\Services\Price\GameDataAggregatorService;
use App\Services\Price\GiantBomb\GiantBombService;
use App\Services\Price\ItchIo\ItchIoScraperService;
use App\Services\Price\PlayStation\PlayStationStoreService;
use App\Services\Price\Steam\SteamStoreService;
use App\Services\Price\Xbox\XboxStoreService;
use Illuminate\Bus\Queueable;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Bus\Dispatchable;
use Illuminate\Queue\InteractsWithQueue;
use Illuminate\Queue\SerializesModels;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Log;

class EnrichGameJob implements ShouldQueue
{
    use Dispatchable, InteractsWithQueue, Queueable, SerializesModels;
    use InteractsWithEnrichmentProviders;

    public function __construct(
        public int $gameId,
        public array $providers,
        public bool $worldwide = false
    ) {}

    public function handle(
        SteamStoreService $steam,
        XboxStoreService $xbox,
        PlayStationStoreService $playstation,
        AmazonScraperService $amazon,
        GameDataAggregatorService $aggregator,
        GiantBombService $giantBomb,
        TGDBService $tgdb,
        ItchIoScraperService $itchIo
    ): void {
        $game = DB::table('video_games')->where('id', $this->gameId)->first();

        if (! $game) {
            return;
        }

        try {
            // Pre-resolve IDs from attributes (IGDB websites) if missing
            $attributes = match (true) {
                is_array($game->attributes) => $game->attributes,
                is_string($game->attributes) => json_decode($game->attributes, true) ?? [],
                default => [],
            };
            $attributes = $this->resolveIdsFromExternalLinks($game, $attributes);

            // Persist updated attributes if IDs were found
            DB::table('video_games')->where('id', $game->id)->update([
                'attributes' => json_encode($attributes),
            ]);

            foreach ($this->providers as $provider) {
                match ($provider) {
                    'steam' => $this->enrichWithSteam($steam, $aggregator, $game, $this->worldwide),
                    'xbox' => $this->enrichWithXbox($xbox, $aggregator, $game, $this->worldwide),
                    'playstation' => $this->enrichWithPlayStation($playstation, $aggregator, $game, $this->worldwide),
                    'amazon' => $this->enrichWithAmazon($aggregator, $game, $this->worldwide),
                    'gog' => $this->enrichWithGog($aggregator, $game, $this->worldwide),
                    'epic' => $this->enrichWithEpic($aggregator, $game, $this->worldwide),
                    'ubisoft' => $this->enrichWithUbisoft($aggregator, $game, $this->worldwide),
                    'ea' => $this->enrichWithEA($aggregator, $game, $this->worldwide),
                    'giantbomb' => $this->enrichWithGiantBomb($giantBomb, $aggregator, $game),
                    'tgdb' => $this->enrichWithTGDB($tgdb, $aggregator, $game),
                    'itchio' => $this->enrichWithItchIo($itchIo, $aggregator, $game, $this->worldwide),
                    default => null,
                };
            }
        } catch (\Throwable $e) {
            Log::error("EnrichGameJob failed for game {$this->gameId}", [
                'error' => $e->getMessage(),
                'trace' => $e->getTraceAsString(),
            ]);
            // Re-throw to trigger job failure handling
            throw $e;
        }
    }
}
