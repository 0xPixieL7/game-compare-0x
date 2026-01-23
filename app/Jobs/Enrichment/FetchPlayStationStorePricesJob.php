<?php

declare(strict_types=1);

namespace App\Jobs\Enrichment;

use App\Models\VideoGame;
use App\Models\VideoGameTitleSource;
use App\Services\Providers\PlayStationStoreProvider;
use Illuminate\Bus\Queueable;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Bus\Dispatchable;
use Illuminate\Queue\InteractsWithQueue;
use Illuminate\Queue\Middleware\RateLimited;
use Illuminate\Queue\SerializesModels;
use Illuminate\Support\Facades\Log;

final class FetchPlayStationStorePricesJob implements ShouldQueue
{
    use Dispatchable, InteractsWithQueue, Queueable, SerializesModels;

    public int $tries = 3;

    /** @var array<int, int> */
    public array $backoff = [60, 180, 600];

    public function __construct(
        public int $videoGameId,
        public int $titleSourceId
    ) {}

    /**
     * @return array<int, object>
     */
    public function middleware(): array
    {
        return [new RateLimited('psstore')];
    }

    public function handle(): void
    {
        $game = VideoGame::with('title.product')->find($this->videoGameId);
        $source = VideoGameTitleSource::find($this->titleSourceId);

        if (! $game || ! $game->title || ! $source) {
            return;
        }

        // Prefer the RAW string ID (e.g. UP0001-...) stored in raw_payload.
        $rawId = $source->raw_payload['rawg_store_id']
            ?? $source->raw_payload['provider_item_id_raw']
            ?? null;

        if (! is_string($rawId) || $rawId === '') {
            // If we somehow only have numeric, we cannot call PS GraphQL reliably.
            Log::debug('FetchPlayStationStorePricesJob: missing raw store id', [
                'video_game_id' => $game->id,
                'title_source_id' => $source->id,
            ]);

            return;
        }

        $regions = config('services.playstation.regions', 'en-us');
        $regionList = array_values(array_filter(array_map('trim', explode(',', (string) $regions))));
        if ($regionList === []) {
            $regionList = ['en-us'];
        }

        $provider = new PlayStationStoreProvider($regionList);

        // This will create/update PlayStationStore provider rows + upsert prices (bucket=snapshot).
        // NOTE: RAWG may provide /product IDs; PS provider expects concept IDs. If this raw ID
        // isn't compatible, it will no-op safely.
        $provider->ingestConceptWithMultiRegionPricing($rawId);

        if ($game->title?->product) {
            $game->title->product->refreshPricingSnapshot();
        }
    }
}
