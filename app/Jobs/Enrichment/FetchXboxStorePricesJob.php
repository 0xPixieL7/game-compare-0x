<?php

declare(strict_types=1);

namespace App\Jobs\Enrichment;

use App\Models\Country;
use App\Models\VideoGame;
use App\Models\VideoGamePrice;
use App\Models\VideoGameTitleSource;
use App\Services\Price\Xbox\XboxStoreService;
use Illuminate\Bus\Queueable;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Bus\Dispatchable;
use Illuminate\Queue\InteractsWithQueue;
use Illuminate\Queue\Middleware\RateLimited;
use Illuminate\Queue\SerializesModels;
use Illuminate\Support\Facades\Log;

final class FetchXboxStorePricesJob implements ShouldQueue
{
    use Dispatchable, InteractsWithQueue, Queueable, SerializesModels;

    /**
     * Build Xbox markets list from countries table.
     *
     * Optimization: default to a small, high-signal market set.
     * Full-country scans are extremely expensive (N markets per game).
     *
     * @return array<int, array{country:string, market:string, language:string}>
     */
    private function targets(): array
    {
        $language = (string) config('services.xbox.language', 'en-US');

        // Keep market set in lockstep with Steam region set.
        $marketsConfig = (string) config('services.xbox.markets', '');
        $fallbackMarket = (string) config('services.xbox.market', 'US');
        $markets = array_values(array_filter(array_map('trim', explode(',', $marketsConfig))));

        if ($markets === []) {
            // Default: pull first 15 ISO2 country codes from DB.
            try {
                $markets = Country::query()
                    ->select(['code'])
                    ->whereRaw('length(code) = 2')
                    ->orderBy('code')
                    ->limit(15)
                    ->pluck('code')
                    ->all();
            } catch (\Throwable) {
                $markets = [];
            }

            if ($markets === []) {
                $markets = [$fallbackMarket];
            }
        }

        $markets = array_values(array_unique(array_map(static function (string $value): string {
            $value = trim($value);
            if ($value === '') {
                return '';
            }

            // Accept locale style (en-us) and country style (US).
            if (str_contains($value, '-')) {
                $parts = explode('-', $value);
                $value = (string) end($parts);
            }

            return strtoupper($value);
        }, $markets)));
        $markets = array_values(array_filter($markets));

        // If markets are explicitly configured, use them (fast path).
        if ($markets !== []) {
            return array_map(static function (string $cc) use ($language): array {
                return ['country' => $cc, 'market' => $cc, 'language' => $language];
            }, $markets);
        }

        // Slow fallback (avoid unless explicitly needed).
        return Country::query()
            ->select(['code'])
            ->whereRaw('length(code) = 2')
            ->orderBy('code')
            ->pluck('code')
            ->map(function (string $code) use ($language): array {
                $cc = strtoupper($code);

                return ['country' => $cc, 'market' => $cc, 'language' => $language];
            })
            ->all();
    }

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
        return [new RateLimited('xbox')];
    }

    public function handle(XboxStoreService $xbox): void
    {
        $game = VideoGame::with('title.product')->find($this->videoGameId);
        $source = VideoGameTitleSource::find($this->titleSourceId);

        if (! $game || ! $game->title || ! $source) {
            return;
        }

        $productId = $game->title->product_id;

        // Prefer the raw BigId from RAWG store URL (e.g. 9NBLGGH5FV84)
        $bigId = $source->raw_payload['rawg_store_id']
            ?? $source->raw_payload['provider_item_id_raw']
            ?? null;

        if (! is_string($bigId) || $bigId === '') {
            Log::debug('FetchXboxStorePricesJob: missing raw bigId', [
                'video_game_id' => $game->id,
                'title_source_id' => $source->id,
            ]);

            return;
        }

        foreach ($this->targets() as $t) {
            $price = $xbox->getPrice($bigId, $t['market'], $t['language']);
            if (! $price) {
                continue;
            }

            VideoGamePrice::query()->updateOrCreate(
                [
                    'video_game_id' => $game->id,
                    'retailer' => 'Xbox Store',
                    'country_code' => $t['country'],
                ],
                [
                    'product_id' => $productId,
                    'currency' => $price['currency'],
                    'amount_minor' => $price['amount_minor'],
                    'recorded_at' => now(),
                    'url' => $source->raw_payload['rawg_store_url'] ?? null,
                    'is_active' => true,
                    'bucket' => 'snapshot',
                    'region_code' => $t['country'],
                    'condition' => 'digital',
                    'sku' => null,
                    'metadata' => [
                        'market' => $price['market'],
                        'language' => $price['language'],
                        'msrp' => $price['msrp'],
                        'list_price' => $price['list_price'],
                        'source' => 'xbox_displaycatalog',
                        'big_id' => strtoupper($bigId),
                    ],
                ]
            );
        }

        if ($game->title->product) {
            $game->title->product->refreshPricingSnapshot();
        }
    }
}
