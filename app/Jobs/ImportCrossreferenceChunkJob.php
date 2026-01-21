<?php

declare(strict_types=1);

namespace App\Jobs;

use App\Models\VideoGame;
use App\Models\VideoGamePrice;
use Carbon\Carbon;
use Illuminate\Bus\Batchable;
use Illuminate\Bus\Queueable;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Bus\Dispatchable;
use Illuminate\Queue\InteractsWithQueue;
use Illuminate\Queue\SerializesModels;
use Illuminate\Support\Facades\Log;

class ImportCrossreferenceChunkJob implements ShouldQueue
{
    use Batchable, Dispatchable, InteractsWithQueue, Queueable, SerializesModels;

    protected array $chunk;

    protected array $giantBomb;

    protected array $nexarda;

    public function __construct(array $chunk, array $giantBomb, array $nexarda)
    {
        $this->chunk = $chunk;
        $this->giantBomb = $giantBomb;
        $this->nexarda = $nexarda;
    }

    public function handle(): void
    {
        foreach ($this->chunk as $row) {
            // --- Fill all relevant columns in all touched tables ---
            $videoGame = $this->resolveOrCreateVideoGame($row);
            $videoGameId = $videoGame ? $videoGame->id : null;
            $currency = 'USD';
            $amount_minor = $this->parsePrice($row['pc_price']);
            $recorded_at = Carbon::now();
            $retailer = 'PriceCharting';
            $country_code = 'US';
            $source_ids = [
                'price_charting' => $row['pc_id'],
                'igdb' => $row['igdb_id'],
                'nexarda' => $row['igdb_external_id'] ?? null,
            ];
            $metadata = [
                'pc_name' => $row['pc_name'],
                'pc_console' => $row['pc_console'],
                'igdb_name' => $row['igdb_name'],
                'igdb_platform' => $row['igdb_platform'],
                'igdb_slug' => $row['igdb_slug'],
                'sources' => $source_ids,
            ];
            $media = $this->crossReferenceMedia($row, $this->giantBomb, $this->nexarda);
            if ($media) {
                $metadata['media'] = $media;
            }
            if ($videoGameId) {
                // --- Fill all columns in video_game_prices ---
                VideoGamePrice::updateOrCreate([
                    'video_game_id' => $videoGameId,
                    'retailer' => $retailer,
                    'country_code' => $country_code,
                ], [
                    'currency' => $currency,
                    'amount_minor' => $amount_minor,
                    'recorded_at' => $recorded_at,
                    'metadata' => $metadata,
                    'is_active' => true,
                    'tax_inclusive' => false,
                    'sku' => null,
                    'condition' => null,
                    'is_retail_buy' => false,
                    'sales_volume' => null,
                    'url' => null,
                    'product_id' => null,
                    'region_code' => null,
                ]);
            } else {
                Log::warning('Could not resolve or create video_game for row', $row);
            }
            foreach ($this->rows as $row) {
                // Map fields from CSV
                $gameId = $row['game_id'] ?? null;
                $source = $row['source'] ?? null;
                $sourceId = $row['source_id'] ?? null;
                $region = $row['region'] ?? null;
                $date = $row['date'] ?? null;

                // Support multiple currencies and values per row (if present)
                // Assume CSV columns: price_{CURRENCY} (e.g., price_USD, price_EUR, ...)
                $currencies = [];
                foreach ($row as $key => $value) {
                    if (preg_match('/^price_([A-Z]{3})$/', $key, $m) && is_numeric($value)) {
                        $currencies[$m[1]] = $value;
                    }
                }

                foreach ($currencies as $currency => $price) {
                    // Normalize price to BTC (or other target) using FX rate for the original currency
                    $normalizedPrice = null;
                    $normalizedCurrency = 'BTC';
                    $fxRate = $this->getFxRate($currency, $normalizedCurrency, $date);
                    if ($fxRate) {
                        $normalizedPrice = $price * $fxRate;
                    }

                    // Upsert into video_game_prices for each currency
                    \App\Models\VideoGamePrice::updateOrCreate(
                        [
                            'game_id' => $gameId,
                            'source' => $source,
                            'source_id' => $sourceId,
                            'region' => $region,
                            'currency' => $currency,
                        ],
                        [
                            'price' => $price,
                            'normalized_price' => $normalizedPrice,
                            'normalized_currency' => $normalizedCurrency,
                            'date' => $date,
                        ]
                    );
                }
            }
        }
    }

    protected function parsePrice(string $price): int
    {
        $price = str_replace(['$', ','], '', $price);

        return (int) round(floatval($price) * 100);
    }

    protected function resolveOrCreateVideoGame(array $row): ?VideoGame
    {
        // Try IGDB, then PriceCharting, then Nexarda, or create if not found
        $externalIds = array_filter([
            $row['igdb_id'] ?? null,
            $row['pc_id'] ?? null,
            $row['igdb_external_id'] ?? null,
        ]);
        foreach ($externalIds as $extId) {
            $game = VideoGame::where('external_id', $extId)->first();
            if ($game) {
                // Fill all columns if missing
                $game->fill([
                    'name' => $row['igdb_name'] ?? $row['pc_name'] ?? null,
                    'platform' => $row['igdb_platform'] ?? $row['pc_console'] ?? null,
                    'slug' => $row['igdb_slug'] ?? null,
                    'provider' => 'import',
                    'external_id' => $extId,
                ]);
                $game->save();

                return $game;
            }
        }
        // Create if not found
        if (! empty($row['igdb_id']) || ! empty($row['pc_id'])) {
            return VideoGame::create([
                'name' => $row['igdb_name'] ?? $row['pc_name'] ?? null,
                'platform' => $row['igdb_platform'] ?? $row['pc_console'] ?? null,
                'slug' => $row['igdb_slug'] ?? null,
                'provider' => 'import',
                'external_id' => $row['igdb_id'] ?? $row['pc_id'] ?? null,
            ]);
        }

        return null;
    }

    protected function crossReferenceMedia(array $row, array $giantBomb, array $nexarda): array
    {
        $media = [];
        if (! empty($row['igdb_slug']) && isset($giantBomb[$row['igdb_slug']])) {
            $media['giantbomb'] = $giantBomb[$row['igdb_slug']];
        } elseif (! empty($row['igdb_name'])) {
            foreach ($giantBomb as $slug => $data) {
                if (strtolower($data['name'] ?? '') === strtolower($row['igdb_name'])) {
                    $media['giantbomb'] = $data;
                    break;
                }
            }
        }
        if (! empty($row['igdb_external_id']) && isset($nexarda[$row['igdb_external_id']])) {
            $media['nexarda'] = $nexarda[$row['igdb_external_id']];
        }

        return $media;
    }
}
