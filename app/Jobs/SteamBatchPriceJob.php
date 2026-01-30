<?php

declare(strict_types=1);

namespace App\Jobs;

use App\Services\Price\GameDataAggregatorService;
use App\Services\Price\Steam\SteamStoreService;
use Illuminate\Bus\Queueable;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Bus\Dispatchable;
use Illuminate\Queue\InteractsWithQueue;
use Illuminate\Queue\SerializesModels;
use Illuminate\Support\Facades\DB;

final class SteamBatchPriceJob implements ShouldQueue
{
    use Dispatchable, InteractsWithQueue, Queueable, SerializesModels;

    /** @param array<int, int> $videoGameIds */
    public function __construct(
        public array $videoGameIds,
        public bool $worldwide = true
    ) {}

    public function handle(SteamStoreService $steam, GameDataAggregatorService $aggregator): void
    {
        if ($this->videoGameIds === []) {
            return;
        }

        $rows = DB::table('video_games as vg')
            ->select(['vg.id', 'vg.attributes', 'el.external_id as steam_external_id', 'el.url as steam_url'])
            ->leftJoin('video_game_external_links as el', function ($join) {
                $join->on('el.video_game_id', '=', 'vg.id')
                    ->where('el.category', '=', 0);
            })
            ->whereIn('vg.id', $this->videoGameIds)
            ->get();

        $gameIdByAppId = [];

        foreach ($rows as $row) {
            $attrs = is_string($row->attributes ?? null)
                ? json_decode($row->attributes, true) ?? []
                : (is_array($row->attributes) ? $row->attributes : []);

            $steamId = $attrs['steam_id'] ?? null;

            if (! $steamId && is_string($row->steam_url ?? null)) {
                if (preg_match('/store\.steampowered\.com\/app\/(\d+)/', (string) $row->steam_url, $m) === 1) {
                    $steamId = (int) $m[1];
                }
            }

            if (! $steamId && is_string($row->steam_external_id ?? null) && preg_match('/^\d+$/', (string) $row->steam_external_id) === 1) {
                $steamId = (int) $row->steam_external_id;
            }

            if (! $steamId || (int) $steamId <= 0) {
                continue;
            }

            $gameIdByAppId[(int) $steamId] = (int) $row->id;
        }

        if ($gameIdByAppId === []) {
            return;
        }

        $regions = array_filter(array_map('trim', explode(',', (string) config('services.steam.regions'))));
        if ($regions === []) {
            // Avoid accidental full-country scan.
            $regions = ['US'];
        }

        $appIds = array_keys($gameIdByAppId);

        foreach ($regions as $cc) {
            $prices = $steam->getPricesForAppIds($appIds, $cc);

            foreach ($prices as $appId => $priceData) {
                if (! $priceData) {
                    continue;
                }

                $gameId = $gameIdByAppId[(int) $appId] ?? null;
                if (! $gameId) {
                    continue;
                }

                $aggregator->persistStandardPrice(
                    $gameId,
                    'Steam',
                    $cc,
                    $priceData
                );
            }
        }
    }
}
