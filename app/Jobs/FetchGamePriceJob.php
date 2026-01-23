<?php

namespace App\Jobs;

use App\Models\Retailer;
use App\Services\Price\Steam\SteamStoreService;
use Illuminate\Bus\Queueable;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Bus\Dispatchable;
use Illuminate\Queue\InteractsWithQueue;
use Illuminate\Queue\SerializesModels;
use Illuminate\Support\Facades\DB;

class FetchGamePriceJob implements ShouldQueue
{
    use Dispatchable, InteractsWithQueue, Queueable, SerializesModels;

    public function __construct(
        public int $videoGameId,
        public int $retailerId,
        public string $url,
        public string $country = 'US'
    ) {}

    public function handle(
        SteamStoreService $steamService, 
        \App\Services\Price\Amazon\AmazonScraperService $amazonService,
        \App\Services\Price\EpicGames\EpicGamesStoreService $epicService,
        \App\Services\Price\Gog\GogStoreService $gogService
    ): void
    {
        $retailer = Retailer::find($this->retailerId);
        if (! $retailer) {
            return;
        }

        $priceData = null;

        if ($retailer->slug === 'steam') {
            if (preg_match('/app\/(\d+)/', $this->url, $matches)) {
                $appId = $matches[1];
                $priceData = $steamService->getPrice($appId, $this->country);
            }
        } elseif ($retailer->name === 'Amazon') { 
             $priceData = $amazonService->getPrice($this->url, $this->country);
        } elseif ($retailer->name === 'Epic Games') {
             $priceData = $epicService->getPrice($this->url, $this->country);
        } elseif ($retailer->name === 'GOG') {
             $priceData = $gogService->getPrice($this->url, $this->country);
        }

        if ($priceData) {
            $currency = $priceData['currency'] ?? null;
            $amountMinor = $priceData['amount_minor'] ?? null;
            
            // Allow 0 for free games, but ignore nulls
            if ($currency === null || $amountMinor === null) {
                return;
            }

            DB::table('video_game_prices')->updateOrInsert(
                [
                    'video_game_id' => $this->videoGameId,
                    'retailer' => $retailer->name,
                    'country_code' => $this->country,
                ],
                [
                    'url' => $this->url,
                    'currency' => $currency,
                    'amount_minor' => $amountMinor,
                    'recorded_at' => now(),
                    'updated_at' => now(),
                ]
            );
        }
    }
}
