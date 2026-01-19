<?php

declare(strict_types=1);

namespace App\Jobs\Enrichment;

use App\Models\Retailer;
use App\Models\VideoGame;
use App\Services\Price\Steam\SteamStoreService;
use Illuminate\Bus\Queueable;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Bus\Dispatchable;
use Illuminate\Queue\InteractsWithQueue;
use Illuminate\Queue\Middleware\RateLimited;
use Illuminate\Queue\SerializesModels;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Log;

/**
 * Fetch price for a SINGLE region (ultra-lightweight, parallelizable).
 * 
 * Uses 'filters=price_overview' to minimize bandwidth.
 * Dispatched in bulk for the 29 secondary regions.
 */
class FetchSteamPriceForRegionJob implements ShouldQueue
{
    use Dispatchable, InteractsWithQueue, Queueable, SerializesModels;

    public int $tries = 3;
    
    /** @var array<int, int> */
    public array $backoff = [60, 300, 900]; // 1m, 5m, 15m

    public function __construct(
        public int $videoGameId,
        public int $steamAppId,
        public string $region
    ) {}

    public function middleware(): array
    {
        return [new RateLimited('steam')];
    }

    public function handle(SteamStoreService $steam): void
    {
        // 1. Fetch price (lightweight call)
        $priceData = $steam->getPrice((string) $this->steamAppId, $this->region);

        if (!$priceData) {
            return; // Game not available in this region or free
        }

        // 2. Upsert Price Record
        $retailerName = 'Steam'; // Default fallback
        $retailer = Retailer::where('slug', 'steam')->first();
        if ($retailer) {
            $retailerName = $retailer->name;
        }

        DB::table('video_game_prices')->upsert(
            [
                'video_game_id' => $this->videoGameId,
                'currency' => $priceData['currency'],
                'country_code' => $this->region,
                'amount_minor' => $priceData['amount_minor'],
                'retailer' => $retailerName,
                'url' => "https://store.steampowered.com/app/{$this->steamAppId}/",
                'recorded_at' => now(),
                'is_active' => true,
                'metadata' => json_encode([
                    'steam_app_id' => $this->steamAppId,
                    'discount_percent' => $priceData['discount_percent'] ?? 0,
                ]),
                'updated_at' => now(),
            ],
            ['video_game_id', 'retailer', 'country_code'],
            ['currency', 'amount_minor', 'recorded_at', 'is_active', 'metadata', 'updated_at']
        );
    }
}
