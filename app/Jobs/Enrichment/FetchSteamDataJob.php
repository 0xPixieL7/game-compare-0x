<?php

declare(strict_types=1);

namespace App\Jobs\Enrichment;

use App\Models\Image;
use App\Models\Retailer;
use App\Models\Video;
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
 * Fetch combined Media AND Price for the primary region (US).
 *
 * One API call returns:
 * - Current Price (for US)
 * - Screenshots (English)
 * - Movies/Trailers (English)
 * - Header/Capsule Images
 */
class FetchSteamDataJob implements ShouldQueue
{
    use Dispatchable, InteractsWithQueue, Queueable, SerializesModels;

    public int $tries = 3;

    public array $backoff = [30, 120, 300];

    public function __construct(
        public int $videoGameId,
        public int $steamAppId,
        public bool $fetchMediaOnly = false,
        public ?string $language = null
    ) {}

    public function middleware(): array
    {
        return [new RateLimited('steam')];
    }

    public function handle(SteamStoreService $steam): void
    {
        $game = VideoGame::find($this->videoGameId);

        if (! $game) {
            Log::warning('FetchSteamDataJob: Game not found', ['id' => $this->videoGameId]);

            return;
        }

        // ONE Call: Get Full Details (Price + Media) for US
        $data = $steam->getFullDetails((string) $this->steamAppId, 'US', $this->language);

        if (! $data) {
            return; // API Error or Invalid App ID
        }

        DB::transaction(function () use ($game, $data) {
            // 1. Store Media (Global)
            $this->storeMedia($game, $data['media']);

            // 2. Store US Price (if not media-only mode)
            if (! $this->fetchMediaOnly && $data['price']) {
                $this->storePrice($game, $data['price'], 'US');
            }
        });
    }

    private function storePrice(VideoGame $game, array $priceData, string $region): void
    {
        $retailerName = 'Steam';
        $retailer = Retailer::where('slug', 'steam')->first();
        if ($retailer) {
            $retailerName = $retailer->name;
        }

        DB::table('video_game_prices')->upsert(
            [
                'video_game_id' => $game->id,
                'currency' => $priceData['currency'],
                'country_code' => $region,
                'amount_minor' => $priceData['amount_minor'],
                'retailer' => $retailerName,
                'url' => "https://store.steampowered.com/app/{$this->steamAppId}/",
                'recorded_at' => now(),
                'is_active' => true,
                'metadata' => json_encode([
                    'steam_app_id' => $this->steamAppId,
                    'discount_percent' => $priceData['discount_percent'] ?? 0,
                    'initial_amount_minor' => $priceData['initial_amount_minor'] ?? null,
                ]),
                'updated_at' => now(),
            ],
            ['video_game_id', 'retailer', 'country_code'],
            ['currency', 'amount_minor', 'recorded_at', 'is_active', 'metadata', 'updated_at']
        );
    }

    private function storeMedia(VideoGame $game, array $media): void
    {
        $headerImage = $media['header_image'] ?? null;
        $background = $media['background_raw'] ?? $media['background'] ?? null;
        $capsuleImage = $media['capsule_imagev5'] ?? $media['capsule_image'] ?? null;
        $screenshots = $media['screenshots'] ?? [];
        $movies = $media['movies'] ?? [];

        // 1. Store Header Image (Cover/Landscape)
        if ($headerImage) {
            Image::updateOrCreate(
                [
                    'video_game_id' => $game->id,
                    'imageable_type' => VideoGame::class,
                    'imageable_id' => $game->id,
                    'primary_collection' => 'cover_images',
                ],
                [
                    'provider' => 'steam',
                    'collection_names' => ['cover_images'],
                    'url' => $headerImage,
                    'urls' => [$headerImage],
                    'metadata' => ['steam_app_id' => $this->steamAppId],
                ]
            );
        }

        // 2. Store Capsule Image (Poster/Vertical Art)
        if ($capsuleImage) {
            Image::firstOrCreate(
                [
                    'video_game_id' => $game->id,
                    'url' => $capsuleImage,
                ],
                [
                    'imageable_type' => VideoGame::class,
                    'imageable_id' => $game->id,
                    'provider' => 'steam',
                    'primary_collection' => 'posters',
                    'collection_names' => ['posters', 'artwork'],
                    'urls' => [$capsuleImage],
                    'metadata' => ['steam_app_id' => $this->steamAppId, 'type' => 'capsule'],
                ]
            );
        }

        // 3. Store Background Image (Store Page BG)
        if ($background) {
            Image::firstOrCreate(
                [
                    'video_game_id' => $game->id,
                    'url' => $background,
                ],
                [
                    'imageable_type' => VideoGame::class,
                    'imageable_id' => $game->id,
                    'provider' => 'steam',
                    'primary_collection' => 'backgrounds',
                    'collection_names' => ['backgrounds'],
                    'urls' => [$background],
                    'metadata' => ['steam_app_id' => $this->steamAppId, 'type' => 'store_bg'],
                ]
            );
        }

        // 4. Store Library Assets (Hero + Logo) - Constructed URLs
        // Prefer 2x assets for high-DPI displays
        $libraryHero = "https://shared.akamai.steamstatic.com/store_item_assets/steam/apps/{$this->steamAppId}/library_hero.jpg";
        $libraryHero2x = "https://shared.akamai.steamstatic.com/store_item_assets/steam/apps/{$this->steamAppId}/library_hero_2x.jpg";

        $libraryLogo = "https://shared.akamai.steamstatic.com/store_item_assets/steam/apps/{$this->steamAppId}/logo.png";
        $libraryLogo2x = "https://shared.akamai.steamstatic.com/store_item_assets/steam/apps/{$this->steamAppId}/logo_2x.png";

        Image::firstOrCreate(
            [
                'video_game_id' => $game->id,
                'url' => $libraryHero2x, // Prefer 2x as primary key URL
            ],
            [
                'imageable_type' => VideoGame::class,
                'imageable_id' => $game->id,
                'provider' => 'steam',
                'primary_collection' => 'hero',
                'collection_names' => ['hero', 'backgrounds'],
                'urls' => [$libraryHero2x, $libraryHero], // Store both for fallback
                'metadata' => ['steam_app_id' => $this->steamAppId, 'type' => 'library_hero'],
            ]
        );

        Image::firstOrCreate(
            [
                'video_game_id' => $game->id,
                'url' => $libraryLogo2x,
            ],
            [
                'imageable_type' => VideoGame::class,
                'imageable_id' => $game->id,
                'provider' => 'steam',
                'primary_collection' => 'clear_logo',
                'collection_names' => ['clear_logo', 'logos'],
                'urls' => [$libraryLogo2x, $libraryLogo], // Store both for fallback
                'metadata' => ['steam_app_id' => $this->steamAppId, 'type' => 'library_logo'],
            ]
        );

        // 5. Store Screenshots (Batch)
        // Only store the first 10 to avoid bloating DB
        foreach (array_slice($screenshots, 0, 10) as $shot) {
            if (empty($shot['full'])) {
                continue;
            }

            Image::firstOrCreate(
                [
                    'video_game_id' => $game->id,
                    'url' => $shot['full'],
                ],
                [
                    'imageable_type' => VideoGame::class,
                    'imageable_id' => $game->id,
                    'provider' => 'steam',
                    'primary_collection' => 'screenshots',
                    'collection_names' => ['screenshots'],
                    'urls' => [$shot['full'], $shot['thumbnail'] ?? null],
                    'metadata' => ['steam_id' => $shot['id'] ?? null],
                ]
            );
        }

        // 4. Store Trailers (Videos)
        foreach (array_slice($movies, 0, 3) as $movie) {
            // Prioritize standard formats, fallback to HLS
            $url = $movie['mp4_max']
                ?? $movie['mp4_480']
                ?? $movie['webm_max']
                ?? $movie['webm_480']
                ?? $movie['hls_max']
                ?? null;

            if (! $url) {
                continue;
            }

            Video::updateOrCreate(
                [
                    'video_game_id' => $game->id,
                    'url' => $url,
                ],
                [
                    'videoable_type' => VideoGame::class,
                    'videoable_id' => $game->id,
                    'provider' => 'steam',
                    'primary_collection' => 'trailers',
                    'collection_names' => ['trailers'],
                    'title' => $movie['name'] ?? 'Trailer',
                    'thumbnail_url' => $movie['thumbnail'] ?? null,
                    'metadata' => ['steam_id' => $movie['id'] ?? null],
                ]
            );
        }
    }
}
