<?php

namespace App\Console\Commands\Enrich\Concerns;

use App\Models\VideoGame;
use App\Services\Media\RAWG\RawgService;
use App\Services\Media\TGDB\TGDBService;
use App\Services\Price\GameDataAggregatorService;
use App\Services\Price\GiantBomb\GiantBombService;
use App\Services\Price\ItchIo\ItchIoScraperService;
use App\Services\Price\PlayStation\PlayStationStoreService;
use App\Services\Price\Steam\SteamStoreService;
use App\Services\Price\Xbox\XboxStoreService;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Log;

trait InteractsWithEnrichmentProviders
{
    /**
     * Enrich with Steam media AND prices - Standardized.
     */
    protected function enrichWithSteam(SteamStoreService $steam, GameDataAggregatorService $aggregator, object $game, bool $worldwide = false, bool $search = true, bool $pricesOnly = false): array
    {
        $this->logProvider('Steam', 'start', ['game_id' => $game->id, 'worldwide' => $worldwide, 'prices_only' => $pricesOnly]);
        // 1. Check for Steam ID first (skip search if not found and search disabled)
        $attributes = is_string($game->attributes ?? null) ? json_decode($game->attributes, true) : (array) ($game->attributes ?? []);
        $steamId = $attributes['steam_id'] ?? null;

        // Fallback to external_links table (IGDB category 0 = Steam)
        if (! $steamId) {
            $steamId = $this->getExternalLinkId($game, 0);
            if ($steamId) {
                Log::info('Steam: Using external_links ID', ['game_id' => $game->id, 'steam_id' => $steamId]);
            }
        }

        if (! $steamId && ! $search) {
            $result = ['images' => 0, 'videos' => 0, 'prices' => 0];
            $this->logProvider('Steam', 'done', ['game_id' => $game->id, 'result' => $result, 'reason' => 'no_steam_id']);

            return $result;
        }

        if (! $steamId) {
            $steamId = $steam->search($game->name);
        }

        if (! is_numeric($steamId) || (int) $steamId <= 0) {
            $result = ['images' => 0, 'videos' => 0, 'prices' => 0];
            $reason = $steamId === null ? 'steam_api_blocked_or_not_found' : 'invalid_steam_id';
            $this->logProvider('Steam', 'done', ['game_id' => $game->id, 'result' => $result, 'reason' => $reason]);

            return $result;
        }

        $steamId = (int) $steamId;

        if (! $steamId) {
            $result = ['images' => 0, 'videos' => 0, 'prices' => 0];
            $this->logProvider('Steam', 'done', ['game_id' => $game->id, 'result' => $result]);

            return $result;
        }

        // 2. Persist ID
        DB::table('video_games')->where('id', $game->id)->update([
            'attributes' => json_encode(array_merge($attributes, ['steam_id' => $steamId])),
        ]);

        if ($pricesOnly) {
            if ($worldwide) {
                if (method_exists($this, 'info')) {
                    $this->info("   🌎 Syncing Steam prices worldwide for {$game->name}...");
                }
                $result = $aggregator->syncRegionalPricesForRetailer((VideoGame::find($game->id)), 'Steam');
                $result = ['images' => 0, 'videos' => 0, 'prices' => $result['prices'] ?? 0];
                $this->logProvider('Steam', 'done', ['game_id' => $game->id, 'result' => $result]);

                return $result;
            }

            $price = $steam->getPrice((string) $steamId);
            if (! $price) {
                $result = ['images' => 0, 'videos' => 0, 'prices' => 0];
                $this->logProvider('Steam', 'done', ['game_id' => $game->id, 'result' => $result]);

                return $result;
            }

            $aggregator->persistStandardPrice($game->id, 'Steam', 'US', $price);

            $result = ['images' => 0, 'videos' => 0, 'prices' => 1];
            $this->logProvider('Steam', 'done', ['game_id' => $game->id, 'result' => $result]);

            return $result;
        }

        // 3. Fetch Full Details (Media + Base Price)
        $data = $steam->getFullDetails((string) $steamId);
        if (! $data) {
            $result = ['images' => 0, 'videos' => 0, 'prices' => 0];
            $this->logProvider('Steam', 'done', ['game_id' => $game->id, 'result' => $result]);

            return $result;
        }

        // 4. Standardized Media Persistence
        $aggregator->persistMedia($game->id, 'Steam', $data['media'] ?? []);

        // 5. Pricing Persistence
        $pricesSynced = 0;
        if ($worldwide) {
            if (method_exists($this, 'info')) {
                $this->info("   🌎 Syncing Steam prices worldwide for {$game->name}...");
            }
            $result = $aggregator->syncRegionalPricesForRetailer((VideoGame::find($game->id)), 'Steam');
            $pricesSynced = $result['prices'];
        } elseif (! empty($data['price'])) {
            $aggregator->persistStandardPrice($game->id, 'Steam', 'US', $data['price']);
            $pricesSynced = 1;
        }

        $result = [
            'images' => count($data['media']['screenshots'] ?? []) + (isset($data['media']['header_image']) ? 1 : 0),
            'videos' => count($data['media']['movies'] ?? []),
            'prices' => $pricesSynced,
        ];

        $this->logProvider('Steam', 'done', ['game_id' => $game->id, 'result' => $result]);

        return $result;
    }

    /**
     * Enrich with Xbox Store data - Standardized.
     */
    protected function enrichWithXbox(XboxStoreService $xbox, GameDataAggregatorService $aggregator, object $game, bool $worldwide = false, bool $search = true, bool $pricesOnly = false): array
    {
        $this->logProvider('Xbox Store', 'start', ['game_id' => $game->id, 'worldwide' => $worldwide, 'prices_only' => $pricesOnly]);
        // 1. Resolve Xbox Product ID
        $attributes = is_string($game->attributes ?? null) ? json_decode($game->attributes, true) : (array) ($game->attributes ?? []);
        $productId = $attributes['xbox_bigid'] ?? null;

        // Fallback to external_links table (IGDB category 31 = Xbox Marketplace)
        if (! $productId) {
            $productId = $this->getExternalLinkId($game, 31);
            if ($productId) {
                Log::info('Xbox: Using external_links ID', ['game_id' => $game->id, 'xbox_id' => $productId]);
            }
        }

        if (! $productId) {
            $result = ['images' => 0, 'videos' => 0, 'prices' => 0];
            $this->logProvider('Xbox Store', 'done', ['game_id' => $game->id, 'result' => $result]);

            return $result;
        }

        // 2. Persist ID
        DB::table('video_games')->where('id', $game->id)->update([
            'attributes' => json_encode(array_merge($attributes, ['xbox_bigid' => $productId])),
        ]);

        if ($pricesOnly) {
            if ($worldwide) {
                if (method_exists($this, 'info')) {
                    $this->info("   🌎 Syncing Xbox prices worldwide for {$game->name}...");
                }
                $result = $aggregator->syncRegionalPricesForRetailer((VideoGame::find($game->id)), 'Xbox Store');
                $result = ['images' => 0, 'videos' => 0, 'prices' => $result['prices'] ?? 0];
                $this->logProvider('Xbox Store', 'done', ['game_id' => $game->id, 'result' => $result]);

                return $result;
            }

            $data = $xbox->getFullDetails($productId);
            if (! $data || empty($data['price'])) {
                $result = ['images' => 0, 'videos' => 0, 'prices' => 0];
                $this->logProvider('Xbox Store', 'done', ['game_id' => $game->id, 'result' => $result]);

                return $result;
            }

            $aggregator->persistStandardPrice($game->id, 'Xbox Store', 'US', $data['price']);

            $result = ['images' => 0, 'videos' => 0, 'prices' => 1];
            $this->logProvider('Xbox Store', 'done', ['game_id' => $game->id, 'result' => $result]);

            return $result;
        }

        // 3. Fetch Full Details
        $data = $xbox->getFullDetails($productId);
        if (! $data) {
            $result = ['images' => 0, 'videos' => 0, 'prices' => 0];
            $this->logProvider('Xbox Store', 'done', ['game_id' => $game->id, 'result' => $result]);

            return $result;
        }

        // 4. Standardized Media Persistence
        $aggregator->persistMedia($game->id, 'Xbox Store', $data['media'] ?? []);

        // 5. Pricing Persistence
        $pricesSynced = 0;
        if ($worldwide) {
            if (method_exists($this, 'info')) {
                $this->info("   🌎 Syncing Xbox prices worldwide for {$game->name}...");
            }
            $result = $aggregator->syncRegionalPricesForRetailer((VideoGame::find($game->id)), 'Xbox Store');
            $pricesSynced = $result['prices'];
        } elseif (! empty($data['price'])) {
            $aggregator->persistStandardPrice($game->id, 'Xbox Store', 'US', $data['price']);
            $pricesSynced = 1;
        }

        $result = [
            'images' => count($data['media']['images'] ?? []),
            'videos' => count($data['media']['videos'] ?? []),
            'prices' => $pricesSynced,
        ];

        $this->logProvider('Xbox Store', 'done', ['game_id' => $game->id, 'result' => $result]);

        return $result;
    }

    /**
     * Enrich with PlayStation Store data - Standardized.
     */
    protected function enrichWithPlayStation(PlayStationStoreService $playstation, GameDataAggregatorService $aggregator, object $game, bool $worldwide = false, bool $search = true): array
    {
        $this->logProvider('PlayStation Store', 'start', ['game_id' => $game->id, 'worldwide' => $worldwide]);
        // 1. Resolve PS Product ID
        $attributes = is_string($game->attributes ?? null) ? json_decode($game->attributes, true) : (array) ($game->attributes ?? []);
        $productId = $attributes['ps_product_id'] ?? null;
        $conceptId = $attributes['ps_concept_id'] ?? null;

        // Fallback to external_links table (IGDB category 36 = PlayStation Store)
        if (! $productId && ! $conceptId) {
            $conceptId = $this->getExternalLinkId($game, 36);
            if ($conceptId) {
                Log::info('PlayStation: Using external_links concept ID', ['game_id' => $game->id, 'ps_concept_id' => $conceptId]);
            }
        }

        if (! $productId && ! $conceptId) {
            $result = ['images' => 0, 'videos' => 0, 'prices' => 0];
            $this->logProvider('PlayStation Store', 'done', ['game_id' => $game->id, 'result' => $result]);

            return $result;
        }

        if (! $productId) {
            $productId = $playstation->resolveProductIdFromConcept($conceptId);
        }

        if (! $productId) {
            $result = ['images' => 0, 'videos' => 0, 'prices' => 0];
            $this->logProvider('PlayStation Store', 'done', ['game_id' => $game->id, 'result' => $result]);

            return $result;
        }

        // 2. Persist ID
        DB::table('video_games')->where('id', $game->id)->update([
            'attributes' => json_encode(array_merge($attributes, ['ps_product_id' => $productId])),
        ]);

        // 3. Fetch Full Details
        $data = $playstation->getFullDetails($productId);
        if (! $data) {
            $result = ['images' => 0, 'videos' => 0, 'prices' => 0];
            $this->logProvider('PlayStation Store', 'done', ['game_id' => $game->id, 'result' => $result]);

            return $result;
        }

        // 4. Standardized Media Persistence
        $aggregator->persistMedia($game->id, 'PlayStation Store', $data['media'] ?? []);

        // 5. Pricing Persistence
        $pricesSynced = 0;
        if ($worldwide) {
            if (method_exists($this, 'info')) {
                $this->info("   🌎 Syncing PlayStation prices worldwide for {$game->name}...");
            }

            $configuredRegions = array_filter(explode(',', env('PS_STORE_REGIONS', '')));
            if (! empty($configuredRegions)) {
                $this->line('      - Checking configured regions: '.implode(', ', $configuredRegions));
                foreach ($configuredRegions as $regionStr) {
                    try {
                        $parts = explode('-', $regionStr);
                        if (count($parts) !== 2) {
                            continue;
                        }

                        $lang = $parts[0];
                        $country = strtoupper($parts[1]);

                        $data = $playstation->getFullDetails($productId, $country, $lang);

                        if ($data && isset($data['price'])) {
                            $aggregator->persistStandardPrice($game->id, 'PlayStation Store', $country, $data['price']);
                            $pricesSynced++;
                        }
                    } catch (\Exception $e) {
                        // Ignore individual region failures
                    }
                }
            } else {
                // Fallback: sync from DB countries
                $result = $aggregator->syncRegionalPricesForRetailer((VideoGame::find($game->id)), 'PlayStation Store');
                $pricesSynced = $result['prices'];
            }
        } elseif (! empty($data['price'])) {
            $aggregator->persistStandardPrice($game->id, 'PlayStation Store', 'US', $data['price']);
            $pricesSynced = 1;
        }

        $result = [
            'images' => count($data['media']['images'] ?? []),
            'videos' => count($data['media']['videos'] ?? []),
            'prices' => $pricesSynced,
        ];

        $this->logProvider('PlayStation Store', 'done', ['game_id' => $game->id, 'result' => $result]);

        return $result;
    }

    /**
     * Enrich with Giant Bomb media - Standardized.
     */
    protected function enrichWithGiantBomb(GiantBombService $giantBomb, GameDataAggregatorService $aggregator, object $game): array
    {
        // 1. Resolve GUID
        $attributes = is_string($game->attributes ?? null) ? json_decode($game->attributes, true) : (array) ($game->attributes ?? []);
        $guid = $attributes['giantbomb_guid'] ?? null;

        if (! $guid) {
            $results = $giantBomb->search($game->name, 1);
            if (empty($results)) {
                return ['images' => 0, 'videos' => 0, 'prices' => 0];
            }
            $guid = $results[0]['guid'] ?? null;
        }
        if (! $guid) {
            return ['images' => 0, 'videos' => 0, 'prices' => 0];
        }

        // Persist ID
        $attributes = json_decode($game->attributes ?? '{}', true);
        DB::table('video_games')->where('id', $game->id)->update([
            'attributes' => json_encode(array_merge($attributes, ['giantbomb_guid' => $guid])),
        ]);

        // Try to use cached source_payload first
        $data = null;
        $cachedPayload = $this->getCachedSourcePayload($game, 'giantbomb');
        if ($cachedPayload) {
            // Transform cached payload to expected format
            $data = [
                'media' => [
                    'images' => array_map(fn ($img) => ['url' => $img['medium_url'] ?? $img['thumb_url'] ?? null], $cachedPayload['images'] ?? []),
                    'videos' => $cachedPayload['videos'] ?? [],
                ],
            ];
            Log::info('GiantBomb: Using cached source_payload', ['game_id' => $game->id]);
        }

        // Fallback to API call if no cache
        if (! $data) {
            $data = $giantBomb->getFullDetails($guid);
            if (! $data) {
                return ['images' => 0, 'videos' => 0, 'prices' => 0];
            }
        }

        // Standardized Media Persistence
        if (! empty($data['media']['videos'])) {
            foreach ($data['media']['videos'] as $video) {
                $streamData = $giantBomb->getVideoStreamUrls($video['guid']);
                $data['media']['videos'][] = [
                    'url' => $streamData['hd_url'] ?? $streamData['high_url'] ?? null,
                    'title' => $video['name'],
                    'thumbnail' => $video['thumbnail_url'],
                    'id' => $video['guid'],
                ];
            }
        }

        $aggregator->persistMedia($game->id, 'Giant Bomb', $data['media'] ?? []);

        return ['images' => 0, 'videos' => count($data['media']['videos'] ?? []), 'prices' => 0];
    }

    /**
     * Enrich with TGDB media - Standardized.
     */
    protected function enrichWithTGDB(TGDBService $tgdb, GameDataAggregatorService $aggregator, object $game): array
    {
        // 1. Resolve TGDB ID
        $attributes = is_string($game->attributes ?? null) ? json_decode($game->attributes, true) : (array) ($game->attributes ?? []);
        $tgdbId = $attributes['tgdb_id'] ?? null;

        if (! $tgdbId) {
            $results = $tgdb->search($game->name);
            if (empty($results)) {
                return ['images' => 0, 'videos' => 0, 'prices' => 0];
            }
            $tgdbId = $results[0]['id'] ?? null;
        }
        if (! $tgdbId) {
            return ['images' => 0, 'videos' => 0, 'prices' => 0];
        }

        // Persist ID
        $attributes = json_decode($game->attributes ?? '{}', true);
        DB::table('video_games')->where('id', $game->id)->update([
            'attributes' => json_encode(array_merge($attributes, ['tgdb_id' => $tgdbId])),
        ]);

        // Try to use cached source_payload first
        $data = null;
        $cachedPayload = $this->getCachedSourcePayload($game, 'tgdb');
        if ($cachedPayload) {
            $data = $cachedPayload;
            Log::info('TGDB: Using cached source_payload', ['game_id' => $game->id]);
        }

        // Fallback to API call if no cache
        if (! $data) {
            $data = $tgdb->getFullDetails($tgdbId);
            if (! $data) {
                return ['images' => 0, 'videos' => 0, 'prices' => 0];
            }
        }

        // Standardized Media Persistence
        $aggregator->persistMedia($game->id, 'TGDB', $data['media'] ?? []);

        return ['images' => count($data['media']['images'] ?? []) + count($data['media']['boxart'] ?? []), 'videos' => 0, 'prices' => 0];
    }

    /**
     * Enrich with itch.io data - Standardized.
     */
    protected function enrichWithItchIo(ItchIoScraperService $itchIo, GameDataAggregatorService $aggregator, object $game, bool $worldwide = false, bool $pricesOnly = false): array
    {
        $this->logProvider('itch.io', 'start', ['game_id' => $game->id, 'worldwide' => $worldwide, 'prices_only' => $pricesOnly]);
        // 1. Resolve itch.io URL
        $attributes = is_string($game->attributes ?? null) ? json_decode($game->attributes, true) : (array) ($game->attributes ?? []);
        $url = $attributes['itchio_url'] ?? null;

        if (! $url) {
            $results = $itchIo->search($game->name, 1);
            $url = $results[0]['url'] ?? null;
        }

        if (! $url) {
            return ['images' => 0, 'videos' => 0, 'prices' => 0];
        }

        // 2. Extract username and slug from URL
        if (preg_match('/https?:\/\/([^\.]+)\.itch\.io\/([^\/]+)/', $url, $matches)) {
            $username = $matches[1];
            $gameSlug = $matches[2];
        } else {
            return ['images' => 0, 'videos' => 0, 'prices' => 0];
        }

        // 3. Fetch Full Details
        $data = $itchIo->getFullDetails($gameSlug, $username);
        if (! $data) {
            return ['images' => 0, 'videos' => 0, 'prices' => 0];
        }

        if (! $pricesOnly) {
            // 4. Standardized Media Persistence
            $aggregator->persistMedia($game->id, 'itch.io', $data['media'] ?? []);
        }

        // 5. Pricing Persistence
        $pricesSynced = 0;
        if ($worldwide) {
            if (method_exists($this, 'info')) {
                $this->info("   🌎 Syncing itch.io prices worldwide for {$game->name}...");
            }
            $result = $aggregator->syncRegionalPricesForRetailer((VideoGame::find($game->id)), 'itch.io');
            $pricesSynced = $result['prices'];
        } elseif (! empty($data['price'])) {
            $aggregator->persistStandardPrice($game->id, 'itch.io', 'US', $data['price']);
            $pricesSynced = 1;
        }

        $result = [
            'images' => count($data['media']['images'] ?? []),
            'videos' => count($data['media']['videos'] ?? []),
            'prices' => $pricesSynced,
        ];

        $this->logProvider('itch.io', 'done', ['game_id' => $game->id, 'result' => $result]);

        return $result;
    }

    protected function enrichWithAmazon(GameDataAggregatorService $aggregator, object $game, bool $worldwide = false): array
    {
        $this->logProvider('Amazon', 'start', ['game_id' => $game->id, 'worldwide' => $worldwide]);
        $videoGame = VideoGame::find($game->id);
        if (! $videoGame) {
            return ['images' => 0, 'videos' => 0, 'prices' => 0];
        }

        // Check for Amazon URL
        $attributes = is_string($videoGame->attributes ?? null)
            ? json_decode($videoGame->attributes, true)
            : (array) ($videoGame->attributes ?? []);
        if (empty($attributes['amazon_url'])) {
            $result = ['images' => 0, 'videos' => 0, 'prices' => 0];
            $this->logProvider('Amazon', 'done', ['game_id' => $game->id, 'result' => $result, 'reason' => 'no_amazon_url']);

            return $result;
        }

        if ($worldwide) {
            if (method_exists($this, 'info')) {
                $this->info("   🌎 Syncing Amazon prices worldwide for {$game->name}...");
            }
            $result = $aggregator->syncRegionalPricesForRetailer($videoGame, 'Amazon');
        } else {
            $result = $aggregator->syncAmazonPrice($videoGame);
        }

        $result = ['images' => 0, 'videos' => 0, 'prices' => $result['prices'] ?? 0];
        $this->logProvider('Amazon', 'done', ['game_id' => $game->id, 'result' => $result]);

        return $result;
    }

    protected function enrichWithGog(GameDataAggregatorService $aggregator, object $game, bool $worldwide = false, bool $pricesOnly = false): array
    {
        $this->logProvider('GOG', 'start', ['game_id' => $game->id, 'worldwide' => $worldwide, 'prices_only' => $pricesOnly]);
        $videoGame = VideoGame::find($game->id);
        if (! $videoGame) {
            return ['images' => 0, 'videos' => 0, 'prices' => 0];
        }

        $attributes = is_string($videoGame->attributes ?? null)
            ? json_decode($videoGame->attributes, true)
            : (array) ($videoGame->attributes ?? []);

        $gogSlug = $attributes['gog_slug'] ?? null;

        // Fallback to external_links table (IGDB category 3 = GOG)
        if (empty($gogSlug)) {
            $gogSlug = $this->getExternalLinkId($game, 3);
            if ($gogSlug) {
                Log::info('GOG: Using external_links ID', ['game_id' => $game->id, 'gog_id' => $gogSlug]);
            }
        }

        if (empty($gogSlug)) {
            $result = ['images' => 0, 'videos' => 0, 'prices' => 0];
            $this->logProvider('GOG', 'done', ['game_id' => $game->id, 'result' => $result, 'reason' => 'no_gog_slug']);

            return $result;
        }

        $url = 'https://www.gog.com/game/'.$attributes['gog_slug'];

        if ($pricesOnly) {
            if ($worldwide) {
                if (method_exists($this, 'info')) {
                    $this->info("   🌎 Syncing GOG prices worldwide for {$game->name}...");
                }
                $result = $aggregator->syncRegionalPricesForRetailer($videoGame, 'GOG');
                $result = ['images' => 0, 'videos' => 0, 'prices' => $result['prices'] ?? 0];
                $this->logProvider('GOG', 'done', ['game_id' => $game->id, 'result' => $result]);

                return $result;
            }

            $price = app(\App\Services\Price\Gog\GogStoreService::class)->getPrice($url);
            if (! $price) {
                $result = ['images' => 0, 'videos' => 0, 'prices' => 0];
                $this->logProvider('GOG', 'done', ['game_id' => $game->id, 'result' => $result]);

                return $result;
            }

            $aggregator->persistStandardPrice($game->id, 'GOG', 'US', $price);

            $result = ['images' => 0, 'videos' => 0, 'prices' => 1];
            $this->logProvider('GOG', 'done', ['game_id' => $game->id, 'result' => $result]);

            return $result;
        }

        // Fetch full details (media + price)
        $data = app(\App\Services\Price\Gog\GogStoreService::class)->getFullDetails($url);
        if (! $data) {
            $result = ['images' => 0, 'videos' => 0, 'prices' => 0];
            $this->logProvider('GOG', 'done', ['game_id' => $game->id, 'result' => $result]);

            return $result;
        }

        // Persist media
        $aggregator->persistMedia($game->id, 'GOG', $data['media'] ?? []);

        // Persist prices
        $pricesSynced = 0;
        if ($worldwide) {
            if (method_exists($this, 'info')) {
                $this->info("   🌎 Syncing GOG prices worldwide for {$game->name}...");
            }
            $result = $aggregator->syncRegionalPricesForRetailer($videoGame, 'GOG');
            $pricesSynced = $result['prices'];
        } elseif (! empty($data['price'])) {
            $aggregator->persistStandardPrice($game->id, 'GOG', 'US', $data['price']);
            $pricesSynced = 1;
        }

        $result = [
            'images' => count($data['media']['screenshots'] ?? []) + (isset($data['media']['header_image']) ? 1 : 0),
            'videos' => count($data['media']['movies'] ?? []),
            'prices' => $pricesSynced,
        ];

        $this->logProvider('GOG', 'done', ['game_id' => $game->id, 'result' => $result]);

        return $result;
    }

    protected function enrichWithEpic(GameDataAggregatorService $aggregator, object $game, bool $worldwide = false, bool $pricesOnly = false): array
    {
        $this->logProvider('Epic Games', 'start', ['game_id' => $game->id, 'worldwide' => $worldwide, 'prices_only' => $pricesOnly]);
        $videoGame = VideoGame::find($game->id);
        if (! $videoGame) {
            return ['images' => 0, 'videos' => 0, 'prices' => 0];
        }

        $attributes = is_string($videoGame->attributes ?? null)
            ? json_decode($videoGame->attributes, true)
            : (array) ($videoGame->attributes ?? []);

        $epicSlug = $attributes['epic_slug'] ?? null;

        // Fallback to external_links table (IGDB category 1 = Epic Games Store)
        if (empty($epicSlug)) {
            $epicSlug = $this->getExternalLinkId($game, 1);
            if ($epicSlug) {
                Log::info('Epic: Using external_links ID', ['game_id' => $game->id, 'epic_id' => $epicSlug]);
            }
        }

        if (is_string($epicSlug) && $epicSlug !== '') {
            // Guard: external_links may contain a numeric ID (not a store slug). Epic content API needs the slug.
            if (preg_match('/^\d+$/', $epicSlug) === 1) {
                $result = ['images' => 0, 'videos' => 0, 'prices' => 0];
                $this->logProvider('Epic Games', 'done', ['game_id' => $game->id, 'result' => $result, 'reason' => 'invalid_epic_slug']);

                return $result;
            }

            // If we resolved a slug, persist it so syncRegionalPricesForRetailer() can build the URL.
            if (empty($attributes['epic_slug'])) {
                $attributes['epic_slug'] = $epicSlug;
                DB::table('video_games')->where('id', $game->id)->update([
                    'attributes' => json_encode($attributes),
                ]);
                $videoGame->setAttribute('attributes', $attributes);
            }
        }

        if (empty($epicSlug)) {
            $result = ['images' => 0, 'videos' => 0, 'prices' => 0];
            $this->logProvider('Epic Games', 'done', ['game_id' => $game->id, 'result' => $result, 'reason' => 'no_epic_slug']);

            return $result;
        }

        $url = 'https://store.epicgames.com/en-US/p/'.$epicSlug;

        if ($pricesOnly) {
            if ($worldwide) {
                if (method_exists($this, 'info')) {
                    $this->info("   🌎 Syncing Epic prices worldwide for {$game->name}...");
                }
                $result = $aggregator->syncRegionalPricesForRetailer($videoGame, 'Epic Games');
                $result = ['images' => 0, 'videos' => 0, 'prices' => $result['prices'] ?? 0];
                $this->logProvider('Epic Games', 'done', ['game_id' => $game->id, 'result' => $result]);

                return $result;
            }

            $price = app(\App\Services\Price\EpicGames\EpicGamesStoreService::class)->getPrice($url);
            if (! $price) {
                $result = ['images' => 0, 'videos' => 0, 'prices' => 0];
                $this->logProvider('Epic Games', 'done', ['game_id' => $game->id, 'result' => $result]);

                return $result;
            }

            $aggregator->persistStandardPrice($game->id, 'Epic Games', 'US', $price);

            $result = ['images' => 0, 'videos' => 0, 'prices' => 1];
            $this->logProvider('Epic Games', 'done', ['game_id' => $game->id, 'result' => $result]);

            return $result;
        }

        // Fetch full details (media + price)
        $data = app(\App\Services\Price\EpicGames\EpicGamesStoreService::class)->getFullDetails($url);
        if (! $data) {
            $result = ['images' => 0, 'videos' => 0, 'prices' => 0];
            $this->logProvider('Epic Games', 'done', ['game_id' => $game->id, 'result' => $result]);

            return $result;
        }

        // Persist media
        $aggregator->persistMedia($game->id, 'Epic Games', $data['media'] ?? []);

        // Persist prices
        $pricesSynced = 0;
        if ($worldwide) {
            if (method_exists($this, 'info')) {
                $this->info("   🌎 Syncing Epic prices worldwide for {$game->name}...");
            }
            $result = $aggregator->syncRegionalPricesForRetailer($videoGame, 'Epic Games');
            $pricesSynced = $result['prices'];
        } elseif (! empty($data['price'])) {
            $aggregator->persistStandardPrice($game->id, 'Epic Games', 'US', $data['price']);
            $pricesSynced = 1;
        }

        $result = [
            'images' => count($data['media']['screenshots'] ?? []) + (isset($data['media']['header_image']) ? 1 : 0),
            'videos' => count($data['media']['movies'] ?? []),
            'prices' => $pricesSynced,
        ];

        $this->logProvider('Epic Games', 'done', ['game_id' => $game->id, 'result' => $result]);

        return $result;
    }

    protected function enrichWithRawg(RawgService $rawg, GameDataAggregatorService $aggregator, object $game): array
    {
        $attributes = is_string($game->attributes ?? null) ? json_decode($game->attributes, true) : (array) ($game->attributes ?? []);
        $slug = $attributes['rawg_slug'] ?? $game->slug;

        if (! $slug) {
            return ['images' => 0, 'videos' => 0, 'prices' => 0];
        }

        // Try to use cached source_payload first
        $data = null;
        $cachedPayload = $this->getCachedSourcePayload($game, 'rawg');
        if ($cachedPayload) {
            $data = $cachedPayload;
            Log::info('RAWG: Using cached source_payload', ['game_id' => $game->id]);
        }

        // Fallback to API call if no cache
        if (! $data) {
            $data = $rawg->getGameDetails($slug);
            if (! $data || isset($data['detail'])) {
                return ['images' => 0, 'videos' => 0, 'prices' => 0];
            }
        }

        $media = ['screenshots' => [], 'banners' => []];
        if (! empty($data['background_image'])) {
            $media['banners'][] = ['url' => $data['background_image'], 'type' => 'hero'];
        }
        if (! empty($data['background_image_additional'])) {
            $media['banners'][] = ['url' => $data['background_image_additional'], 'type' => 'background'];
        }

        $aggregator->persistMedia($game->id, 'RAWG', $media);

        return ['images' => count($media['banners']), 'videos' => 0, 'prices' => 0];
    }

    /**
     * Resolve provider-specific IDs from IGDB websites metadata.
     */
    protected function resolveIdsFromExternalLinks(object $game, array $attributes): array
    {
        // 1. Get websites from metadata payloads - ensure it's always an array
        $websites = $attributes['websites'] ?? [];

        // Ensure $websites is an array (handle case where it might be a string)
        if (! is_array($websites)) {
            $websites = [];
        }

        if (empty($websites)) {
            $payload = $game->source_payload ?? '{}';
            while (is_string($payload)) {
                $decoded = json_decode($payload, true);
                if (json_last_error() !== JSON_ERROR_NONE || $decoded === $payload) {
                    break;
                }
                $payload = $decoded;
            }
            $websitesFromPayload = is_array($payload) ? ($payload['websites'] ?? []) : [];
            $websites = is_array($websitesFromPayload) ? $websitesFromPayload : [];
        }

        // Also check IGDB source directly if websites still empty
        if (empty($websites)) {
            $igdbSource = DB::table('video_game_title_sources')
                ->where('video_game_title_id', $game->video_game_title_id)
                ->where('provider', 'igdb')
                ->first();

            if ($igdbSource && $igdbSource->raw_payload) {
                $payload = $igdbSource->raw_payload;
                while (is_string($payload)) {
                    $decoded = json_decode($payload, true);
                    if (json_last_error() !== JSON_ERROR_NONE || $decoded === $payload) {
                        break;
                    }
                    $payload = $decoded;
                }
                $websitesFromPayload = is_array($payload) ? ($payload['websites'] ?? []) : [];
                $websites = is_array($websitesFromPayload) ? $websitesFromPayload : [];
            }
        }

        // 2. Get websites from the dedicated table
        $dbWebsites = DB::table('video_game_websites')
            ->where('video_game_id', $game->id)
            ->get();

        // Ensure $websites is an array before appending
        if (! is_array($websites)) {
            $websites = [];
        }

        foreach ($dbWebsites as $dbSite) {
            $websites[] = ['url' => $dbSite->url];
        }

        if (empty($websites)) {
            return $attributes;
        }

        if (! is_array($websites)) {
            return $attributes;
        }

        foreach ($websites as $site) {
            $url = $site['url'] ?? null;
            if (! $url) {
                continue;
            }

            // Steam
            if (str_contains($url, 'steampowered.com/app/')) {
                if (preg_match('/app\/(\d+)/', $url, $matches)) {
                    $attributes['steam_id'] = $attributes['steam_id'] ?? $matches[1];
                }
            }

            // Xbox Store (BigId is usually a 12-char alphanumeric at the end)
            if (str_contains($url, 'xbox.com/') && str_contains($url, '/store/')) {
                // Example: https://www.xbox.com/en-US/games/store/halo-infinite/9NP1P1WFS0LB
                $parts = explode('/', rtrim($url, '/'));
                $lastPart = end($parts);
                if (strlen($lastPart) === 12 && ctype_alnum($lastPart)) {
                    $attributes['xbox_bigid'] = $attributes['xbox_bigid'] ?? $lastPart;
                }
            }

            // PlayStation Store (Product ID like UP1234-CUSA12345_00-EXAMPLE)
            if (str_contains($url, 'store.playstation.com/')) {
                // Matches /product/ID or /en-us/product/ID
                if (preg_match('/product\/([A-Za-z0-9_\-]+)/', $url, $matches)) {
                    $attributes['ps_product_id'] = $attributes['ps_product_id'] ?? $matches[1];
                }
                // Matches /concept/12345
                if (preg_match('/concept\/(\d+)/', $url, $matches)) {
                    $attributes['ps_concept_id'] = $attributes['ps_concept_id'] ?? $matches[1];
                }
            }

            // Nintendo Store
            if (str_contains($url, 'nintendo.com/')) {
                if (str_contains($url, '/store/products/') || str_contains($url, '/games/detail/')) {
                    $attributes['nintendo_url'] = $attributes['nintendo_url'] ?? $url;
                }
            }

            // Amazon
            if (str_contains($url, 'amazon.')) {
                $attributes['amazon_url'] = $attributes['amazon_url'] ?? $url;
            }

            // GOG
            if (str_contains($url, 'gog.com/game/')) {
                $parts = explode('/', rtrim($url, '/'));
                $attributes['gog_slug'] = $attributes['gog_slug'] ?? end($parts);
            }

            // Epic Games
            if (str_contains($url, 'epicgames.com/p/') || str_contains($url, 'epicgames.com/store/')) {
                if (preg_match('/\/p\/([^\/\?]+)/', $url, $matches)) {
                    $attributes['epic_slug'] = $attributes['epic_slug'] ?? $matches[1];
                }
            }

            // itch.io (format: https://username.itch.io/game-slug)
            if (str_contains($url, 'itch.io/')) {
                $attributes['itchio_url'] = $attributes['itchio_url'] ?? $url;
            }
        }

        return $attributes;
    }

    private function logProvider(string $provider, string $event, array $context = []): void
    {
        Log::info("Enrich {$provider}: {$event}", $context);

        if (method_exists($this, 'line')) {
            $color = match ($event) {
                'start' => "\033[36m",
                'done' => "\033[32m",
                default => "\033[33m",
            };
            $this->line("{$color}{$provider}\033[0m {$event} ".json_encode($context));
        }
    }

    /**
     * Get cached source payload if available, avoiding redundant API calls.
     * Returns decoded payload array or null if not available.
     */
    private function getCachedSourcePayload(object $game, ?string $expectedProvider = null): ?array
    {
        if (empty($game->source_payload)) {
            return null;
        }

        $payload = $game->source_payload;

        // Handle double-encoded JSON (common issue)
        while (is_string($payload)) {
            $decoded = json_decode($payload, true);
            if (json_last_error() !== JSON_ERROR_NONE || $decoded === $payload) {
                break;
            }
            $payload = $decoded;
        }

        // Verify payload is valid array
        if (! is_array($payload) || empty($payload)) {
            return null;
        }

        // Optional: verify provider matches
        if ($expectedProvider && isset($game->provider) && $game->provider !== $expectedProvider) {
            return null;
        }

        return $payload;
    }

    /**
     * Get external link ID from video_game_external_links table.
     * IGDB category enum: 0=Steam, 1=Epic, 3=GOG, 5=Xbox, 13=PlayStation, 26=Itch.io, etc.
     */
    private function getExternalLinkId(object $game, int $category): ?string
    {
        $externalLink = DB::table('video_game_external_links')
            ->where('video_game_id', $game->id)
            ->where('category', $category)
            ->first();

        return $externalLink?->external_id;
    }
}
