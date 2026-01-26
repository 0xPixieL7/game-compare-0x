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

trait InteractsWithEnrichmentProviders
{
    /**
     * Enrich with Steam media AND prices - Standardized.
     */
    protected function enrichWithSteam(SteamStoreService $steam, GameDataAggregatorService $aggregator, object $game, bool $worldwide = false): array
    {
        // 1. Resolve Steam ID
        $attributes = is_string($game->attributes ?? null) ? json_decode($game->attributes, true) : (array) ($game->attributes ?? []);
        $steamId = $attributes['steam_id'] ?? $steam->search($game->name);

        if (! $steamId) {
            return ['images' => 0, 'videos' => 0, 'prices' => 0];
        }

        // 2. Persist ID
        DB::table('video_games')->where('id', $game->id)->update([
            'attributes' => json_encode(array_merge($attributes, ['steam_id' => $steamId])),
        ]);

        // 3. Fetch Full Details (Media + Base Price)
        $data = $steam->getFullDetails((string) $steamId);
        if (! $data) {
            return ['images' => 0, 'videos' => 0, 'prices' => 0];
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

        return [
            'images' => count($data['media']['screenshots'] ?? []) + (isset($data['media']['header_image']) ? 1 : 0),
            'videos' => count($data['media']['movies'] ?? []),
            'prices' => $pricesSynced,
        ];
    }

    /**
     * Enrich with Xbox Store data - Standardized.
     */
    protected function enrichWithXbox(XboxStoreService $xbox, GameDataAggregatorService $aggregator, object $game, bool $worldwide = false): array
    {
        // 1. Resolve Xbox Product ID
        $attributes = is_string($game->attributes ?? null) ? json_decode($game->attributes, true) : (array) ($game->attributes ?? []);
        $productId = $attributes['xbox_bigid'] ?? $xbox->resolveProductId($game->name);

        if (! $productId) {
            return ['images' => 0, 'videos' => 0, 'prices' => 0];
        }

        // 2. Persist ID
        DB::table('video_games')->where('id', $game->id)->update([
            'attributes' => json_encode(array_merge($attributes, ['xbox_bigid' => $productId])),
        ]);

        // 3. Fetch Full Details
        $data = $xbox->getFullDetails($productId);
        if (! $data) {
            return ['images' => 0, 'videos' => 0, 'prices' => 0];
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

        return [
            'images' => count($data['media']['images'] ?? []),
            'videos' => count($data['media']['videos'] ?? []),
            'prices' => $pricesSynced,
        ];
    }

    /**
     * Enrich with PlayStation Store data - Standardized.
     */
    protected function enrichWithPlayStation(PlayStationStoreService $playstation, GameDataAggregatorService $aggregator, object $game, bool $worldwide = false): array
    {
        // 1. Resolve PS Product ID
        $attributes = is_string($game->attributes ?? null) ? json_decode($game->attributes, true) : (array) ($game->attributes ?? []);
        $productId = $attributes['ps_product_id'] ?? $playstation->resolveProductId($game->name);

        if (! $productId) {
            return ['images' => 0, 'videos' => 0, 'prices' => 0];
        }

        // 2. Persist ID
        DB::table('video_games')->where('id', $game->id)->update([
            'attributes' => json_encode(array_merge($attributes, ['ps_product_id' => $productId])),
        ]);

        // 3. Fetch Full Details
        $data = $playstation->getFullDetails($productId);
        if (! $data) {
            return ['images' => 0, 'videos' => 0, 'prices' => 0];
        }

        // 4. Standardized Media Persistence
        $aggregator->persistMedia($game->id, 'PlayStation Store', $data['media'] ?? []);

        // 5. Pricing Persistence
        $pricesSynced = 0;
        if ($worldwide) {
            if (method_exists($this, 'info')) {
                $this->info("   🌎 Syncing PlayStation prices worldwide for {$game->name}...");
            }

            // 1. Sync from DB Countries (Standard)
            $result = $aggregator->syncRegionalPricesForRetailer((VideoGame::find($game->id)), 'PlayStation Store');
            $pricesSynced = $result['prices'];

            // 2. Sync from .env Configuration (Explicit)
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
            }
        } elseif (! empty($data['price'])) {
            $aggregator->persistStandardPrice($game->id, 'PlayStation Store', 'US', $data['price']);
            $pricesSynced = 1;
        }

        return [
            'images' => count($data['media']['images'] ?? []),
            'videos' => count($data['media']['videos'] ?? []),
            'prices' => $pricesSynced,
        ];
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

        $data = $giantBomb->getFullDetails($guid);
        if (! $data) {
            return ['images' => 0, 'videos' => 0, 'prices' => 0];
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

        $data = $tgdb->getFullDetails($tgdbId);
        if (! $data) {
            return ['images' => 0, 'videos' => 0, 'prices' => 0];
        }

        // Standardized Media Persistence
        $aggregator->persistMedia($game->id, 'TGDB', $data['media'] ?? []);

        return ['images' => count($data['media']['images'] ?? []) + count($data['media']['boxart'] ?? []), 'videos' => 0, 'prices' => 0];
    }

    /**
     * Enrich with itch.io data - Standardized.
     */
    protected function enrichWithItchIo(ItchIoScraperService $itchIo, GameDataAggregatorService $aggregator, object $game, bool $worldwide = false): array
    {
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

        // 4. Standardized Media Persistence
        $aggregator->persistMedia($game->id, 'itch.io', $data['media'] ?? []);

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

        return [
            'images' => count($data['media']['images'] ?? []),
            'videos' => count($data['media']['videos'] ?? []),
            'prices' => $pricesSynced,
        ];
    }

    protected function enrichWithAmazon(GameDataAggregatorService $aggregator, object $game, bool $worldwide = false): array
    {
        $videoGame = VideoGame::find($game->id);
        if (! $videoGame) {
            return ['images' => 0, 'videos' => 0, 'prices' => 0];
        }

        if ($worldwide) {
            if (method_exists($this, 'info')) {
                $this->info("   🌎 Syncing Amazon prices worldwide for {$game->name}...");
            }
            $result = $aggregator->syncRegionalPricesForRetailer($videoGame, 'Amazon');
        } else {
            $result = $aggregator->syncAmazonPrice($videoGame);
        }

        return ['images' => 0, 'videos' => 0, 'prices' => $result['prices'] ?? 0];
    }

    protected function enrichWithGog(GameDataAggregatorService $aggregator, object $game, bool $worldwide = false): array
    {
        $videoGame = VideoGame::find($game->id);
        if (! $videoGame) {
            return ['images' => 0, 'videos' => 0, 'prices' => 0];
        }

        if ($worldwide) {
            if (method_exists($this, 'info')) {
                $this->info("   🌎 Syncing GOG prices worldwide for {$game->name}...");
            }
            $result = $aggregator->syncRegionalPricesForRetailer($videoGame, 'GOG');
        } else {
            $result = $aggregator->syncGogPrice($videoGame);
        }

        return ['images' => 0, 'videos' => 0, 'prices' => $result['prices'] ?? 0];
    }

    protected function enrichWithEpic(GameDataAggregatorService $aggregator, object $game, bool $worldwide = false): array
    {
        $videoGame = VideoGame::find($game->id);
        if (! $videoGame) {
            return ['images' => 0, 'videos' => 0, 'prices' => 0];
        }

        if ($worldwide) {
            if (method_exists($this, 'info')) {
                $this->info("   🌎 Syncing Epic prices worldwide for {$game->name}...");
            }
            $result = $aggregator->syncRegionalPricesForRetailer($videoGame, 'Epic Games');
        } else {
            $result = $aggregator->syncEpicPrice($videoGame);
        }

        return ['images' => 0, 'videos' => 0, 'prices' => $result['prices'] ?? 0];
    }

    protected function enrichWithRawg(RawgService $rawg, GameDataAggregatorService $aggregator, object $game): array
    {
        $attributes = is_string($game->attributes ?? null) ? json_decode($game->attributes, true) : (array) ($game->attributes ?? []);
        $slug = $attributes['rawg_slug'] ?? $game->slug;

        if (! $slug) {
            return ['images' => 0, 'videos' => 0, 'prices' => 0];
        }

        $data = $rawg->getGameDetails($slug);
        if (! $data || isset($data['detail'])) {
            return ['images' => 0, 'videos' => 0, 'prices' => 0];
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
            $websites = is_array($payload) ? ($payload['websites'] ?? []) : [];
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
                $websites = is_array($payload) ? ($payload['websites'] ?? []) : [];
            }
        }

        // 2. Get websites from the dedicated table
        $dbWebsites = DB::table('video_game_websites')
            ->where('video_game_id', $game->id)
            ->get();

        foreach ($dbWebsites as $dbSite) {
            $websites[] = ['url' => $dbSite->url];
        }

        if (empty($websites)) {
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
        }

        return $attributes;
    }
}
