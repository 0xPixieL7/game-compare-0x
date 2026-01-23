<?php

namespace App\Console\Commands;

use App\Services\Media\RAWG\RawgService;
use App\Services\Media\TGDB\TGDBService;
use App\Services\Price\GiantBomb\GiantBombService;
use App\Services\Price\ItchIo\ItchIoScraperService;
use App\Services\Price\PlayStation\PlayStationStoreService;
use App\Services\Price\Steam\SteamStoreService;
use App\Services\Price\Xbox\XboxStoreService;
use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;

class PersistAllProviderMediaCommand extends Command
{
    protected $signature = 'games:persist-all-media 
        {--discover-via=rawg : Provider to discover new games (rawg, tgdb)}
        {--enrich-providers=steam,xbox,playstation,giantbomb,tgdb,itchio : Comma-separated providers to enrich with}
        {--year=2024 : Year to discover games from}
        {--limit=10 : Games to discover}
        {--enrich-existing : Skip discovery, only enrich existing games}
        {--game-id= : Enrich specific game ID only}
        {--dry-run : Preview without saving}';

    protected $description = 'Discover games and enrich with media from all providers (Steam, Xbox, PS, GB, TGDB, RAWG, Itch.io)';

    public function handle(
        RawgService $rawg,
        SteamStoreService $steam,
        XboxStoreService $xbox,
        PlayStationStoreService $playstation,
        GiantBombService $giantBomb,
        TGDBService $tgdb,
        ItchIoScraperService $itchIo
    ) {
        $this->info('🎮 Multi-Provider Media Aggregation & Persistence');
        $this->newLine();

        $discoverVia = $this->option('discover-via');
        $enrichProviders = explode(',', $this->option('enrich-providers') ?? '');
        $enrichProviders = array_map('trim', $enrichProviders);
        $year = $this->option('year');
        $limit = (int) $this->option('limit');
        $enrichExisting = $this->option('enrich-existing');
        $gameId = $this->option('game-id');
        $dryRun = $this->option('dry-run');

        $stats = [
            'games_discovered' => 0,
            'games_enriched' => 0,
            'images_saved' => 0,
            'videos_saved' => 0,
            'prices_saved' => 0,
            'provider_calls' => 0,
            'errors' => 0,
        ];

        // Step 1: Discover new games (unless --enrich-existing)
        $gameIds = [];
        
        if ($gameId) {
            $gameIds = [$gameId];
            $this->info("🎯 Enriching specific game ID: {$gameId}");
        } elseif (!$enrichExisting) {
            $this->info("📥 Step 1: Discovering games via {$discoverVia}...");
            
            if ($discoverVia === 'rawg') {
                $discovered = $this->discoverViaRAWG($rawg, $year, $limit, $dryRun);
                $stats['games_discovered'] = count($discovered);
                $gameIds = $discovered;
            }
            
            $this->info("   Found {$stats['games_discovered']} games");
            $this->newLine();
        } else {
            // Get existing games to enrich
            $this->info("📚 Getting existing games to enrich...");
            $games = DB::table('video_games')
                ->orderBy('created_at', 'desc')
                ->limit($limit)
                ->pluck('id')
                ->toArray();
            $gameIds = $games;
            $this->info("   Found " . count($gameIds) . " games");
            $this->newLine();
        }

        // Step 2: Enrich games with media from all providers
        if (!empty($gameIds) && !empty($enrichProviders)) {
            $this->info("🎨 Step 2: Enriching games with media + prices from providers...");
            $this->line("   Providers: " . implode(', ', $enrichProviders));
            $this->newLine();

            $progressBar = $this->output->createProgressBar(count($gameIds));
            $progressBar->start();

            foreach ($gameIds as $videoGameId) {
                try {
                    $game = DB::table('video_games')->where('id', $videoGameId)->first();
                    
                    if (!$game) {
                        continue;
                    }

                    if (!$dryRun) {
                        foreach ($enrichProviders as $provider) {
                            $result = match($provider) {
                                'steam' => $this->enrichWithSteam($steam, $game),
                                'xbox' => $this->enrichWithXbox($xbox, $game),
                                'playstation' => $this->enrichWithPlayStation($playstation, $game),
                                'giantbomb' => $this->enrichWithGiantBomb($giantBomb, $game),
                                'tgdb' => $this->enrichWithTGDB($tgdb, $game),
                                'itchio' => $this->enrichWithItchIo($itchIo, $game),
                                default => ['images' => 0, 'videos' => 0, 'prices' => 0],
                            };

                            $stats['images_saved'] += $result['images'];
                            $stats['videos_saved'] += $result['videos'];
                            $stats['prices_saved'] += $result['prices'] ?? 0;
                            $stats['provider_calls']++;
                        }
                    }
                    
                    $stats['games_enriched']++;
                } catch (\Throwable $e) {
                    $stats['errors']++;
                    $this->newLine();
                    $this->error("   Error enriching game {$videoGameId}: {$e->getMessage()}");
                }
                
                $progressBar->advance();
            }

            $progressBar->finish();
            $this->newLine(2);
        }

        // Summary
        $this->info('=== Summary ===');
        $this->table(
            ['Metric', 'Count'],
            [
                ['Games Discovered', $stats['games_discovered']],
                ['Games Enriched', $stats['games_enriched']],
                ['Provider API Calls', $stats['provider_calls']],
                ['Images Saved', $stats['images_saved']],
                ['Videos Saved', $stats['videos_saved']],
                ['Prices Saved', $stats['prices_saved']],
                ['Errors', $stats['errors']],
            ]
        );

        return self::SUCCESS;
    }

    /**
     * Discover new games via RAWG and persist.
     */
    private function discoverViaRAWG(RawgService $rawg, string $year, int $limit, bool $dryRun): array
    {
        $response = $rawg->getAllGames([
            'dates' => "{$year}-01-01,{$year}-12-31",
            'ordering' => '-rating',
        ], page: 1, pageSize: $limit);

        $gameIds = [];

        foreach ($response['results'] ?? [] as $game) {
            if ($dryRun) {
                $this->line("   [DRY RUN] Would discover: {$game['name']}");
                continue;
            }

            $result = $this->persistRAWGGame($game);
            $gameIds[] = $result['game_id'];
        }

        return $gameIds;
    }

    /**
     * Persist RAWG game (same as before).
     */
    private function persistRAWGGame(array $game): array
    {
        $gameId = DB::transaction(function () use ($game) {
            $existingGame = DB::table('video_games')
                ->where('provider', 'rawg')
                ->where('external_id', (string) $game['id'])
                ->first();

            if ($existingGame) {
                return $existingGame->id;
            }

            // Extract developer and publisher names as JSON arrays
            $developers = array_map(fn($d) => $d['name'], $game['developers'] ?? []);
            $publishers = array_map(fn($p) => $p['name'], $game['publishers'] ?? []);

            return DB::table('video_games')->insertGetId([
                'name' => $game['name'],
                'slug' => $game['slug'],
                'provider' => 'rawg',
                'external_id' => (string) $game['id'],
                'description' => $game['description_raw'] ?? null,
                'summary' => mb_substr($game['description_raw'] ?? '', 0, 500),
                'url' => "https://rawg.io/games/{$game['slug']}",
                'release_date' => $game['released'] ?? null,
                'rating' => $game['rating'] ?? null,
                'rating_count' => $game['ratings_count'] ?? null,
                'genre' => json_encode(array_map(fn($g) => $g['name'], $game['genres'] ?? [])),
                'developer' => !empty($developers) ? json_encode($developers) : null,
                'publisher' => !empty($publishers) ? json_encode($publishers) : null,
                'platform' => json_encode(array_map(fn($p) => $p['platform']['name'] ?? 'Unknown', $game['platforms'] ?? [])),
                'attributes' => json_encode(['rawg_slug' => $game['slug']]),
                'source_payload' => json_encode($game),
                'created_at' => now(),
                'updated_at' => now(),
            ]);
        });

        // Save RAWG media
        $this->saveRAWGMedia($gameId, $game);

        return ['game_id' => $gameId];
    }

    private function saveRAWGMedia(int $gameId, array $game): int
    {
        $count = 0;

        if (!empty($game['background_image'])) {
            DB::table('images')->updateOrInsert(
                ['imageable_type' => 'App\\Models\\VideoGame', 'imageable_id' => $gameId, 'url' => $game['background_image']],
                ['metadata' => json_encode(['source' => 'rawg', 'type' => 'hero']), 'updated_at' => now(), 'created_at' => now()]
            );
            $count++;
        }

        if (!empty($game['background_image_additional'])) {
            DB::table('images')->updateOrInsert(
                ['imageable_type' => 'App\\Models\\VideoGame', 'imageable_id' => $gameId, 'url' => $game['background_image_additional']],
                ['metadata' => json_encode(['source' => 'rawg', 'type' => 'background']), 'updated_at' => now(), 'created_at' => now()]
            );
            $count++;
        }

        return $count;
    }

    /**
     * Enrich with Steam media AND prices - COMPLETE EDITION (fills every column).
     */
    private function enrichWithSteam(SteamStoreService $steam, object $game): array
    {
        // Try to find Steam ID from external links or metadata
        $attributes = json_decode($game->attributes ?? '{}', true);
        $steamId = $attributes['steam_id'] ?? null;

        if (!$steamId) {
            // Try searching by name
            $steamId = $steam->search($game->name);
        }

        if (!$steamId) {
            return ['images' => 0, 'videos' => 0, 'prices' => 0];
        }

        // Save Steam ID for future use
        DB::table('video_games')
            ->where('id', $game->id)
            ->update([
                'attributes' => json_encode(array_merge($attributes, ['steam_id' => $steamId])),
            ]);

        $data = $steam->getFullDetails((string) $steamId);
        
        if (!$data) {
            return ['images' => 0, 'videos' => 0, 'prices' => 0];
        }

        $imagesCount = 0;
        $videosCount = 0;
        $pricesCount = 0;

        $steamUrl = "https://store.steampowered.com/app/{$steamId}";
        $metadata = $data['metadata'] ?? [];

        // Save header image (COMPLETE - all columns including media_id)
        if (!empty($data['media']['header_image'])) {
            // Create or update media record first
            $existingMedia = DB::table('media')
                ->where('model_type', 'App\\Models\\VideoGame')
                ->where('model_id', $game->id)
                ->where('collection_name', 'header')
                ->first();
            
            if ($existingMedia) {
                $mediaId = $existingMedia->id;
            } else {
                $mediaId = DB::table('media')->insertGetId([
                    'model_type' => 'App\\Models\\VideoGame',
                    'model_id' => $game->id,
                    'uuid' => \Illuminate\Support\Str::uuid(),
                    'collection_name' => 'header',
                    'name' => 'header-image',
                    'file_name' => basename(parse_url($data['media']['header_image'], PHP_URL_PATH)),
                    'mime_type' => 'image/jpeg',
                    'disk' => 'public',
                    'conversions_disk' => 'public',
                    'size' => 0,
                    'manipulations' => json_encode([]),
                    'custom_properties' => json_encode(['steam_id' => $steamId, 'type' => 'header']),
                    'generated_conversions' => json_encode([]),
                    'responsive_images' => json_encode([]),
                    'order_column' => 1,
                    'created_at' => now(),
                    'updated_at' => now(),
                ]);
            }
            
            // Then create image record linked to media
            DB::table('images')->updateOrInsert(
                ['imageable_type' => 'App\\Models\\VideoGame', 'imageable_id' => $game->id, 'url' => $data['media']['header_image']],
                [
                    'video_game_id' => $game->id,
                    'media_id' => $mediaId,
                    'provider' => 'steam',
                    'external_id' => "steam-{$steamId}-header",
                    'source_url' => $steamUrl,
                    'collection_names' => json_encode(['header', 'cover']),
                    'primary_collection' => 'default',
                    'alt_text' => ($metadata['name'] ?? $game->name) . ' - Header Image',
                    'urls' => json_encode(['original' => $data['media']['header_image']]),
                    'metadata' => json_encode(['source' => 'steam', 'type' => 'header', 'steam_id' => $steamId]),
                    'updated_at' => now(),
                    'created_at' => now(),
                ]
            );
            $imagesCount++;
        }

        // Save background image (COMPLETE including media_id)
        if (!empty($data['media']['background'])) {
            // Create or update media record
            $existingMedia = DB::table('media')
                ->where('model_type', 'App\\Models\\VideoGame')
                ->where('model_id', $game->id)
                ->where('collection_name', 'background')
                ->first();
            
            if ($existingMedia) {
                $mediaId = $existingMedia->id;
            } else {
                $mediaId = DB::table('media')->insertGetId([
                    'model_type' => 'App\\Models\\VideoGame',
                    'model_id' => $game->id,
                    'uuid' => \Illuminate\Support\Str::uuid(),
                    'collection_name' => 'background',
                    'name' => 'background-image',
                    'file_name' => basename(parse_url($data['media']['background'], PHP_URL_PATH)),
                    'mime_type' => 'image/jpeg',
                    'disk' => 'public',
                    'conversions_disk' => 'public',
                    'size' => 0,
                    'manipulations' => json_encode([]),
                    'custom_properties' => json_encode(['steam_id' => $steamId, 'type' => 'background']),
                    'generated_conversions' => json_encode([]),
                    'responsive_images' => json_encode([]),
                    'order_column' => 2,
                    'created_at' => now(),
                    'updated_at' => now(),
                ]);
            }
            
            // Create image record
            DB::table('images')->updateOrInsert(
                ['imageable_type' => 'App\\Models\\VideoGame', 'imageable_id' => $game->id, 'url' => $data['media']['background']],
                [
                    'video_game_id' => $game->id,
                    'media_id' => $mediaId,
                    'provider' => 'steam',
                    'external_id' => "steam-{$steamId}-background",
                    'source_url' => $steamUrl,
                    'collection_names' => json_encode(['background', 'hero']),
                    'primary_collection' => 'default',
                    'alt_text' => ($metadata['name'] ?? $game->name) . ' - Background',
                    'urls' => json_encode(['original' => $data['media']['background']]),
                    'metadata' => json_encode(['source' => 'steam', 'type' => 'background', 'steam_id' => $steamId]),
                    'updated_at' => now(),
                    'created_at' => now(),
                ]
            );
            $imagesCount++;
        }

        // Save movies/trailers (COMPLETE - all columns including media_id)
        foreach ($data['media']['movies'] ?? [] as $index => $movie) {
            $url = $movie['mp4_max'] ?? $movie['webm_max'] ?? null;
            if ($url) {
                // Create media record for video
                $mediaId = DB::table('media')->insertGetId([
                    'model_type' => 'App\\Models\\VideoGame',
                    'model_id' => $game->id,
                    'uuid' => \Illuminate\Support\Str::uuid(),
                    'collection_name' => 'trailers',
                    'name' => $movie['name'] ?? "trailer-{$index}",
                    'file_name' => basename(parse_url($url, PHP_URL_PATH)),
                    'mime_type' => 'video/mp4',
                    'disk' => 'public',
                    'conversions_disk' => 'public',
                    'size' => 0,
                    'manipulations' => json_encode([]),
                    'custom_properties' => json_encode(['steam_id' => $steamId, 'type' => 'trailer', 'highlight' => $movie['highlight'] ?? false]),
                    'generated_conversions' => json_encode([]),
                    'responsive_images' => json_encode([]),
                    'order_column' => $index + 1,
                    'created_at' => now(),
                    'updated_at' => now(),
                ]);
                
                // Create video record
                DB::table('videos')->updateOrInsert(
                    ['videoable_type' => 'App\\Models\\VideoGame', 'videoable_id' => $game->id, 'url' => $url],
                    [
                        'video_game_id' => $game->id,
                        'media_id' => $mediaId,
                        'provider' => 'steam',
                        'video_id' => $movie['id'] ?? "steam-{$steamId}-video-{$index}",
                        'external_id' => $movie['id'] ?? "steam-{$steamId}-video-{$index}",
                        'source_url' => $steamUrl,
                        'collection_names' => json_encode(['trailers', 'gameplay']),
                        'primary_collection' => 'default',
                        'thumbnail_url' => $movie['thumbnail'] ?? null,
                        'title' => $movie['name'] ?? ($metadata['name'] ?? $game->name) . ' - Trailer',
                        'description' => "Official trailer for " . ($metadata['name'] ?? $game->name),
                        'urls' => json_encode([
                            'webm_480' => $movie['webm_480'] ?? null,
                            'webm_max' => $movie['webm_max'] ?? null,
                            'mp4_480' => $movie['mp4_480'] ?? null,
                            'mp4_max' => $movie['mp4_max'] ?? null,
                        ]),
                        'order_column' => $index,
                        'metadata' => json_encode([
                            'source' => 'steam',
                            'steam_id' => $steamId,
                            'highlight' => $movie['highlight'] ?? false,
                        ]),
                        'updated_at' => now(),
                        'created_at' => now(),
                    ]
                );
                $videosCount++;
            }
        }

        // Save price data - COMPLETE (all columns)
        if (!empty($data['price'])) {
            $priceData = $data['price'];
            
            // First, create or update the product (COMPLETE)
            DB::table('products')->updateOrInsert(
                ['id' => (string) $steamId],
                [
                    'id' => (string) $steamId,
                    'type' => 'steam',
                    'name' => $metadata['name'] ?? $game->name,
                    'slug' => 'steam-' . $steamId,
                    'title' => $metadata['name'] ?? $game->name,
                    'normalized_title' => strtolower(preg_replace('/[^a-z0-9]+/i', '-', $metadata['name'] ?? $game->name)),
                    'platform' => 'PC',
                    'category' => $metadata['type'] ?? 'game',
                    'synopsis' => $metadata['short_description'] ?? null,
                    'release_date' => $metadata['release_date'] ?? $game->release_date,
                    'rating' => $game->rating,
                    'popularity_score' => $game->popularity_score,
                    'external_ids' => json_encode([
                        'steam_id' => $steamId,
                        'steam_appid' => $steamId,
                    ]),
                    'metadata' => json_encode([
                        'source' => 'steam',
                        'is_free' => $metadata['is_free'] ?? false,
                        'developers' => $metadata['developers'] ?? [],
                        'publishers' => $metadata['publishers'] ?? [],
                    ]),
                    'created_at' => now(),
                    'updated_at' => now(),
                ]
            );
            
            // Then save the price (COMPLETE - all columns)
            DB::table('video_game_prices')->updateOrInsert(
                [
                    'video_game_id' => $game->id,
                    'retailer' => 'Steam',
                    'country_code' => 'US',
                ],
                [
                    'product_id' => (string) $steamId,
                    'currency' => $priceData['currency'] ?? 'USD',
                    'country_code' => 'US',
                    'region_code' => 'NA',
                    'condition' => 'new',
                    'amount_minor' => $priceData['amount_minor'] ?? 0,
                    'url' => $steamUrl,
                    'sku' => (string) $steamId,
                    'recorded_at' => now(),
                    'tax_inclusive' => false, // US Steam prices exclude tax
                    'is_active' => true,
                    'is_retail_buy' => true,
                    'metadata' => json_encode([
                        'source' => 'steam',
                        'discount_percent' => $priceData['discount_percent'] ?? 0,
                        'initial_amount_minor' => $priceData['initial_amount_minor'] ?? null,
                        'price_overview' => $priceData,
                    ]),
                    'updated_at' => now(),
                    'created_at' => now(),
                ]
            );
            $pricesCount++;
        }

        return ['images' => $imagesCount, 'videos' => $videosCount, 'prices' => $pricesCount];
    }

    /**
     * Enrich with Xbox media and prices.
     */
    private function enrichWithXbox(XboxStoreService $xbox, object $game): array
    {
        // Search for game on Xbox Store
        $results = $xbox->search($game->name);
        
        if (empty($results)) {
            return ['images' => 0, 'videos' => 0, 'prices' => 0];
        }

        // Try to match title exactly, otherwise take first
        $xboxId = null;
        $cleanSearch = strtolower(preg_replace('/[^a-z0-9]/', '', $game->name));
        
        foreach ($results as $result) {
            $cleanResult = strtolower(preg_replace('/[^a-z0-9]/', '', $result['title']));
            if ($cleanResult === $cleanSearch) {
                $xboxId = $result['id'];
                break;
            }
        }

        if (!$xboxId) {
            $xboxId = $results[0]['id'];
        }

        $data = $xbox->getFullDetails($xboxId);
        
        if (!$data) {
            return ['images' => 0, 'videos' => 0, 'prices' => 0];
        }

        $imagesCount = 0;
        $videosCount = 0;
        $pricesCount = 0;

        // Save Xbox images
        foreach ($data['media']['images'] ?? [] as $image) {
            DB::table('images')->updateOrInsert(
                ['imageable_type' => 'App\\Models\\VideoGame', 'imageable_id' => $game->id, 'url' => $image['url']],
                [
                    'metadata' => json_encode([
                        'source' => 'xbox',
                        'purpose' => $image['purpose'] ?? 'unknown',
                        'width' => $image['width'] ?? null,
                        'height' => $image['height'] ?? null
                    ]),
                    'updated_at' => now(),
                    'created_at' => now()
                ]
            );
            $imagesCount++;
        }

        // Save Xbox videos
        foreach ($data['media']['videos'] ?? [] as $video) {
            DB::table('videos')->updateOrInsert(
                ['videoable_type' => 'App\\Models\\VideoGame', 'videoable_id' => $game->id, 'url' => $video['url']],
                [
                    'thumbnail_url' => $video['thumbnail'] ?? null,
                    'title' => $video['title'] ?? (($game->name) . ' - Trailer'),
                    'metadata' => json_encode(['source' => 'xbox', 'duration' => $video['duration'] ?? null]),
                    'updated_at' => now(),
                    'created_at' => now()
                ]
            );
            $videosCount++;
        }

        // Save Xbox price
        if (!empty($data['price'])) {
            $priceData = $data['price'];
            DB::table('video_game_prices')->updateOrInsert(
                [
                    'video_game_id' => $game->id,
                    'retailer' => 'Xbox',
                    'country_code' => $priceData['market'] ?? 'US',
                ],
                [
                    'sku' => $xboxId,
                    'product_id' => null, // Xbox IDs are alphanumeric, don't fit in int8 column
                    'currency' => $priceData['currency'] ?? 'USD',
                    'amount_minor' => $priceData['amount_minor'] ?? 0,
                    'url' => "https://www.xbox.com/en-US/games/store/p/{$xboxId}",
                    'recorded_at' => now(),
                    'is_active' => true,
                    'metadata' => json_encode([
                        'source' => 'xbox',
                        'market' => $priceData['market'] ?? 'US',
                        'msrp' => $priceData['msrp'] ?? null,
                        'list_price' => $priceData['list_price'] ?? null,
                    ]),
                    'updated_at' => now(),
                    'created_at' => now(),
                ]
            );
            $pricesCount++;
        }

        return ['images' => $imagesCount, 'videos' => $videosCount, 'prices' => $pricesCount];
    }


    /**
     * Enrich with PlayStation media.
     */
    private function enrichWithPlayStation(PlayStationStoreService $playstation, object $game): array
    {
        // Would need PS product ID - skip for now unless we have it
        return ['images' => 0, 'videos' => 0, 'prices' => 0];
    }

    /**
     * Enrich with Giant Bomb media.
     */
    private function enrichWithGiantBomb(GiantBombService $giantBomb, object $game): array
    {
        // Search for game
        $results = $giantBomb->search($game->name, 1);
        
        if (empty($results)) {
            return ['images' => 0, 'videos' => 0, 'prices' => 0];
        }

        $guid = $results[0]['guid'] ?? null;
        if (!$guid) {
            return ['images' => 0, 'videos' => 0, 'prices' => 0];
        }

        $data = $giantBomb->getFullDetails($guid);
        
        if (!$data) {
            return ['images' => 0, 'videos' => 0, 'prices' => 0];
        }

        $videosCount = 0;

        // Save Giant Bomb videos (gameplay, reviews)
        foreach ($data['media']['videos'] ?? [] as $video) {
            $streamData = $giantBomb->getVideoStreamUrls($video['guid']);
            $url = $streamData['hd_url'] ?? $streamData['high_url'] ?? null;
            
            if ($url) {
                DB::table('videos')->updateOrInsert(
                    ['videoable_type' => 'App\\Models\\VideoGame', 'videoable_id' => $game->id, 'url' => $url],
                    [
                        'thumbnail_url' => $video['thumbnail_url'] ?? null,
                        'title' => $video['name'] ?? null,
                        'metadata' => json_encode(['source' => 'giantbomb', 'video_type' => $video['video_type'] ?? 'unknown', 'duration' => $video['duration'] ?? null]),
                        'updated_at' => now(),
                        'created_at' => now()
                    ]
                );
                $videosCount++;
            }
        }

        return ['images' => 0, 'videos' => $videosCount, 'prices' => 0];
    }

    /**
     * Enrich with TGDB media.
     */
    private function enrichWithTGDB(TGDBService $tgdb, object $game): array
    {
        // Search for game
        $results = $tgdb->search($game->name);
        
        if (empty($results)) {
            return ['images' => 0, 'videos' => 0, 'prices' => 0];
        }

        $tgdbId = $results[0]['id'] ?? null;
        if (!$tgdbId) {
            return ['images' => 0, 'videos' => 0, 'prices' => 0];
        }

        $data = $tgdb->getFullDetails($tgdbId);
        
        if (!$data) {
            return ['images' => 0, 'videos' => 0, 'prices' => 0];
        }

        $imagesCount = 0;

        // Save box art (front)
        foreach ($data['media']['boxart'] ?? [] as $boxart) {
            if (($boxart['side'] ?? null) === 'front' && !empty($boxart['url'])) {
                DB::table('images')->updateOrInsert(
                    ['imageable_type' => 'App\\Models\\VideoGame', 'imageable_id' => $game->id, 'url' => $boxart['url']],
                    ['metadata' => json_encode(['source' => 'tgdb', 'type' => 'boxart_front']), 'updated_at' => now(), 'created_at' => now()]
                );
                $imagesCount++;
                break;
            }
        }

        // Save clear logo
        foreach ($data['media']['clearlogo'] ?? [] as $logo) {
            if (!empty($logo['url'])) {
                DB::table('images')->updateOrInsert(
                    ['imageable_type' => 'App\\Models\\VideoGame', 'imageable_id' => $game->id, 'url' => $logo['url']],
                    ['metadata' => json_encode(['source' => 'tgdb', 'type' => 'clearlogo']), 'updated_at' => now(), 'created_at' => now()]
                );
                $imagesCount++;
                break;
            }
        }

        return ['images' => $imagesCount, 'videos' => 0, 'prices' => 0];
    }

    /**
     * Enrich with itch.io media and prices.
     */
    private function enrichWithItchIo(ItchIoScraperService $itchIo, object $game): array
    {
        // Search for game on itch.io
        $results = $itchIo->search($game->name, 1);
        
        if (empty($results)) {
            return ['images' => 0, 'videos' => 0, 'prices' => 0];
        }

        $url = $results[0]['url'] ?? null;
        if (!$url) {
            return ['images' => 0, 'videos' => 0, 'prices' => 0];
        }

        // Extract username and slug from URL
        if (preg_match('/https?:\/\/([^\.]+)\.itch\.io\/([^\/]+)/', $url, $matches)) {
            $username = $matches[1];
            $gameSlug = $matches[2];
        } else {
            return ['images' => 0, 'videos' => 0, 'prices' => 0];
        }

        $data = $itchIo->getFullDetails($gameSlug, $username);
        
        if (!$data) {
            return ['images' => 0, 'videos' => 0, 'prices' => 0];
        }

        $imagesCount = 0;
        $videosCount = 0;
        $pricesCount = 0;

        // Save itch.io images
        foreach ($data['media']['images'] ?? [] as $image) {
            DB::table('images')->updateOrInsert(
                ['imageable_type' => 'App\\Models\\VideoGame', 'imageable_id' => $game->id, 'url' => $image['url']],
                ['metadata' => json_encode(['source' => 'itch.io', 'type' => $image['type'] ?? 'screenshot']), 'updated_at' => now(), 'created_at' => now()]
            );
            $imagesCount++;
        }

        // Save itch.io videos
        foreach ($data['media']['videos'] ?? [] as $video) {
            DB::table('videos')->updateOrInsert(
                ['videoable_type' => 'App\\Models\\VideoGame', 'videoable_id' => $game->id, 'url' => $video['url']],
                [
                    'thumbnail_url' => $video['thumbnail'] ?? null,
                    'title' => ($game->name) . ' - Trailer',
                    'metadata' => json_encode(['source' => 'itch.io', 'type' => 'trailer']),
                    'updated_at' => now(),
                    'created_at' => now()
                ]
            );
            $videosCount++;
        }

        // Save itch.io price
        if (!empty($data['price'])) {
            $priceData = $data['price'];
            DB::table('video_game_prices')->updateOrInsert(
                [
                    'video_game_id' => $game->id,
                    'retailer' => 'itch.io',
                    'country_code' => 'US',
                ],
                [
                    'sku' => "itchio-{$username}-{$gameSlug}",
                    'currency' => $priceData['currency'] ?? 'USD',
                    'amount_minor' => $priceData['amount_minor'] ?? 0,
                    'url' => $url,
                    'recorded_at' => now(),
                    'is_active' => true,
                    'metadata' => json_encode([
                        'source' => 'itch.io',
                        'display_price' => $priceData['display_price'] ?? null,
                        'is_free' => $priceData['is_free'] ?? false,
                        'is_pwyw' => $priceData['is_pwyw'] ?? false,
                    ]),
                    'updated_at' => now(),
                    'created_at' => now(),
                ]
            );
            $pricesCount++;
        }

        return ['images' => $imagesCount, 'videos' => $videosCount, 'prices' => $pricesCount];
    }
}
