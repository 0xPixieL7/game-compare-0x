<?php

namespace App\Console\Commands\Enrich;

use App\Services\Media\RAWG\RawgService;
use App\Services\Media\TGDB\TGDBService;
use App\Services\Price\Amazon\AmazonScraperService;
use App\Services\Price\GameDataAggregatorService;
use App\Services\Price\GiantBomb\GiantBombService;
use App\Services\Price\ItchIo\ItchIoScraperService;
use App\Services\Price\PlayStation\PlayStationStoreService;
use App\Services\Price\Steam\SteamStoreService;
use App\Services\Price\Xbox\XboxStoreService;
use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;

class PersistAllProviderMediaCommand extends Command
{
    use Concerns\InteractsWithEnrichmentProviders;

    protected $signature = 'games:persist-all-media 
        {--discover-via=rawg : Provider to discover new games (rawg, tgdb, giantbomb)}
        {--enrich-providers=steam,xbox,playstation,amazon,gog,epic,ubisoft,ea,giantbomb,tgdb,itchio : Comma-separated providers to enrich with}
        {--year=2024 : Year to discover games from}
        {--limit=10 : Games to discover}
        {--enrich-existing : Skip discovery, only enrich existing games}
        {--game-id= : Enrich specific game ID only}
        {--worldwide : Fetch prices for all countries}
        {--only-priced : Only enrich games that already have prices}
        {--game-id= : Enrich specific game ID only}
        {--worldwide : Fetch prices for all countries}
        {--only-priced : Only enrich games that already have prices}
        {--only-priced : Only enrich games that already have prices}
        {--search= : Search query for discovery (instead of recent)}
        {--queue : Dispatch jobs to queue instead of running synchronously}
        {--workers=1 : Number of parallel workers (IGDB-style)}
        {--chunk= : Internal chunk spec (N/Total) for worker}
        {--dry-run : Preview without saving}';

    protected $description = 'Discover games and enrich with media from all providers (Steam, Xbox, PS, Amazon, GB, TGDB, RAWG, Itch.io)';

    public function handle(
        RawgService $rawg,
        SteamStoreService $steam,
        XboxStoreService $xbox,
        PlayStationStoreService $playstation,
        AmazonScraperService $amazon,
        GameDataAggregatorService $aggregator,
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
        $onlyPriced = $this->option('only-priced');
        $gameId = $this->option('game-id');
        $worldwide = $this->option('worldwide');
        $worldwide = $this->option('worldwide');
        $worldwide = $this->option('worldwide');
        $search = $this->option('search');
        $queue = $this->option('queue');
        $workers = (int) $this->option('workers');
        $chunk = $this->option('chunk');
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

        // Step 1: Target Selection
        $gameIds = [];

        if ($gameId) {
            $gameIds = [$gameId];
            $this->info("🎯 Enriching specific game ID: {$gameId}");
        } elseif (! $enrichExisting && ! $onlyPriced) {
            $this->info("📥 Step 1: Discovering games via {$discoverVia}...");

            if ($discoverVia === 'rawg') {
                $discovered = $this->discoverViaRAWG($rawg, $year, $limit, $dryRun, $search);
            } elseif ($discoverVia === 'tgdb') {
                $discovered = $this->discoverViaTGDB($tgdb, $limit, $dryRun, $search);
            } elseif ($discoverVia === 'giantbomb') {
                $discovered = $this->discoverViaGiantBomb($giantBomb, $limit, $dryRun, $search);
            } else {
                $this->error("Unknown discovery provider: {$discoverVia}");

                return self::FAILURE;
            }

            $stats['games_discovered'] = count($discovered);
            $gameIds = $discovered;

            $this->info("   Found {$stats['games_discovered']} games");
            $this->newLine();
        } else {
            // Get existing games to enrich
            $this->info('📚 Step 1: Identifying existing games to enrich...');

            $query = DB::table('video_games');

            if ($onlyPriced) {
                $this->info('   ⚡ Mode: High-Speed Price Backfill (subsetting targets with price entries)');
                $query->join('video_game_prices', 'video_games.id', '=', 'video_game_prices.video_game_id')
                    ->distinct();
            }

            $gameIds = $query->orderBy('video_games.created_at', 'desc')
                ->limit($limit)
                ->pluck('video_games.id')
                ->toArray();

            $this->info('   Found '.count($gameIds).' games matching criteria');
            $this->newLine();
        }

        // Step 2: Enrich games with media from all providers
        if (! empty($gameIds) && ! empty($enrichProviders)) {
            $this->info('🎨 Step 2: Enriching games with media + prices from providers...');
            $this->line('   Providers: '.implode(', ', $enrichProviders));
            $this->newLine();

            if ($queue && ! $dryRun) {
                // Queue Mode
                $this->info('   🚀 Dispatching jobs to queue...');
                $bar = $this->output->createProgressBar(count($gameIds));

                // Process in chunks to prevent memory issues during dispatch
                $chunkSize = 500;
                $chunks = array_chunk($gameIds, $chunkSize);

                foreach ($chunks as $chunk) {
                    foreach ($chunk as $id) {
                        \App\Jobs\EnrichGameJob::dispatch($id, $enrichProviders, $worldwide);
                        $bar->advance();
                    }
                }

                $bar->finish();
                $this->newLine(2);
                $this->info('   ✅ Dispatched '.count($gameIds).' jobs to the queue.');

                return self::SUCCESS;
            }

            // Parallel Worker Mode
            if ($workers > 1 && ! $chunk) {
                return $this->runParallelWorkers($gameIds, $workers);
            }

            // If we are a worker (or single process), slice the gameIds based on chunk
            if ($chunk) {
                // chunk=1/4
                [$currentChunk, $totalChunks] = explode('/', $chunk);
                $currentChunk = (int) $currentChunk;
                $totalChunks = (int) $totalChunks;

                // Calculate slice
                $total = count($gameIds);
                $perChunk = ceil($total / $totalChunks);
                $offset = ($currentChunk - 1) * $perChunk;

                $gameIds = array_slice($gameIds, $offset, $perChunk);
                $this->info("   👷 Worker {$currentChunk}/{$totalChunks}: Processing ".count($gameIds)." games (Offset: {$offset})");
            }

            $progressBar = $this->output->createProgressBar(count($gameIds));
            $progressBar->start();

            foreach ($gameIds as $videoGameId) {
                try {
                    $game = DB::table('video_games')->where('id', $videoGameId)->first();

                    if (! $game) {
                        continue;
                    }

                    if (! $dryRun) {
                        // Pre-resolve IDs from attributes (IGDB websites) if missing
                        $attributes = match (true) {
                            is_array($game->attributes) => $game->attributes,
                            is_string($game->attributes) => json_decode($game->attributes, true) ?? [],
                            default => [],
                        };
                        $attributes = $this->resolveIdsFromExternalLinks($game, $attributes);

                        // Persist updated attributes if IDs were found
                        DB::table('video_games')->where('id', $game->id)->update([
                            'attributes' => json_encode($attributes),
                        ]);

                        foreach ($enrichProviders as $provider) {
                            $result = match ($provider) {
                                'steam' => $this->enrichWithSteam($steam, $aggregator, $game, $worldwide),
                                'xbox' => $this->enrichWithXbox($xbox, $aggregator, $game, $worldwide),
                                'playstation' => $this->enrichWithPlayStation($playstation, $aggregator, $game, $worldwide),
                                'amazon' => $this->enrichWithAmazon($aggregator, $game, $worldwide),
                                'gog' => $this->enrichWithGog($aggregator, $game, $worldwide),
                                'epic' => $this->enrichWithEpic($aggregator, $game, $worldwide),
                                'ubisoft' => $this->enrichWithUbisoft($aggregator, $game, $worldwide),
                                'ea' => $this->enrichWithEA($aggregator, $game, $worldwide),
                                'giantbomb' => $this->enrichWithGiantBomb($giantBomb, $aggregator, $game),
                                'tgdb' => $this->enrichWithTGDB($tgdb, $aggregator, $game),
                                'itchio' => $this->enrichWithItchIo($itchIo, $aggregator, $game, $worldwide),
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
    private function discoverViaRAWG(RawgService $rawg, string $year, int $limit, bool $dryRun, ?string $search = null): array
    {
        if ($search) {
            $response = $rawg->search($search, $limit);
            // rawg search returns simplified objects, handle carefully
            // Actually, RawgService::search returns results array directly.
            // But we need full persistence logic which expects 'results' array structure or individual items?
            // current logic iterates $response['results']. RawgService::search returns list.
            // Let's normalize.
            $games = $response;
        } else {
            $response = $rawg->getAllGames([
                'dates' => "{$year}-01-01,{$year}-12-31",
                'ordering' => '-rating',
            ], page: 1, pageSize: $limit);
            $games = $response['results'] ?? [];
        }

        $gameIds = [];

        foreach ($games as $game) {
            if ($dryRun) {
                $this->line("   [DRY RUN] Would discover: {$game['name']}");

                continue;
            }

            // If searched, we might need full details if simplified result doesn't have enough
            // RAWG search result has slug, so we can use persistRAWGGame which re-fetches or uses data?
            // persistRAWGGame uses the data passed. If it's partial, we might need to fetch full.
            // Checking persistRAWGGame: it uses developers, publishers, description_raw. Search result usually lacks description.
            // So we should fetch full details if searching.

            if ($search) {
                $fullData = $rawg->getGameDetails($game['slug']);
                if ($fullData) {
                    $game = $fullData;
                }
            }

            $result = $this->persistRAWGGame($game);
            $gameIds[] = $result['game_id'];
        }

        return $gameIds;
    }

    /**
     * Persist RAWG game.
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
            $developers = array_map(fn ($d) => $d['name'], $game['developers'] ?? []);
            $publishers = array_map(fn ($p) => $p['name'], $game['publishers'] ?? []);

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
                'genre' => json_encode(array_map(fn ($g) => $g['name'], $game['genres'] ?? [])),
                'developer' => ! empty($developers) ? json_encode($developers) : null,
                'publisher' => ! empty($publishers) ? json_encode($publishers) : null,
                'platform' => json_encode(array_map(fn ($p) => $p['platform']['name'] ?? 'Unknown', $game['platforms'] ?? [])),
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

        if (! empty($game['background_image'])) {
            DB::table('images')->updateOrInsert(
                ['imageable_type' => 'App\\Models\\VideoGame', 'imageable_id' => $gameId, 'url' => $game['background_image']],
                ['metadata' => json_encode(['source' => 'rawg', 'type' => 'hero']), 'updated_at' => now(), 'created_at' => now()]
            );
            $count++;
        }

        if (! empty($game['background_image_additional'])) {
            DB::table('images')->updateOrInsert(
                ['imageable_type' => 'App\\Models\\VideoGame', 'imageable_id' => $gameId, 'url' => $game['background_image_additional']],
                ['metadata' => json_encode(['source' => 'rawg', 'type' => 'background']), 'updated_at' => now(), 'created_at' => now()]
            );
            $count++;
        }

        return $count;
    }

    /**
     * Discovery via TGDB.
     */
    private function discoverViaTGDB(TGDBService $tgdb, int $limit, bool $dryRun, ?string $search = null): array
    {
        if ($search) {
            $games = $tgdb->search($search);
        } else {
            $games = $tgdb->getRecentlyUpdatedGames();
        }

        $games = array_slice($games, 0, $limit);
        $gameIds = [];

        foreach ($games as $game) {
            if ($dryRun) {
                $this->line("   [DRY RUN] Would discover via TGDB: {$game['game_title']} (ID: {$game['id']})");

                continue;
            }

            // Fetch full details to get genres/metadata
            $fullDetails = $tgdb->getFullDetails($game['id']);
            if (! $fullDetails) {
                continue;
            }

            $meta = $fullDetails['metadata'];
            $meta['external_id'] = (string) $game['id'];

            $gameId = $this->persistGenericGame($meta, 'tgdb');

            // Save media
            if ($gameId) {
                $this->enrichWithTGDB($tgdb, app(GameDataAggregatorService::class), (object) ['id' => $gameId, 'name' => $meta['game_title']]);
                $gameIds[] = $gameId;
            }
        }

        return $gameIds;
    }

    /**
     * Discovery via Giant Bomb.
     */
    private function discoverViaGiantBomb(GiantBombService $gb, int $limit, bool $dryRun, ?string $search = null): array
    {
        if ($search) {
            $games = $gb->search($search, $limit);
        } else {
            $games = $gb->getRecentlyAddedGames($limit);
        }

        $gameIds = [];

        foreach ($games as $game) {
            if ($dryRun) {
                $this->line("   [DRY RUN] Would discover via Giant Bomb: {$game['name']} (GUID: {$game['guid']})");

                continue;
            }

            $meta = [
                'game_title' => $game['name'],
                'external_id' => $game['guid'],
                'overview' => $game['deck'],
                'release_date' => $game['original_release_date'],
                'slug' => \Illuminate\Support\Str::slug($game['name']),
            ];

            $gameId = $this->persistGenericGame($meta, 'giantbomb');

            if ($gameId) {
                $gameIds[] = $gameId;
            }
        }

        return $gameIds;
    }

    /**
     * Generic persistence helper.
     */
    private function persistGenericGame(array $meta, string $provider): int
    {
        return DB::transaction(function () use ($meta, $provider) {
            $existing = DB::table('video_games')
                ->where('provider', $provider)
                ->where('external_id', $meta['external_id'])
                ->first();

            if ($existing) {
                return $existing->id;
            }

            return DB::table('video_games')->insertGetId([
                'name' => $meta['game_title'] ?? 'Unknown',
                'slug' => $meta['slug'] ?? \Illuminate\Support\Str::slug($meta['game_title'] ?? uniqid()),
                'provider' => $provider,
                'external_id' => $meta['external_id'],
                'description' => $meta['overview'] ?? null,
                'summary' => mb_substr($meta['overview'] ?? '', 0, 500),
                'release_date' => $meta['release_date'] ?? null,
                'created_at' => now(),
                'updated_at' => now(),
            ]);
        });
    }

    /**
     * Run parallel workers.
     */
    private function runParallelWorkers(array $gameIds, int $workers): int
    {
        $this->info("🚀 Starting {$workers} parallel workers...");
        $this->newLine();

        $processes = [];
        $pipes = []; // Fix: Declare pipes array
        $phpBinary = PHP_BINARY;
        $artisan = base_path('artisan');

        // Construct base command options
        $options = [
            '--enrich-existing' => true,
            '--limit' => count($gameIds), // Limit is total for this run
            '--worldwide' => $this->option('worldwide'),
            '--only-priced' => $this->option('only-priced'),
            // Don't pass workers or queue to children
        ];

        // Pass boolean flags
        $flags = [];
        foreach ($options as $key => $val) {
            if ($val === true) {
                $flags[] = $key;
            } elseif ($val !== null && $val !== false) {
                $flags[] = "{$key}={$val}";
            }
        }

        // Launch workers
        for ($i = 1; $i <= $workers; $i++) {
            $cmd = escapeshellarg($phpBinary)." {$artisan} games:persist-all-media ".implode(' ', $flags)." --chunk={$i}/{$workers}";

            // Allow discovering via specific provider if set
            if ($discoverVia = $this->option('discover-via')) {
                $cmd .= " --discover-via={$discoverVia}";
            }

            $descriptorSpec = [
                0 => ['pipe', 'r'],
                1 => ['pipe', 'w'],
                2 => ['pipe', 'w'],
            ];

            $process = proc_open($cmd, $descriptorSpec, $currentPipes, base_path());

            if (is_resource($process)) {
                $processes[$i] = ['process' => $process, 'pipes' => $currentPipes];
                $this->line("   Started worker {$i}...");

                // Non-blocking output
                stream_set_blocking($currentPipes[1], false);
                stream_set_blocking($currentPipes[2], false);
            }
        }

        // Monitor
        $active = count($processes);
        while ($active > 0) {
            foreach ($processes as $i => &$data) {
                if (! $data) {
                    continue;
                }

                $status = proc_get_status($data['process']);

                // Read output
                $stdout = stream_get_contents($data['pipes'][1]);
                if ($stdout) {
                    $this->output->write($stdout);
                } // Forward output

                $stderr = stream_get_contents($data['pipes'][2]);
                if ($stderr) {
                    $this->output->write($stderr);
                }

                if (! $status['running']) {
                    fclose($data['pipes'][0]);
                    fclose($data['pipes'][1]);
                    fclose($data['pipes'][2]);
                    proc_close($data['process']);
                    $data = null;
                    $active--;
                    $this->info("   ✓ Worker {$i} finished.");
                }
            }
            usleep(100000); // 100ms
        }

        $this->info('✅ All workers completed.');

        return self::SUCCESS;
    }
}
