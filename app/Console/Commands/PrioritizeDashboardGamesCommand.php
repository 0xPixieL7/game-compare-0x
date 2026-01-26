<?php

namespace App\Console\Commands;

use App\Services\Price\GameDataAggregatorService;
use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Log;

class PrioritizeDashboardGamesCommand extends Command
{
    use \App\Console\Commands\Enrich\Concerns\InteractsWithEnrichmentProviders;

    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'games:prioritize-dashboard {--limit= : Limit the number of games per section}';

    /**
     * The console command description.
     *
     * @var string
     */
    protected $description = 'Prioritize price updates for games currently featured on the dashboard';

    /**
     * Execute the console command.
     */
    public function handle(
        GameDataAggregatorService $aggregator,
        \App\Services\Price\Steam\SteamStoreService $steam,
        \App\Services\Price\Xbox\XboxStoreService $xbox,
        \App\Services\Price\PlayStation\PlayStationStoreService $playstation
    ) {
        $this->info('Starting priority price sync for Dashboard games...');
        $startTime = microtime(true);

        $limit = $this->option('limit') ? (int) $this->option('limit') : null;

        // 1. Collect all Game IDs from dashboard sections
        $gameIds = $this->collectDashboardGameIds($limit);

        $this->info('Found '.count($gameIds).' unique games on the dashboard.');

        // 2. Sync prices for each
        $bar = $this->output->createProgressBar(count($gameIds));
        $bar->start();

        $marqueeIds = [
            12026, 199224, 117836, 221441, 174739, 221545, 233062,
            220587, 235660, 260502, 92989, 142457, 220450, 274649, 257269,
            37470, 53320, 1014215,
        ];

        foreach ($gameIds as $gameId) {
            try {
                $game = \App\Models\VideoGame::find($gameId);
                if (! $game) {
                    continue;
                }

                $this->info("\n   Processing: {$game->name} (ID: {$gameId})");
                $isSpotlight = in_array($game->id, $marqueeIds);

                // 1. Resolve IDs from metadata (IGDB websites, etc.)
                $attributes = match (true) {
                    is_array($game->attributes) => $game->attributes,
                    is_string($game->attributes) => json_decode($game->attributes, true) ?? [],
                    default => [],
                };

                // Add existing websites from the table if missing
                $attributes = $this->resolveIdsFromExternalLinks($game, $attributes);

                // Persist to model and database
                $game->attributes = $attributes;
                $game->save();

                // 2. Use the enrichment trait to bridge gaps for all major providers
                // Only do full worldwide sync for Spotlight/Marquee games to avoid rate limits
                $syncWorldwide = $isSpotlight;

                $this->enrichWithSteam($steam, $aggregator, $game, $syncWorldwide);
                $this->enrichWithXbox($xbox, $aggregator, $game, $syncWorldwide);
                $this->enrichWithPlayStation($playstation, $aggregator, $game, $syncWorldwide);

                // 3. Standard global refresh for other retailers (Amazon, Epic, etc.)
                $aggregator->getAllData($game->id, $syncWorldwide, true);

            } catch (\Exception $e) {
                Log::error("Failed to sync dashboard game {$gameId}: ".$e->getMessage());
            }
            $bar->advance();
        }

        $bar->finish();
        $this->newLine();

        $duration = round(microtime(true) - $startTime, 2);
        $this->info("Completed priority sync in {$duration}s.");

        return 0;
    }

    private function collectDashboardGameIds(?int $limit): array
    {
        $ids = [];

        // 1. Spotlight Games (80+)
        $marqueeIds = [
            12026, 199224, 117836, 221441, 174739, 221545, 233062,
            220587, 235660, 260502, 92989, 142457, 220450, 274649, 257269,
        ];

        $spotlight = DB::table('video_games')
            ->join('video_games_ranked_mv', 'video_games.id', '=', 'video_games_ranked_mv.id')
            ->leftJoin('video_game_titles', 'video_games.video_game_title_id', '=', 'video_game_titles.id')
            ->leftJoin('video_game_title_sources', function ($join) {
                $join->on('video_game_title_sources.video_game_title_id', '=', 'video_game_titles.id')
                    ->where('video_game_title_sources.provider', '=', 'igdb');
            })
            ->whereIn('video_games.id', $marqueeIds)
            ->where('video_games_ranked_mv.rating', '>=', 80)
            ->whereNotNull('video_game_title_sources.raw_payload')
            ->limit($limit ?? 12)
            ->pluck('video_games.id')
            ->toArray();

        $ids = array_merge($ids, $spotlight);
        $this->info('Collected '.count($spotlight).' Spotlight games');

        // 2. Top Rated (85+, 20+ reviews)
        $topRated = DB::table('video_games')
            ->join('video_game_titles', 'video_games.video_game_title_id', '=', 'video_game_titles.id')
            ->leftJoin('video_game_title_sources', function ($join) {
                $join->on('video_game_title_sources.video_game_title_id', '=', 'video_game_titles.id')
                    ->where('video_game_title_sources.provider', '=', 'igdb');
            })
            ->whereNotNull('video_game_title_sources.rating')
            ->where('video_game_title_sources.rating', '>=', 85)
            ->where('video_game_title_sources.rating_count', '>=', 20)
            ->orderBy('video_game_title_sources.rating', 'desc')
            ->limit($limit ?? 25)
            ->pluck('video_games.id')
            ->toArray();

        $ids = array_merge($ids, $topRated);
        $this->info('Collected '.count($topRated).' Top Rated games');

        // 3. New Releases (75+)
        $newReleases = DB::table('video_games')
            ->join('video_game_titles', 'video_games.video_game_title_id', '=', 'video_game_titles.id')
            ->leftJoin('video_game_title_sources', function ($join) {
                $join->on('video_game_title_sources.video_game_title_id', '=', 'video_game_titles.id')
                    ->where('video_game_title_sources.provider', '=', 'igdb');
            })
            ->whereNotNull('video_games.release_date')
            ->whereNotNull('video_game_title_sources.rating')
            ->where('video_game_title_sources.rating', '>=', 75)
            ->where('video_game_title_sources.rating_count', '>=', 5)
            ->orderBy('video_games.release_date', 'desc')
            ->limit($limit ?? 20)
            ->pluck('video_games.id')
            ->toArray();

        $ids = array_merge($ids, $newReleases);
        $this->info('Collected '.count($newReleases).' New Releases');

        // 4. Genre Leaders (75+) - Sample a few key genres
        $genres = ['Action', 'RPG', 'Adventure', 'Shooter'];
        foreach ($genres as $genre) {
            $genreGames = DB::table('video_games')
                ->join('video_game_titles', 'video_games.video_game_title_id', '=', 'video_game_titles.id')
                ->leftJoin('video_game_title_sources', function ($join) {
                    $join->on('video_game_title_sources.video_game_title_id', '=', 'video_game_titles.id')
                        ->where('video_game_title_sources.provider', '=', 'igdb');
                })
                ->whereNotNull('video_game_title_sources.rating')
                ->where('video_game_title_sources.rating', '>=', 75)
                ->where('video_game_title_sources.rating_count', '>=', 10)
                ->whereNotNull('video_game_title_sources.genre')
                ->whereRaw('LOWER(video_game_title_sources.genre::text) LIKE LOWER(?)', ["%{$genre}%"])
                ->orderBy('video_game_title_sources.rating', 'desc')
                ->limit($limit ?? 10)
                ->pluck('video_games.id')
                ->toArray();

            $ids = array_merge($ids, $genreGames);
        }

        return array_unique($ids);
    }
}
