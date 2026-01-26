<?php

declare(strict_types=1);

namespace App\Console\Commands;

use App\Models\VideoGame;
use App\Services\Price\GameDataAggregatorService;
use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Log;

class EnrichAllGamesCommand extends Command
{
    use \App\Console\Commands\Enrich\Concerns\InteractsWithEnrichmentProviders;

    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'games:enrich-all
                            {--chunk=100 : Number of games to process per batch}
                            {--skip-prices : Skip price fetching (faster)}
                            {--skip-media : Skip media enrichment (faster)}
                            {--only-missing : Only enrich games with missing data}
                            {--min-rating=70 : Minimum rating to process}
                            {--resume : Resume from last position}
                            {--reset : Reset cursor and start fresh}
                            {--queue : Dispatch to queue for parallel processing (FAST)}';

    /**
     * The console command description.
     *
     * @var string
     */
    protected $description = 'Enrich all games in database with optimized batching and resume support';

    /**
     * Execute the console command.
     */
    public function handle(
        GameDataAggregatorService $aggregator,
        \App\Services\Price\Steam\SteamStoreService $steam,
        \App\Services\Price\Xbox\XboxStoreService $xbox,
        \App\Services\Price\PlayStation\PlayStationStoreService $playstation
    ): int {
        $commandName = 'games:enrich-all';
        $startTime = microtime(true);

        // Handle reset
        if ($this->option('reset')) {
            DB::table('command_cursors')->where('command_name', $commandName)->delete();
            $this->info('✓ Cursor reset. Starting fresh.');
        }

        $this->info('🚀 Starting optimized full database enrichment...');
        $this->newLine();

        // Get options
        $chunkSize = (int) $this->option('chunk');
        $skipPrices = $this->option('skip-prices');
        $skipMedia = $this->option('skip-media');
        $onlyMissing = $this->option('only-missing');
        $minRating = (int) $this->option('min-rating');
        $useQueue = $this->option('queue');

        // Build query
        $query = VideoGame::query()
            ->select(['id', 'name', 'video_game_title_id', 'attributes', 'rating'])
            ->orderBy('id');

        // Filter by rating
        if ($minRating > 0) {
            $query->where('rating', '>=', $minRating);
        }

        // Filter only missing data
        if ($onlyMissing) {
            $query->where(function ($q) {
                $q->whereNull('attributes->steam_id')
                    ->orWhereNull('attributes->xbox_id')
                    ->orWhereNull('attributes->playstation_id');
            });
        }

        $totalGames = $query->count();
        $this->info("📊 Total games to process: {$totalGames}");
        $this->info("⚙️  Chunk size: {$chunkSize}");
        $this->info('⏩ Skip prices: '.($skipPrices ? 'YES' : 'NO'));
        $this->info('⏩ Skip media: '.($skipMedia ? 'YES' : 'NO'));
        $this->info("🎯 Min rating: {$minRating}");
        $this->info('⚡ Queue mode: '.($useQueue ? 'YES (PARALLEL)' : 'NO (SEQUENTIAL)'));
        $this->newLine();

        // Get or create cursor
        $cursor = DB::table('command_cursors')->where('command_name', $commandName)->first();
        $startPosition = 0;

        // If queue mode, dispatch jobs and return
        if ($useQueue) {
            // Apply resume filter if needed
            if ($this->option('resume') && $cursor) {
                $startPosition = $cursor->current_position;
                $query->where('id', '>', $startPosition);
                $this->info("↻ Resuming dispatch from Game ID > {$startPosition}");
            }

            return $this->dispatchQueuedJobs($query, $chunkSize, $skipPrices, $skipMedia, $totalGames, $commandName);
        }

        if ($this->option('resume') && $cursor) {
            $startPosition = $cursor->current_position;
            $this->info("↻ Resuming from position {$startPosition} of {$totalGames}");
        } else {
            DB::table('command_cursors')->updateOrInsert(
                ['command_name' => $commandName],
                [
                    'current_position' => 0,
                    'total_items' => $totalGames,
                    'started_at' => now(),
                    'completed_at' => null,
                    'metadata' => json_encode([
                        'chunk_size' => $chunkSize,
                        'skip_prices' => $skipPrices,
                        'skip_media' => $skipMedia,
                        'min_rating' => $minRating,
                    ]),
                    'updated_at' => now(),
                ]
            );
        }

        // Progress bar
        $bar = $this->output->createProgressBar($totalGames);
        $bar->setFormat(' %current%/%max% [%bar%] %percent:3s%% | %elapsed:6s% | %memory:6s% | ETA: %estimated:-6s%');
        $bar->setProgress($startPosition);
        $bar->start();

        $processed = 0;
        $errors = 0;
        $skipped = 0;

        // Process in chunks using chunkById for speed (O(1) pagination)
        // Note: chunkById uses 'id > last_id' which is much faster than 'offset'
        $query->where('id', '>', $startPosition)->chunkById($chunkSize, function ($games) use (
            &$processed,
            &$errors,
            &$skipped,
            $bar,
            $commandName,
            $skipPrices,
            $skipMedia,
            $aggregator,
            $steam,
            $xbox,
            $playstation
        ) {
            foreach ($games as $game) {
                try {
                    // Resolve external IDs from metadata
                    $attributes = match (true) {
                        is_array($game->attributes) => $game->attributes,
                        is_string($game->attributes) => json_decode($game->attributes, true) ?? [],
                        default => [],
                    };

                    $attributes = $this->resolveIdsFromExternalLinks($game, $attributes);

                    // Update attributes
                    $game->attributes = $attributes;
                    $game->save();

                    // Enrich with providers (skip worldwide sync for speed)
                    if (! $skipPrices) {
                        $this->enrichWithSteam($steam, $aggregator, $game, false);
                        $this->enrichWithXbox($xbox, $aggregator, $game, false);
                        $this->enrichWithPlayStation($playstation, $aggregator, $game, false);
                    }

                    // Media enrichment
                    if (! $skipMedia) {
                        // Dispatch media enrichment job instead of blocking
                        \App\Jobs\EnrichVideoGameMediaJob::dispatch($game);
                    }

                    $processed++;
                } catch (\Exception $e) {
                    Log::error("Failed to enrich game {$game->id}: ".$e->getMessage());
                    $errors++;
                }

                // Update cursor every 10 games for performance
                // Store the LAST PROCESSED ID as the current position
                if ($processed % 10 === 0) {
                    DB::table('command_cursors')
                        ->where('command_name', $commandName)
                        ->update([
                            'current_position' => $game->id, // Store ID, not count
                            'updated_at' => now(),
                        ]);
                }

                $bar->advance();
            }

            // Force cursor update after each chunk
            // Use the last game's ID from the chunk
            if ($games->isNotEmpty()) {
                DB::table('command_cursors')
                    ->where('command_name', $commandName)
                    ->update([
                        'current_position' => $games->last()->id,
                        'updated_at' => now(),
                    ]);
            }

            // Clear memory to prevent leaks
            if (function_exists('gc_collect_cycles')) {
                gc_collect_cycles();
            }
        });

        $bar->finish();
        $this->newLine(2);

        // Mark as completed
        DB::table('command_cursors')
            ->where('command_name', $commandName)
            ->update([
                'current_position' => $totalGames,
                'completed_at' => now(),
                'updated_at' => now(),
            ]);

        $duration = round(microtime(true) - $startTime, 2);
        $perSecond = $processed > 0 ? round($processed / $duration, 2) : 0;

        $this->info("✓ Completed enrichment in {$duration}s");
        $this->info("  Processed: {$processed} games");
        $this->info("  Errors: {$errors}");
        $this->info("  Speed: {$perSecond} games/second");

        return 0;
    }

    /**
     * Dispatch jobs to queue for parallel processing
     */
    private function dispatchQueuedJobs($query, int $chunkSize, bool $skipPrices, bool $skipMedia, int $totalGames, string $commandName): int
    {
        $this->info('🚀 Dispatching jobs to queue for parallel processing...');
        $this->newLine();

        $jobsDispatched = 0;
        $bar = $this->output->createProgressBar($totalGames);
        $bar->setFormat(' %current%/%max% [%bar%] %percent:3s%% | Jobs dispatched');
        $bar->start();

        // Use chunkById to dispatch jobs without loading all IDs into memory
        // This is extremely memory efficient for millions of records
        $query->chunkById($chunkSize, function ($games) use (&$jobsDispatched, $bar, $skipPrices, $skipMedia, $commandName) {
            $ids = $games->pluck('id')->toArray();
            $lastId = $games->last()->id;

            \App\Jobs\EnrichGameBatchJob::dispatch($ids, $skipPrices, $skipMedia);

            // Update cursor after dispatching batch
            // This allows resuming DISPATCH if the command is interrupted
            DB::table('command_cursors')->updateOrInsert(
                ['command_name' => $commandName],
                [
                    'current_position' => $lastId,
                    'updated_at' => now(),
                ]
            );

            $jobsDispatched++;
            $bar->advance(count($ids));
        });

        $bar->finish();
        $this->newLine(2);

        $this->info("✓ Dispatched {$jobsDispatched} batch jobs to queue");
        $this->info("  Total games: {$totalGames}");
        $this->info("  Batch size: {$chunkSize}");
        $this->newLine();
        $this->info('💡 Run queue workers to process:');
        $this->info('   php artisan queue:work --tries=2 --timeout=300');
        $this->newLine();
        $this->info('💡 Monitor queue:');
        $this->info('   php artisan queue:listen');

        return 0;
    }
}
