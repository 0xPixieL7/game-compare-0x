<?php

namespace App\Console\Commands;

use App\Models\VideoGame;
use App\Services\Price\GameDataAggregatorService;
use Illuminate\Console\Command;

class GetAllPrices extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'prices:get-all {game_id : Video Game ID} {--force : Force refresh even if recently updated}';

    /**
     * The console command description.
     *
     * @var string
     */
    protected $description = 'Fetch all prices for a game from all retailers using APIs and scrapers';

    /**
     * Execute the console command.
     */
    public function handle(GameDataAggregatorService $aggregator)
    {
        $gameId = (int) $this->argument('game_id');
        $forceRefresh = $this->option('force');

        $this->info("Fetching all prices for game ID: {$gameId}");
        if ($forceRefresh) {
            $this->warn('Force refresh enabled - fetching all prices regardless of update status');
        }

        $game = VideoGame::find($gameId);
        if (! $game) {
            $this->error('Game not found.');

            return 1;
        }

        // 1. Sync Scraped Providers (Discovery & Update)
        $this->info('Syncing scraped providers...');

        // Defined output callback
        $onProgress = function ($message) {
            $this->output->write("  - $message\r");
        };

        $this->info('Starting Amazon sync...');
        $amazon = $aggregator->syncAmazonPrice($game, function ($msg) {
            $this->line("   $msg");
        });
        if (($amazon['prices'] ?? 0) > 0) {
            $this->info('✓ Amazon synced');
        }

        $this->info('Starting GOG sync...');
        $gog = $aggregator->syncGogPrice($game, function ($msg) {
            $this->line("   $msg");
        });
        if (($gog['prices'] ?? 0) > 0) {
            $this->info('✓ GOG synced');
        }

        $this->info('Starting Epic Games sync...');
        $epic = $aggregator->syncEpicPrice($game, function ($msg) {
            $this->line("   $msg");
        });
        if (($epic['prices'] ?? 0) > 0) {
            $this->info('✓ Epic Games synced');
        }

        $this->info('Starting Steam sync...');
        $steam = $aggregator->syncSteamPrice($game, function ($msg) {
            $this->line("   $msg");
        });
        if (($steam['prices'] ?? 0) > 0) {
            $this->info('✓ Steam synced');
        }

        $this->info('Starting Xbox sync...');
        $xbox = $aggregator->syncXboxPrice($game, function ($msg) {
            $this->line("   $msg");
        });
        if (($xbox['prices'] ?? 0) > 0) {
            $this->info('✓ Xbox synced');
        }

        $this->info('Starting PlayStation sync...');
        $ps = $aggregator->syncPlayStationPrice($game, function ($msg) {
            $this->line("   $msg");
        });
        if (($ps['prices'] ?? 0) > 0) {
            $this->info('✓ PlayStation synced');
        }

        // 2. Fetch All Data (API Providers + Aggregation from DB)
        $this->info('Fetching aggregated data (Prices + Media)...');
        $results = $aggregator->getAllData($gameId, $forceRefresh, true);

        if (isset($results['error'])) {
            $this->error($results['error']);

            return 1;
        }

        $this->newLine();
        $this->info("Game: {$results['game_name']}");
        $this->info("Fetched at: {$results['fetched_at']}");
        $this->newLine();

        // Display prices in a table
        if (! empty($results['prices'])) {
            $this->info('✓ Successfully fetched '.count($results['prices']).' prices:');
            $this->table(
                ['Retailer', 'Country', 'Price', 'Currency', 'Amount (minor)', 'Media'],
                collect($results['prices'])->map(function ($p) use ($results) {
                    // Check if this retailer found any media
                    $retailer = $p['retailer'];
                    $mediaCount = 0;
                    if (isset($results['media'][$retailer])) {
                        $m = $results['media'][$retailer];
                        $mediaCount += count($m['screenshots'] ?? []);
                        $mediaCount += count($m['movies'] ?? []);
                        if (isset($m['header_image'])) {
                            $mediaCount++;
                        }
                        if (isset($m['background'])) {
                            $mediaCount++;
                        }
                    }

                    return [
                        $p['retailer'],
                        $p['country'],
                        $p['amount_formatted'],
                        $p['currency'],
                        $p['amount_minor'],
                        $mediaCount > 0 ? "✓ ({$mediaCount})" : '-',
                    ];
                })->toArray()
            );
        } else {
            $this->warn('No prices fetched.');
        }

        // Display errors
        if (! empty($results['errors'])) {
            $this->newLine();
            $this->error('✗ '.count($results['errors']).' errors occurred:');
            $this->table(
                ['Retailer', 'Country', 'Error'],
                collect($results['errors'])->map(fn ($e) => [
                    $e['retailer'],
                    $e['country'],
                    $e['message'],
                ])->toArray()
            );
        }

        $this->newLine();
        $this->info('Done!');

        return 0;
    }
}
