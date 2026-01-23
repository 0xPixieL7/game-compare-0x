<?php

namespace App\Console\Commands;

use App\Services\Price\PriceAggregatorService;
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
    public function handle(PriceAggregatorService $aggregator)
    {
        $gameId = (int) $this->argument('game_id');
        $forceRefresh = $this->option('force');

        $this->info("Fetching all prices for game ID: {$gameId}");
        if ($forceRefresh) {
            $this->warn("Force refresh enabled - fetching all prices regardless of update status");
        }

        $results = $aggregator->getAllPrices($gameId, $forceRefresh);

        if (isset($results['error'])) {
            $this->error($results['error']);
            return 1;
        }

        $this->newLine();
        $this->info("Game: {$results['game_name']}");
        $this->info("Fetched at: {$results['fetched_at']}");
        $this->newLine();

        // Display prices in a table
        if (!empty($results['prices'])) {
            $this->info("✓ Successfully fetched " . count($results['prices']) . " prices:");
            $this->table(
                ['Retailer', 'Country', 'Price', 'Currency', 'Amount (minor)'],
                collect($results['prices'])->map(fn($p) => [
                    $p['retailer'],
                    $p['country'],
                    $p['amount_formatted'],
                    $p['currency'],
                    $p['amount_minor'],
                ])->toArray()
            );
        } else {
            $this->warn("No prices fetched.");
        }

        // Display errors
        if (!empty($results['errors'])) {
            $this->newLine();
            $this->error("✗ " . count($results['errors']) . " errors occurred:");
            $this->table(
                ['Retailer', 'Country', 'Error'],
                collect($results['errors'])->map(fn($e) => [
                    $e['retailer'],
                    $e['country'],
                    $e['message'],
                ])->toArray()
            );
        }

        $this->newLine();
        $this->info("Done!");

        return 0;
    }
}
