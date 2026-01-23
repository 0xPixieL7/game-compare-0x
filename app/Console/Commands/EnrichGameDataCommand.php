<?php

namespace App\Console\Commands;

use Illuminate\Console\Command;

class EnrichGameDataCommand extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'games:enrich {--skip-retailers : Skip retailer extraction} {--skip-videos : Skip video extraction} {--skip-prices : Skip price scraping} {--limit=100 : Limit for price scraping}';

    /**
     * The console command description.
     *
     * @var string
     */
    protected $description = 'Run complete game data enrichment pipeline: extract retailers, videos, and scrape prices';

    /**
     * Execute the console command.
     */
    public function handle()
    {
        $this->info('=== Game Data Enrichment Pipeline ===');
        $this->newLine();

        $startTime = microtime(true);
        $results = [];

        // Step 1: Extract Retailer Links
        if (!$this->option('skip-retailers')) {
            $this->info('🔗 Step 1: Extracting Retailer Links from IGDB...');
            $exitCode = $this->call('prices:extract-retailers');
            $results['retailers'] = $exitCode === 0 ? 'Success' : 'Failed';
            $this->newLine();
        }

        // Step 2: Extract Videos/Trailers
        if (!$this->option('skip-videos')) {
            $this->info('🎬 Step 2: Extracting Videos from IGDB...');
            $exitCode = $this->call('media:extract-videos');
            $results['videos'] = $exitCode === 0 ? 'Success' : 'Failed';
            $this->newLine();
        }

        // Step 3: Scrape Prices (optional, can be skipped for faster enrichment)
        if (!$this->option('skip-prices')) {
            $limit = (int) $this->option('limit');
            $this->info("💰 Step 3: Scraping Prices (limit: {$limit})...");
            $this->warn('   Note: This step can take a while. Use --skip-prices to skip.');
            
            $exitCode = $this->call('prices:scrape', [
                '--limit' => $limit,
            ]);
            $results['prices'] = $exitCode === 0 ? "Success ({$limit} items)" : 'Failed';
            $this->newLine();
        }

        $duration = round(microtime(true) - $startTime, 2);

        $this->newLine();
        $this->info('=== Enrichment Complete ===');
        $this->table(
            ['Step', 'Status'],
            collect($results)->map(fn($status, $step) => [ucfirst($step), $status])->values()->toArray()
        );
        $this->info("Duration: {$duration}s");

        return self::SUCCESS;
    }
}
