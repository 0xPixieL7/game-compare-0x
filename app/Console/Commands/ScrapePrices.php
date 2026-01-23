<?php

namespace App\Console\Commands;

use App\Jobs\FetchGamePriceJob;
use App\Models\VideoGamePrice;
use Illuminate\Console\Command;

class ScrapePrices extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'prices:scrape {--retailer= : Filter by retailer name like Amazon, Steam} {--limit=100 : Max items to process}';

    /**
     * The console command description.
     *
     * @var string
     */
    protected $description = 'Dispatch jobs to scrape prices for active retailer links';

    /**
     * Execute the console command.
     */
    public function handle()
    {
        $this->info("Starting price scraping...");
        
        $query = VideoGamePrice::where('is_active', true)
            // Filter where price is unknown (-1) or old (older than 24h)
            ->where(function ($q) {
                $q->where('amount_minor', -1)
                  ->orWhere('updated_at', '<', now()->subHours(24));
            });

        if ($this->option('retailer')) {
            $query->where('retailer', $this->option('retailer'));
        }

        $limit = (int) $this->option('limit');
        $prices = $query->limit($limit)->get();

        $this->info("Found {$prices->count()} items to scrape.");
        $bar = $this->output->createProgressBar($prices->count());

        foreach ($prices as $price) {
            // Dispatch job for each item
            // We need retailer_id, but our new Extraction logic didn't set retailer_id, only 'retailer' string.
            // FetchGamePriceJob asks for retailerId. We might need to adjust the job or find the ID.
            // Let's assume Retailer model exists and we should look it up or create it.
            
            $retailerModel = \App\Models\Retailer::firstOrCreate(
                ['name' => $price->retailer],
                [
                    'slug' => \Illuminate\Support\Str::slug($price->retailer), 
                    'is_active' => true,
                    // Simple domain matcher backup, assuming the name maps closely or we fix it later
                    'domain_matcher' => \Illuminate\Support\Str::slug($price->retailer) . '.com'
                ]
            );

            FetchGamePriceJob::dispatch(
                $price->video_game_id,
                $retailerModel->id,
                $price->url,
                $price->country_code ?? 'US'
            );
            
            $bar->advance();
        }

        $bar->finish();
        $this->newLine();
        $this->info("Jobs dispatched.");
    }
}
