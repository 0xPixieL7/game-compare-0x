<?php

declare(strict_types=1);

namespace App\Console\Commands;

use App\Models\Retailer;
use App\Models\VideoGame;
use App\Models\VideoGamePrice;
use App\Models\VideoGameTitleSource;
use App\Services\Catalogue\PriceCrossReferencer;
use App\Services\Nexarda\NexardaClient;
use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Log;

class NexardaLiveFetchCommand extends Command
{
    protected $signature = 'nexarda:live-fetch
                            {--limit=100 : Number of games to process}
                            {--currency=USD : Currency code}
                            {--all : Process all mapped games}
                            {--search : Search Nexarda for games without mappings}
                            {--catalogue : Ingest from nexarda_product_catalogue.json for backfilling}';

    protected $description = 'Fetch live prices from Nexarda and backfill mappings';

    public function handle(PriceCrossReferencer $referencer): int
    {
        if ($this->option('catalogue')) {
            $this->info('Ingesting Nexarda Catalogue...');
            $count = $referencer->ingestNexardaCatalogue();
            $this->info("Complete. Ingested {$count} records.");

            return self::SUCCESS;
        }

        $this->info('Starting Nexarda Live Price Fetcher...');
        $currency = $this->option('currency');
        $limit = (int) $this->option('limit');

        if ($this->option('search')) {
            $this->info("Searching Nexarda for up to {$limit} unmapped titles...");
            $count = $referencer->backfillNexarda($limit, $currency);
            $this->info("Complete. Mapped and fetched {$count} games.");

            return self::SUCCESS;
        }

        $this->info("Syncing Nexarda prices for up to {$limit} games...");
        $count = $referencer->syncNexarda($limit, $this->option('all'), $currency);
        $this->info("Complete. Synced {$count} sources.");

        return self::SUCCESS;
    }

    private function handleCatalogueIngest(): int
    {
        $path = base_path('nexarda_product_catalogue.json');
        if (! file_exists($path)) {
            $this->error("Catalogue file not found at {$path}");

            return self::FAILURE;
        }

        $this->info('Ingesting Nexarda Catalogue for backfilling...');
        $data = json_decode(file_get_contents($path), true);
        $games = $data['games'] ?? [];

        if (empty($games)) {
            $this->warn('No games found in catalogue.');

            return self::SUCCESS;
        }

        $bar = $this->output->createProgressBar(count($games));
        $bar->start();

        $now = now();
        $provider = 'nexarda';

        // Performance caches
        $retailerCache = [];

        $chunks = array_chunk($games, 200);
        foreach ($chunks as $chunk) {
            DB::transaction(function () use ($chunk, $now, $provider, &$retailerCache) {
                foreach ($chunk as $row) {
                    $name = $row['name'];
                    $slug = \Illuminate\Support\Str::slug($name);
                    if ($slug === '') {
                        $slug = 'nexarda-'.$row['id'];
                    }

                    // 1. Product
                    $product = \App\Models\Product::firstOrCreate(
                        ['name' => $name],
                        ['slug' => $slug, 'type' => 'video_game']
                    );

                    // 2. Title
                    $title = \App\Models\VideoGameTitle::firstOrCreate(
                        ['product_id' => $product->id, 'slug' => $slug],
                        ['name' => $name]
                    );

                    // 3. Source
                    $source = VideoGameTitleSource::updateOrCreate(
                        [
                            'video_game_title_id' => $title->id,
                            'provider' => $provider,
                        ],
                        [
                            'provider_item_id' => (string) $row['id'],
                            'name' => $name,
                            'slug' => $row['slug'] ?? null,
                            'external_id' => (int) $row['id'],
                            'raw_payload' => json_encode($row),
                            'updated_at' => $now,
                        ]
                    );

                    // 4. Video Game (Actionable record)
                    $videoGame = VideoGame::updateOrCreate(
                        [
                            'video_game_title_id' => $title->id,
                            'provider' => $provider,
                            'external_id' => (int) $row['id'],
                        ],
                        [
                            'name' => $name,
                            'slug' => $slug,
                            'attributes' => json_encode([
                                'platform' => [], // JSON catalogue doesn't have explicit platforms per game in this root array usually
                                'nexarda_slug' => $row['slug'] ?? null,
                            ]),
                            'updated_at' => $now,
                        ]
                    );

                    // 5. Prices
                    $priceRows = [];
                    foreach ($row['prices'] as $cur => $val) {
                        if ($val === 'unavailable') {
                            continue;
                        }

                        $code = strtoupper($cur);
                        $retailerName = "Nexarda {$code}";
                        $retailerSlug = 'nexarda_'.strtolower($code);

                        if (! isset($retailerCache[$retailerSlug])) {
                            $retailerCache[$retailerSlug] = Retailer::firstOrCreate(
                                ['slug' => $retailerSlug],
                                ['name' => $retailerName.' (Catalogue)']
                            );
                        }

                        $priceRows[] = [
                            'video_game_id' => $videoGame->id,
                            'currency' => $code,
                            'country_code' => $this->getCountryForCurrency($code),
                            'amount_minor' => (int) round(((float) $val) * 100),
                            'retailer' => $retailerCache[$retailerSlug]->name,
                            'recorded_at' => $now,
                            'is_active' => true,
                            'metadata' => json_encode([
                                'src' => 'nexarda_catalogue',
                                'discount_percent' => $row['discounts'][$cur] ?? 0,
                            ]),
                            'updated_at' => $now,
                        ];
                    }

                    if (! empty($priceRows)) {
                        VideoGamePrice::upsert(
                            $priceRows,
                            ['video_game_id', 'retailer', 'country_code'],
                            ['currency', 'amount_minor', 'recorded_at', 'is_active', 'metadata', 'updated_at']
                        );
                    }
                }
            });
            $bar->advance(count($chunk));
        }

        $bar->finish();
        $this->newLine();
        $this->info('Catalogue ingestion complete.');

        return self::SUCCESS;
    }

    private function getCountryForCurrency(string $currency): string
    {
        return match ($currency) {
            'USD' => 'US',
            'EUR' => 'EU',
            'GBP' => 'GB',
            default => 'US',
        };
    }

    private function handleSearchBackfill(NexardaClient $client, string $currency): int
    {
        $limit = (int) $this->option('limit');

        // Find games without Nexarda mapping
        $titles = \App\Models\VideoGameTitle::whereDoesntHave('sources', function ($q) {
            $q->where('provider', 'nexarda');
        })
            ->limit($limit)
            ->get();

        if ($titles->isEmpty()) {
            $this->info('No titles found needing Nexarda mapping.');

            return self::SUCCESS;
        }

        $this->info("Searching Nexarda for {$titles->count()} titles...");
        $bar = $this->output->createProgressBar($titles->count());
        $bar->start();

        foreach ($titles as $title) {
            try {
                $this->searchAndMap($client, $title, $currency);
            } catch (\Exception $e) {
                Log::error("Nexarda Search Error for Title ID {$title->id}: ".$e->getMessage());
            }
            $bar->advance();
            usleep(500000);
        }

        $bar->finish();
        $this->newLine();

        return self::SUCCESS;
    }

    private function searchAndMap(NexardaClient $client, \App\Models\VideoGameTitle $title, string $currency): void
    {
        $results = $client->search($title->name);

        if (empty($results['results'])) {
            return;
        }

        // Take the first result (closest match)
        $match = $results['results'][0];

        // Create source
        $source = VideoGameTitleSource::updateOrCreate(
            [
                'video_game_title_id' => $title->id,
                'provider' => 'nexarda',
            ],
            [
                'provider_item_id' => (string) $match['id'],
                'name' => $match['name'],
                'slug' => $match['slug'] ?? null,
                'external_id' => (int) $match['id'],
                'updated_at' => now(),
            ]
        );

        $this->processSource($client, $source, $currency);
    }

    private function processSource(NexardaClient $client, VideoGameTitleSource $source, string $currency): void
    {
        $data = $client->getPrices($source->provider_item_id, $currency);

        if (empty($data['prices']['list'])) {
            return;
        }

        // Update raw payload
        $source->update([
            'raw_payload' => json_encode($data),
            'updated_at' => now(),
        ]);

        // Find associated video game
        $videoGame = VideoGame::where('video_game_title_id', $source->video_game_title_id)
            ->where('provider', 'nexarda')
            ->first();

        if (! $videoGame) {
            // If not found, try to find any video game linked to this title
            $videoGame = VideoGame::where('video_game_title_id', $source->video_game_title_id)->first();
        }

        if (! $videoGame) {
            return;
        }

        $now = now();
        $priceRows = [];

        foreach ($data['prices']['list'] as $offer) {
            $storeName = $offer['store']['name'] ?? 'Unknown Store';
            $retailerSlug = 'nexarda_'.\Illuminate\Support\Str::slug($storeName);

            $retailer = Retailer::firstOrCreate(
                ['slug' => $retailerSlug],
                ['name' => $storeName.' (via Nexarda)']
            );

            $amount = (int) round(($offer['price'] ?? 0) * 100);

            if ($amount <= 0) {
                continue;
            }

            $priceRows[] = [
                'video_game_id' => $videoGame->id,
                'currency' => strtoupper($currency),
                'country_code' => ['US', 'FR', 'GB', 'GER'], // Default to US for now
                'amount_minor' => $amount,
                'retailer' => $retailer->name,
                'url' => $offer['url'] ?? $source->provider_url,
                'recorded_at' => $now,
                'is_active' => true,
                'metadata' => json_encode([
                    'src' => 'nexarda_live',
                    'store' => $storeName,
                    'is_sale' => ($offer['price'] < ($data['prices']['highest'] ?? 0)),
                ]),
                'updated_at' => $now,
            ];
        }

        if (! empty($priceRows)) {
            VideoGamePrice::upsert(
                $priceRows,
                ['video_game_id', 'retailer', 'country_code'],
                ['currency', 'amount_minor', 'recorded_at', 'is_active', 'metadata', 'updated_at']
            );
        }
    }
}
