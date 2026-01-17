<?php

declare(strict_types=1);

namespace App\Console\Commands;

use App\Models\VideoGamePrice;
use App\Models\VideoGameTitle;
use Illuminate\Console\Command;
use Illuminate\Support\Collection;
use Illuminate\Support\Facades\Cache;
use League\Csv\Reader;

class CrossReferencePriceGuide extends Command
{
    protected $signature = 'game:cross-reference-price-guide
                           {--dry-run : Show matches without importing}
                           {--limit=100 : Limit number of CSV records to process}
                           {--cache-only : Only cache matches without adding prices}';

    protected $description = 'Cross-reference price-guide.csv games with database and add price data from price-charting.com';

    private Collection $gameNameCache;

    private array $matchedGames = [];

    private array $unmatchedGames = [];

    private int $pricesAdded = 0;

    public function handle(): int
    {
        $this->info('🎮 Starting cross-reference of price-guide.csv with database games...');

        // Load and cache database games for efficient matching
        $this->loadGameNameCache();

        // Parse CSV and process matches
        $csvPath = base_path('price-guide.csv');
        if (! file_exists($csvPath)) {
            $this->error('❌ price-guide.csv not found in project root');

            return self::FAILURE;
        }

        $this->processCsvFile($csvPath);

        // Display results
        $this->displayResults();

        return self::SUCCESS;
    }

    private function loadGameNameCache(): void
    {
        $this->info('📋 Loading game names from database...');

        // Get all game titles with their video games for efficient matching
        $this->gameNameCache = VideoGameTitle::with('videoGames')
            ->get()
            ->mapWithKeys(function ($title) {
                $variations = [
                    $title->name,
                    $title->normalized_title,
                    str_replace('-', ' ', $title->normalized_title),
                ];

                // Create lookup array for each variation
                $lookups = [];
                foreach ($variations as $variation) {
                    $normalized = $this->normalizeGameName($variation);
                    $lookups[$normalized] = [
                        'title_id' => $title->id,
                        'title' => $title,
                        'original_name' => $title->name,
                        'video_games' => $title->videoGames,
                    ];
                }

                return $lookups;
            })
            ->collapse();

        $this->info("✅ Loaded {$this->gameNameCache->count()} game name variations for matching");
    }

    private function processCsvFile(string $csvPath): void
    {
        $this->info('📊 Processing CSV file...');

        $csv = Reader::createFromPath($csvPath, 'r');
        $csv->setHeaderOffset(0);

        $records = iterator_to_array($csv->getRecords());
        $limit = (int) $this->option('limit');

        if ($limit > 0) {
            $records = array_slice($records, 0, $limit);
        }

        $progressBar = $this->output->createProgressBar(count($records));
        $progressBar->start();

        foreach ($records as $record) {
            $this->processCsvRecord($record);
            $progressBar->advance();
        }

        $progressBar->finish();
        $this->newLine(2);
    }

    private function processCsvRecord(array $record): void
    {
        $csvGameName = $record['product-name'] ?? '';
        $consoleName = $record['console-name'] ?? '';
        $loosePriceStr = $record['loose-price'] ?? '';
        $csvId = $record['id'] ?? '';

        if (empty($csvGameName) || empty($loosePriceStr)) {
            return;
        }

        // Parse price (remove $ and convert to cents)
        $loosePriceDollars = (float) str_replace(['$', ','], '', $loosePriceStr);
        $loosePriceCents = (int) round($loosePriceDollars * 100);

        $normalizedCsvName = $this->normalizeGameName($csvGameName);

        // Try to find a match in our cached database games
        $match = $this->gameNameCache->get($normalizedCsvName);

        if ($match) {
            $this->recordMatch($csvGameName, $consoleName, $match, $loosePriceCents, $csvId);
        } else {
            $this->recordUnmatched($csvGameName, $consoleName, $loosePriceCents);
        }
    }

    private function recordMatch(string $csvName, string $consoleName, array $match, int $priceCents, string $csvId): void
    {
        $this->matchedGames[] = [
            'csv_name' => $csvName,
            'db_name' => $match['original_name'],
            'console' => $consoleName,
            'title_id' => $match['title_id'],
            'video_games' => $match['video_games'],
            'price_cents' => $priceCents,
            'csv_id' => $csvId,
        ];

        // Add price data if not in cache-only or dry-run mode
        if (! $this->option('cache-only') && ! $this->option('dry-run')) {
            $this->addPriceData($match['video_games'], $priceCents, $consoleName);
        }
    }

    private function recordUnmatched(string $csvName, string $consoleName, int $priceCents): void
    {
        $this->unmatchedGames[] = [
            'csv_name' => $csvName,
            'console' => $consoleName,
            'price_cents' => $priceCents,
        ];
    }

    private function addPriceData(Collection $videoGames, int $priceCents, string $consoleName): void
    {
        foreach ($videoGames as $videoGame) {
            // Check if we already have a recent price from price-charting.com
            $existingPrice = VideoGamePrice::where('video_game_id', $videoGame->id)
                ->where('retailer', 'price-charting.com')
                ->where('recorded_at', '>', now()->subDays(30))
                ->first();

            if ($existingPrice) {
                continue; // Skip if we have recent price data
            }

            // Add new price record
            VideoGamePrice::create([
                'video_game_id' => $videoGame->id,
                'currency' => 'USD',
                'amount_minor' => $priceCents,
                'recorded_at' => now(),
                'retailer' => 'price-charting.com',
                'tax_inclusive' => false,
                'country_code' => 'US',
            ]);

            $this->pricesAdded++;
        }
    }

    private function normalizeGameName(string $name): string
    {
        return strtolower(trim(preg_replace('/[^a-zA-Z0-9\s]/', '', $name)));
    }

    private function displayResults(): void
    {
        $this->newLine();
        $this->info('📊 Cross-Reference Results:');
        $this->table(
            ['Metric', 'Count'],
            [
                ['Matched Games', count($this->matchedGames)],
                ['Unmatched Games', count($this->unmatchedGames)],
                ['Prices Added', $this->pricesAdded],
            ]
        );

        if ($this->option('dry-run')) {
            $this->warn('🔍 DRY RUN - No data was actually imported');
        }

        // Cache the matched game names for future reference
        $this->cacheMatchedNames();

        // Show some sample matches
        if (! empty($this->matchedGames)) {
            $this->info('✅ Sample Matched Games:');
            $sampleMatches = array_slice($this->matchedGames, 0, 10);
            $this->table(
                ['CSV Name', 'DB Name', 'Console', 'Price'],
                array_map(fn ($match) => [
                    $match['csv_name'],
                    $match['db_name'],
                    $match['console'],
                    '$'.number_format($match['price_cents'] / 100, 2),
                ], $sampleMatches)
            );
        }

        // Show some unmatched games for review
        if (! empty($this->unmatchedGames)) {
            $this->warn('❌ Sample Unmatched Games:');
            $sampleUnmatched = array_slice($this->unmatchedGames, 0, 10);
            $this->table(
                ['CSV Name', 'Console', 'Price'],
                array_map(fn ($unmatched) => [
                    $unmatched['csv_name'],
                    $unmatched['console'],
                    '$'.number_format($unmatched['price_cents'] / 100, 2),
                ], $sampleUnmatched)
            );
        }
    }

    private function cacheMatchedNames(): void
    {
        if (empty($this->matchedGames)) {
            return;
        }

        $cacheKey = 'price_guide_matched_names';
        $cacheData = [
            'matched_count' => count($this->matchedGames),
            'unmatched_count' => count($this->unmatchedGames),
            'last_updated' => now()->toISOString(),
            'matches' => array_map(fn ($match) => [
                'csv_name' => $match['csv_name'],
                'db_name' => $match['db_name'],
                'console' => $match['console'],
                'title_id' => $match['title_id'],
                'price_cents' => $match['price_cents'],
                'csv_id' => $match['csv_id'],
            ], $this->matchedGames),
        ];

        Cache::put($cacheKey, $cacheData, now()->addDays(30));
        $this->info('💾 Cached matched game names for future reference');
    }
}
