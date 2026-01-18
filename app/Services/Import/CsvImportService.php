<?php

namespace App\Services\Import;

use App\Models\Currency;
use App\Models\Image;
use App\Models\User;
use App\Models\Video;
use App\Models\VideoGame;
use App\Models\VideoGamePrice;
use App\Models\VideoGameSource;
use App\Models\VideoGameTitle;
use App\Models\VideoGameTitleSource;
use App\Models\Product; 
use App\Models\SkuRegion;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\File;
use Illuminate\Support\Facades\Log;
use League\Csv\Reader;
use Illuminate\Console\View\Components\Factory as OutputFactory;
use Symfony\Component\Console\Output\ConsoleOutput;
use Illuminate\Support\Str;

class CsvImportService
{
    protected string $storagePath;
    protected $output;

    public function __construct()
    {
        $this->storagePath = storage_path('sqlite_exports');
        $this->output = new OutputFactory(new ConsoleOutput());
    }

    public function run()
    {
        DB::disableQueryLog();
        IdentityMap::clear();

        $this->output->info('Starting Synchronous CSV Import...');

        try {
            $this->importSources();
        } catch (\Exception $e) {
            $this->output->error("ImportSources Failed: " . $e->getMessage());
            Log::error("ImportSources Failed", ['error' => $e]);
        }
        
        try {
            $this->importGames();
        } catch (\Exception $e) {
            $this->output->error("ImportGames Failed: " . $e->getMessage());
            Log::error("ImportGames Failed", ['error' => $e]);
        }

        $this->importTitles();
        $this->importTitleSources();
        $this->importProducts();
        $this->importPrices(); 
        $this->importMedia();
        $this->importUsers();
        $this->importCurrencies();
        
        $this->output->success('CSV Import Completed.');
    }

    protected function findFiles(string $pattern): array
    {
        return File::glob("{$this->storagePath}/{$pattern}");
    }

    protected function getReader(string $path): Reader
    {
        $csv = Reader::createFromPath($path, 'r');
        $csv->setHeaderOffset(0);
        return $csv;
    }

    // 1. Sources
    protected function importSources()
    {
        $files = $this->findFiles('video_game_sources.csv'); 
        foreach ($files as $file) {
            $this->output->info("Importing Sources from " . basename($file));
            $records = $this->getReader($file)->getRecords();
            
            foreach ($records as $record) {
                // Determine provider - use provider_key or provider name
                $provider = $record['provider'] ?? $record['provider_key']; 
                // Fix for empty provider field if necessary
                if (empty($provider) && str_contains($record['provider_key'], 'igdb')) $provider = 'igdb';

                try {
                $source = VideoGameSource::updateOrCreate(
                    ['provider_key' => $record['provider_key']],
                    [
                        'provider' => $provider,
                        'display_name' => $record['display_name'],
                        'category' => $record['category'],
                        'slug' => $record['slug'],
                        'metadata' => $record['metadata'] ?? null,
                    ]
                );

                IdentityMap::put('source', 'legacy', $record['id'], $source->id);
                } catch (\Exception $e) {
                     Log::error("Failed to import source: " . json_encode($record) . " Error: " . $e->getMessage());
                     throw $e;
                }
            }
        }
    }

    // 2. Games
    protected function importGames()
    {
        // 2a. Import Main Legacy Table (video_games.csv)
        $mainFiles = $this->findFiles('video_games.csv');
        foreach ($mainFiles as $file) {
            $this->output->info("Importing Main Games from " . basename($file));
            $this->processGameFile($file, 'legacy_main');
        }

        // 2b. Import Provider Specifics (giant_bomb_games.csv etc)
        $files = $this->findFiles('*_games.csv');
        foreach ($files as $file) {
            if (str_contains(basename($file), 'video_games.csv')) continue; // Skip main
            
            $provider = $this->determineProvider(basename($file));
            $this->output->info("Importing Provider Games from " . basename($file) . " ($provider)");
            $this->processGameFile($file, 'legacy_' . $provider, $provider);
        }
    }

    protected function processGameFile($path, $mapProviderKey, $explicitProvider = null)
    {
        $csv = $this->getReader($path);
        
        DB::beginTransaction();
        $i = 0;

        foreach ($csv->getRecords() as $record) {
            $i++;
            
            $provider = $explicitProvider;
            $externalId = null;

            if ($explicitProvider) {
                $externalId = $record['guid'] ?? $record['external_id'] ?? $record['id'];
            } else {
                $meta = json_decode($record['external_ids'] ?? '{}', true);
                if (!empty($meta['igdb'])) {
                    $provider = 'igdb';
                    $externalId = $meta['igdb'];
                } elseif (!empty($meta['giantbomb'])) {
                    $provider = 'giantbomb';
                    $externalId = $meta['giantbomb'];
                } else {
                    $provider = 'legacy_main'; // Fallback
                    $externalId = $record['id'];
                }
            }
            
            if (!$provider || !$externalId) {
                Log::warning("Skipping game record due to missing identity: " . json_encode($record));
                continue;
            }

            try {
                DB::transaction(function () use ($record, $provider, $externalId, $mapProviderKey) {
                    $game = VideoGame::updateOrCreate(
                        ['provider' => $provider, 'external_id' => (string)$externalId],
                        [
                            'title' => $record['name'] ?? $record['title'],
                            'slug' => $record['slug'] ?? Str::slug($record['name'] ?? $record['title']),
                            'genre' => $record['genre'] ?? null,
                            'release_date' => (isset($record['release_date']) && $record['release_date'] !== '') ? $record['release_date'] : ((isset($record['original_release_date']) && $record['original_release_date'] !== '') ? $record['original_release_date'] : null),
                            'description' => $record['deck'] ?? $record['description'] ?? $record['synopsis'] ?? null,
                            'metadata' => $record['metadata'] ?? null,
                        ]
                    );

                    // Map Legacy ID
                    if (isset($record['id'])) {
                        IdentityMap::put('game', $mapProviderKey, $record['id'], $game->id);
                    }
                    
                    // Map Provider Identity
                    IdentityMap::put('game', $provider, $externalId, $game->id);
                });
            } catch (\Exception $e) {
                Log::error("Failed to import game: $provider:$externalId. Error: " . $e->getMessage());
                // Nested transaction rollback handled automatically by DB::transaction
            }

            if ($i % 500 === 0) {
                DB::commit();
                DB::beginTransaction();
                $this->output->info("Processed $i games...");
            }
        }
        DB::commit();
    }

    protected function determineProvider($filename)
    {
        if (str_contains($filename, 'giant_bomb')) return 'giantbomb';
        if (str_contains($filename, 'thegamesdb')) return 'thegamesdb'; 
        return 'unknown';
    }

    // 3. Titles
    protected function importTitles()
    {
        $files = $this->findFiles('video_game_titles.csv');
        foreach ($files as $file) {
            $this->output->info("Importing Titles...");
            $csv = $this->getReader($file);
            
            DB::beginTransaction();
            foreach ($csv->getRecords() as $record) {
                // Find legacy game using video_game_id from CSV
                $gameId = IdentityMap::get('game', 'legacy_main', $record['video_game_id']);
                
                $title = VideoGameTitle::updateOrCreate(
                    [
                        'slug' => $record['slug'],
                    ],
                    [
                        'name' => $record['name'],
                        'normalized_title' => $record['normalized_title'],
                        'product_id' => $record['product_id'] ?: null, // product_id is optional? Migration says foreignId..constrained.
                        'providers' => $record['providers'] ?? null,
                    ]
                );

                IdentityMap::put('title', 'legacy', $record['id'], $title->id);

                if ($gameId) {
                    VideoGame::where('id', $gameId)->update(['video_game_title_id' => $title->id]);
                }
            }
            DB::commit();
        }
    }

    // 4. Title Sources
    protected function importTitleSources()
    {
        $files = $this->findFiles('*_title_sources*.csv');
        foreach ($files as $file) {
            $this->output->info("Importing Title Sources from " . basename($file));
            $csv = $this->getReader($file);
            
            DB::beginTransaction();
            foreach ($csv->getRecords() as $record) {
                $titleId = IdentityMap::get('title', 'legacy', $record['video_game_title_id']);
                $sourceId = IdentityMap::get('source', 'legacy', $record['video_game_source_id']);
                
                if ($titleId && $sourceId) {
                    VideoGameTitleSource::firstOrCreate([
                        'video_game_title_id' => $titleId,
                        'video_game_source_id' => $sourceId,
                        'provider_item_id' => $record['provider_item_id'],
                    ]);
                }
            }
            DB::commit();
        }
    }

    // 5. Products
    protected function importProducts()
    {
        $files = $this->findFiles('products.csv');
        foreach ($files as $file) {
            $this->output->info("Importing Products...");
            $csv = $this->getReader($file);
            
            DB::beginTransaction();
            foreach ($csv->getRecords() as $row) {
                $product = Product::updateOrCreate(
                    ['slug' => $row['slug']],
                    [
                        'name' => $row['name'],
                        'type' => $row['category'] ?? 'video_game',
                        'platform' => $row['platform'],
                        'release_date' => $row['release_date'],
                        'external_ids' => $row['external_ids'],
                        'metadata' => $row['metadata'],
                    ]
                );
                
                IdentityMap::put('product', 'legacy', $row['id'], $product->id);
            }
            DB::commit();
        }
    }

    // 6. Prices
    protected function importPrices()
    {
        // SKU Regions
        $files = $this->findFiles('sku_regions.csv');
        foreach ($files as $file) {
            $this->output->info("Importing Prices (SKU Regions)...");
            $csv = $this->getReader($file);
            
            DB::beginTransaction();
            foreach ($csv->getRecords() as $row) {
                $productLegacyId = $row['product_id'];
                $productId = IdentityMap::get('product', 'legacy', $productLegacyId);
                
                if (!$productId) continue;

                VideoGamePrice::create([
                    'product_id' => $productId,
                    'currency' => $row['currency'],
                    'amount_minor' => $row['amount_minor'] ?? (floatval($row['amount'] ?? 0) * 100),
                    'retailer' => $row['retailer'],
                    'region_code' => $row['region_code'],
                    'sku' => $row['sku'],
                    'is_active' => $row['is_active'],
                ]);
            }
            DB::commit();
        }

        // Price Aggregates
        $files = $this->findFiles('price_series_aggregates.csv');
        foreach ($files as $file) {
            $this->output->info("Importing Historical Prices...");
            $csv = $this->getReader($file);
            DB::beginTransaction();
            foreach ($csv->getRecords() as $row) {
                 $productId = IdentityMap::get('product', 'legacy', $row['product_id']);
                 if (!$productId) continue;
                 
                 VideoGamePrice::create([
                     'product_id' => $productId,
                     'currency' => 'USD', 
                     'amount_minor' => (float)$row['avg_fiat'] * 100, 
                     'recorded_at' => $row['window_end'],
                     'metadata' => json_encode(['min' => $row['min_fiat'], 'max' => $row['max_fiat']]),
                 ]);
            }
            DB::commit();
        }
    }

    // 7. Media
    protected function importMedia()
    {
        // Videos
        $files = $this->findFiles('*_videos.csv');
        foreach ($files as $file) {
            $provider = $this->determineProvider(basename($file));
            $this->output->info("Importing Videos ($provider)...");
            $csv = $this->getReader($file);
            
            DB::beginTransaction();
            foreach ($csv->getRecords() as $row) {
                $legacyGameId = $row['video_game_id'] ?? null;
                $gameId = null;
                
                if ($legacyGameId) {
                    $gameId = IdentityMap::get('game', 'legacy_' . $provider, $legacyGameId);
                }
                
                if ($gameId) {
                    Video::firstOrCreate(
                        ['url' => $row['url']],
                        [
                            'video_game_id' => $gameId,
                            'name' => $row['name'],
                            'metadata' => $row['metadata'] ?? null,
                        ]
                    );
                }
            }
            DB::commit();
        }

        // Images
        $files = $this->findFiles('*_images.csv');
        foreach ($files as $file) {
            $provider = $this->determineProvider(basename($file));
            $this->output->info("Importing Images ($provider)...");
            $csv = $this->getReader($file); 
            
            DB::beginTransaction();
            $i=0;
            foreach ($csv->getRecords() as $row) {
                 $i++;
                 $legacyGameId = $row['video_game_id'] ?? null;
                 $gameId = IdentityMap::get('game', 'legacy_' . $provider, $legacyGameId);
                 
                 if ($gameId) {
                     Image::firstOrCreate(
                         ['url' => $row['url']],
                         [
                             'video_game_id' => $gameId,
                             'type' => $row['type'] ?? 'screenshot',
                         ]
                     );
                 }
                 if($i % 500 == 0) {
                      DB::commit(); 
                      DB::beginTransaction();
                 }
            }
            DB::commit();
        }
    }

    protected function importUsers()
    {
        $files = $this->findFiles('users.csv');
        foreach ($files as $file) {
            $this->output->info("Importing Users...");
            $csv = $this->getReader($file);
            foreach ($csv->getRecords() as $row) {
                User::firstOrCreate(
                    ['email' => $row['email']],
                    [
                        'name' => $row['name'],
                        'password' => $row['password'], 
                    ]
                );
            }
        }
    }

    protected function importCurrencies()
    {
        $files = $this->findFiles('local_currencies.csv');
        foreach ($files as $file) {
            $csv = $this->getReader($file);
            foreach ($csv->getRecords() as $row) {
                Currency::updateOrCreate(
                    ['code' => $row['code']],
                    ['name' => $row['name'], 'symbol' => $row['symbol'] ?? '$']
                );
            }
        }
    }
}
