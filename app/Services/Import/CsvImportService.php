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
use App\Services\Import\Concerns\InteractsWithConsole;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Cache;
use Illuminate\Support\Facades\File;
use Illuminate\Support\Facades\Log;
use League\Csv\Reader;
use League\Csv\Statement;
use Illuminate\Console\View\Components\Factory as OutputFactory;
use Symfony\Component\Console\Output\ConsoleOutput;
use Illuminate\Support\Str;

class CsvImportService
{
    use InteractsWithConsole;

    protected string $storagePath;
    protected $output;

    public function __construct()
    {
        $this->storagePath = storage_path('sqlite_exports');
        $this->output = new OutputFactory(new ConsoleOutput());
    }

    public function run()
    {
        $this->startOptimizedImport();

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

        $this->endOptimizedImport();
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
        
        // Checkpoint logic
        $checkpointKey = 'import_checkpoint_' . md5(basename($path));
        $checkpoint = Cache::get($checkpointKey, 0);
        
        $records = $csv->getRecords();

        if ($checkpoint > 0) {
            $this->output->info("Resuming " . basename($path) . " from record $checkpoint");
            $stmt = Statement::create()->offset($checkpoint);
            $records = $stmt->process($csv);
        }
        
        $i = $checkpoint;
        $batch = [];
        $BATCH_SIZE = 5000;

        foreach ($records as $record) {
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

            $batch[] = [
                'provider' => $provider,
                'external_id' => (string)$externalId,
                'title' => $record['name'] ?? $record['title'] ?? 'Unknown',
                'slug' => $record['slug'] ?? Str::slug($record['name'] ?? $record['title'] ?? 'unknown'),
                'genre' => $record['genre'] ?? null,
                'release_date' => (isset($record['release_date']) && $record['release_date'] !== '') ? $record['release_date'] : ((isset($record['original_release_date']) && $record['original_release_date'] !== '') ? $record['original_release_date'] : null),
                'description' => $record['deck'] ?? $record['description'] ?? $record['synopsis'] ?? null,
                'metadata' => $record['metadata'] ?? null,
                'created_at' => now(),
                'updated_at' => now(),
                'legacy_id_from_csv' => $record['id'] ?? null, // Temporary storage for mapping if needed
                'map_provider_key' => $mapProviderKey,
            ];

            if (count($batch) >= $BATCH_SIZE) {
                $this->flushGamesBatch($batch, $checkpointKey, $i);
                $batch = [];
                $this->output->info("Processed $i games...");
            }
        }

        if (!empty($batch)) {
            $this->flushGamesBatch($batch, $checkpointKey, $i);
        }

        Cache::forget($checkpointKey);
    }

    protected function flushGamesBatch(array $batch, string $checkpointKey, int $progress)
    {
        DB::transaction(function () use ($batch) {
            $rows = array_map(function($item) {
                $copy = $item;
                unset($copy['legacy_id_from_csv'], $copy['map_provider_key']);
                return $copy;
            }, $batch);

            // Use upsert to handle updates and inserts in one go
            DB::table('video_games')->upsert(
                $rows,
                ['provider', 'external_id'],
                ['title', 'slug', 'genre', 'release_date', 'description', 'metadata', 'updated_at']
            );

            // Now we need to update IdentityMap. 
            $keys = array_map(fn($b) => $b['provider'] . '|' . $b['external_id'], $batch);
            
            $results = DB::table('video_games')
                ->whereIn(DB::raw("provider || '|' || external_id"), $keys)
                ->get(['id', 'provider', 'external_id']);

            $idLookup = [];
            foreach ($results as $r) {
                $idLookup[$r->provider . '|' . $r->external_id] = $r->id;
            }

            $legacyMappings = [];
            $providerMappings = [];

            foreach ($batch as $item) {
                $id = $idLookup[$item['provider'] . '|' . $item['external_id']] ?? null;
                if ($id) {
                    if ($item['legacy_id_from_csv']) {
                        $legacyMappings[$item['legacy_id_from_csv']] = $id;
                    }
                    $providerMappings[$item['external_id']] = $id;
                }
            }
            
            if (!empty($legacyMappings)) {
                IdentityMap::putMany('game', $batch[0]['map_provider_key'], $legacyMappings);
            }
            IdentityMap::putMany('game', $batch[0]['provider'], $providerMappings);
        });

        Cache::put($checkpointKey, $progress);
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
            $this->output->info("Importing Titles from " . basename($file));
            $csv = $this->getReader($file);
            $batch = [];
            $BATCH_SIZE = 5000;
            $i = 0;

            foreach ($csv->getRecords() as $record) {
                $i++;
                $batch[] = [
                    'legacy_id' => $record['id'],
                    'slug' => $record['slug'],
                    'name' => $record['name'],
                    'normalized_title' => $record['normalized_title'],
                    'product_id' => $record['product_id'] ?: null,
                    'providers' => $record['providers'] ?? null,
                    'video_game_id' => $record['video_game_id'] ?? null, // From CSV
                    'created_at' => now(),
                    'updated_at' => now(),
                ];

                if (count($batch) >= $BATCH_SIZE) {
                    $this->flushTitlesBatch($batch);
                    $batch = [];
                }
            }
            if (!empty($batch)) $this->flushTitlesBatch($batch);
        }
    }

    protected function flushTitlesBatch(array $batch)
    {
        DB::transaction(function () use ($batch) {
            $rows = array_map(function($item) {
                $copy = $item;
                unset($copy['legacy_id'], $copy['video_game_id']);
                return $copy;
            }, $batch);

            DB::table('video_game_titles')->upsert(
                $rows,
                ['slug'],
                ['name', 'normalized_title', 'product_id', 'providers', 'updated_at']
            );

            $slugs = array_map(fn($b) => $b['slug'], $batch);
            $results = DB::table('video_game_titles')->whereIn('slug', $slugs)->get(['id', 'slug']);
            $idLookup = $results->pluck('id', 'slug')->all();

            $gameUpdates = [];
            foreach ($batch as $item) {
                $titleId = $idLookup[$item['slug']] ?? null;
                if ($titleId) {
                    IdentityMap::put('title', 'legacy', $item['legacy_id'], $titleId);
                    
                    if ($item['video_game_id']) {
                        $gameInternalId = IdentityMap::get('game', 'legacy_main', $item['video_game_id']);
                        if ($gameInternalId) {
                            $gameUpdates[$gameInternalId] = $titleId;
                        }
                    }
                }
            }

            // Bulk update video_games with title_id if we have any
            if (!empty($gameUpdates)) {
                $cases = [];
                foreach ($gameUpdates as $gameId => $titleId) {
                    $cases[] = "WHEN $gameId THEN $titleId";
                }
                $ids = implode(',', array_keys($gameUpdates));
                DB::statement("UPDATE video_games SET video_game_title_id = CASE id " . implode(' ', $cases) . " END WHERE id IN ($ids)");
            }
        });
    }

    // 4. Title Sources
    protected function importTitleSources()
    {
        $files = $this->findFiles('*_title_sources*.csv');
        foreach ($files as $file) {
            $this->output->info("Importing Title Sources from " . basename($file));
            $csv = $this->getReader($file);
            $batch = [];
            $BATCH_SIZE = 5000;

            foreach ($csv->getRecords() as $record) {
                $titleId = IdentityMap::get('title', 'legacy', $record['video_game_title_id']);
                $sourceId = IdentityMap::get('source', 'legacy', $record['video_game_source_id']);
                
                if ($titleId && $sourceId) {
                    $batch[] = [
                        'video_game_title_id' => $titleId,
                        'video_game_source_id' => $sourceId,
                        'provider_item_id' => $record['provider_item_id'],
                        'created_at' => now(),
                        'updated_at' => now(),
                    ];
                }

                if (count($batch) >= $BATCH_SIZE) {
                    DB::table('video_game_title_sources')->insertOrIgnore($batch);
                    $batch = [];
                }
            }
            if (!empty($batch)) DB::table('video_game_title_sources')->insertOrIgnore($batch);
        }
    }

    // 5. Products
    protected function importProducts()
    {
        $files = $this->findFiles('products.csv');
        foreach ($files as $file) {
            $this->output->info("Importing Products...");
            $csv = $this->getReader($file);
            $batch = [];
            $BATCH_SIZE = 5000;

            foreach ($csv->getRecords() as $row) {
                $batch[] = [
                    'legacy_id' => $row['id'],
                    'slug' => $row['slug'],
                    'name' => $row['name'],
                    'type' => $row['category'] ?? 'video_game',
                    'platform' => $row['platform'],
                    'release_date' => $row['release_date'] ?: null,
                    'external_ids' => $row['external_ids'],
                    'metadata' => $row['metadata'],
                    'created_at' => now(),
                    'updated_at' => now(),
                ];

                if (count($batch) >= $BATCH_SIZE) {
                    $this->flushProductsBatch($batch);
                    $batch = [];
                }
            }
            if (!empty($batch)) $this->flushProductsBatch($batch);
        }
    }

    protected function flushProductsBatch(array $batch)
    {
        DB::transaction(function () use ($batch) {
            $rows = array_map(function($item) {
                $copy = $item;
                unset($copy['legacy_id']);
                return $copy;
            }, $batch);

            DB::table('products')->upsert(
                $rows,
                ['slug'],
                ['name', 'type', 'platform', 'release_date', 'external_ids', 'metadata', 'updated_at']
            );

            $slugs = array_map(fn($b) => $b['slug'], $batch);
            $idLookup = DB::table('products')->whereIn('slug', $slugs)->pluck('id', 'slug')->all();

            foreach ($batch as $item) {
                $id = $idLookup[$item['slug']] ?? null;
                if ($id) {
                    IdentityMap::put('product', 'legacy', $item['legacy_id'], $id);
                }
            }
        });
    }

    // 6. Prices
    protected function importPrices()
    {
        // SKU Regions
        $files = $this->findFiles('sku_regions.csv');
        foreach ($files as $file) {
            $this->output->info("Importing Prices (SKU Regions)...");
            $csv = $this->getReader($file);
            $batch = [];
            $BATCH_SIZE = 5000;

            foreach ($csv->getRecords() as $row) {
                $productId = IdentityMap::get('product', 'legacy', $row['product_id']);
                if (!$productId) continue;

                $batch[] = [
                    'product_id' => $productId,
                    'currency' => $row['currency'],
                    'amount_minor' => $row['amount_minor'] ?? (floatval($row['amount'] ?? 0) * 100),
                    'retailer' => $row['retailer'],
                    'region_code' => $row['region_code'],
                    'sku' => $row['sku'],
                    'is_active' => filter_var($row['is_active'], FILTER_VALIDATE_BOOLEAN),
                    'created_at' => now(),
                    'updated_at' => now(),
                ];

                if (count($batch) >= $BATCH_SIZE) {
                    DB::table('video_game_prices')->insert($batch);
                    $batch = [];
                }
            }
            if (!empty($batch)) DB::table('video_game_prices')->insert($batch);
        }

        // Price Aggregates
        $files = $this->findFiles('price_series_aggregates.csv');
        foreach ($files as $file) {
            $this->output->info("Importing Historical Prices...");
            $csv = $this->getReader($file);
            $batch = [];

            foreach ($csv->getRecords() as $row) {
                 $productId = IdentityMap::get('product', 'legacy', $row['product_id']);
                 if (!$productId) continue;
                 
                 $batch[] = [
                     'product_id' => $productId,
                     'currency' => 'USD', 
                     'amount_minor' => (float)$row['avg_fiat'] * 100, 
                     'recorded_at' => $row['window_end'],
                     'metadata' => json_encode(['min' => $row['min_fiat'], 'max' => $row['max_fiat']]),
                     'created_at' => now(),
                     'updated_at' => now(),
                 ];

                 if (count($batch) >= 5000) {
                     DB::table('video_game_prices')->insert($batch);
                     $batch = [];
                 }
            }
            if (!empty($batch)) DB::table('video_game_prices')->insert($batch);
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
            $batch = [];

            foreach ($csv->getRecords() as $row) {
                $legacyGameId = $row['video_game_id'] ?? null;
                $gameId = $legacyGameId ? IdentityMap::get('game', 'legacy_' . $provider, $legacyGameId) : null;
                
                if ($gameId) {
                    $batch[] = [
                        'video_game_id' => $gameId,
                        'url' => $row['url'],
                        'name' => $row['name'],
                        'metadata' => $row['metadata'] ?? null,
                        'created_at' => now(),
                        'updated_at' => now(),
                    ];
                }

                if (count($batch) >= 5000) {
                    DB::table('videos')->insertOrIgnore($batch);
                    $batch = [];
                }
            }
            if (!empty($batch)) DB::table('videos')->insertOrIgnore($batch);
        }

        // Images
        $files = $this->findFiles('*_images.csv');
        foreach ($files as $file) {
            $provider = $this->determineProvider(basename($file));
            $this->output->info("Importing Images ($provider)...");
            $csv = $this->getReader($file); 
            $batch = [];

            foreach ($csv->getRecords() as $row) {
                 $legacyGameId = $row['video_game_id'] ?? null;
                 $gameId = IdentityMap::get('game', 'legacy_' . $provider, $legacyGameId);
                 
                 if ($gameId) {
                    $batch[] = [
                        'video_game_id' => $gameId,
                        'url' => $row['url'],
                        'type' => $row['type'] ?? 'screenshot',
                        'created_at' => now(),
                        'updated_at' => now(),
                    ];
                 }

                 if (count($batch) >= 5000) {
                      DB::table('images')->insertOrIgnore($batch); 
                      $batch = [];
                 }
            }
            if (!empty($batch)) DB::table('images')->insertOrIgnore($batch);
        }
    }

    protected function importUsers()
    {
        $files = $this->findFiles('users.csv');
        foreach ($files as $file) {
            $this->output->info("Importing Users...");
            $csv = $this->getReader($file);
            $batch = [];
            foreach ($csv->getRecords() as $row) {
                $batch[] = [
                    'name' => $row['name'],
                    'email' => $row['email'],
                    'password' => $row['password'], 
                    'created_at' => now(),
                    'updated_at' => now(),
                ];
            }
            if (!empty($batch)) DB::table('users')->insertOrIgnore($batch);
        }
    }

    protected function importCurrencies()
    {
        $files = $this->findFiles('local_currencies.csv');
        foreach ($files as $file) {
            $csv = $this->getReader($file);
            $batch = [];
            foreach ($csv->getRecords() as $row) {
                $batch[] = [
                    'code' => $row['code'],
                    'name' => $row['name'],
                    'symbol' => $row['symbol'] ?? '$',
                    'created_at' => now(),
                    'updated_at' => now(),
                ];
            }
            if (!empty($batch)) {
                DB::table('currencies')->upsert($batch, ['code'], ['name', 'symbol', 'updated_at']);
            }
        }
    }
}
