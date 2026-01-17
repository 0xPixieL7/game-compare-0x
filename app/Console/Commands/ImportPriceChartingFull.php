<?php

declare(strict_types=1);

namespace App\Console\Commands;

use App\Models\Product;
use App\Models\VideoGame;
use App\Models\VideoGameTitle;
use App\Services\PriceCharting\PriceChartingClient;
use Illuminate\Console\Command;
use Illuminate\Http\Client\Pool;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Http;
use Illuminate\Support\Facades\Log;
use Illuminate\Support\Str;

class ImportPriceChartingFull extends Command
{
    protected $signature = 'pricecharting:import-full {--console-id= : Optional specific console ID to import} {--resume=1 : Resume from last checkpoint} {--reset-checkpoint : Clear existing checkpoint} {--limit= : Limit number of items to process}';

    protected $description = 'Import all games from Price Charting API';

    // Known Console IDs and their Region/Currency mappings
    private const CONSOLE_MAPPINGS = [
        'Nintendo Switch' => ['id' => 'G59', 'region' => 'NTSC', 'currency' => 'USD', 'country' => 'US', 'name' => 'Nintendo Switch'],
        'PAL Nintendo Switch' => ['id' => 'G87', 'region' => 'PAL', 'currency' => 'EUR', 'country' => 'EU', 'name' => 'Nintendo Switch (PAL)'],
        'JP Nintendo Switch' => ['id' => 'G100', 'region' => 'JPN', 'currency' => 'JPY', 'country' => 'JP', 'name' => 'Nintendo Switch (JP)'],
        'Playstation 5' => ['id' => 'G67', 'region' => 'NTSC', 'currency' => 'USD', 'country' => 'US', 'name' => 'Playstation 5'],
        'PAL Playstation 5' => ['id' => 'G95', 'region' => 'PAL', 'currency' => 'EUR', 'country' => 'EU', 'name' => 'Playstation 5 (PAL)'],
        'JP Playstation 5' => ['id' => 'G108', 'region' => 'JPN', 'currency' => 'JPY', 'country' => 'JP', 'name' => 'Playstation 5 (JP)'],
    ];

    private const BATCH_SIZE = 50;

    private const CHECKPOINT_INTERVAL = 500;

    public function handle(PriceChartingClient $client): int
    {
        $this->info('Starting Price Charting Full Import (CSV Driven)...');

        $sourceId = $this->ensureSourceExists();

        $csvPath = storage_path('price-charting/price-guide-from-price-charting.csv');
        if (! file_exists($csvPath)) {
            $this->error("CSV file not found at: $csvPath");

            return Command::FAILURE;
        }

        $handle = fopen($csvPath, 'r');
        if ($handle === false) {
            $this->error('Failed to open CSV.');

            return Command::FAILURE;
        }

        $header = fgetcsv($handle);

        // Handle Checkpoint
        $provider = 'price_charting';
        $resumeEnabled = (int) $this->option('resume') !== 0;
        $resetCheckpoint = (bool) $this->option('reset-checkpoint');

        if ($resetCheckpoint) {
            $this->forgetCheckpoint($csvPath, $provider);
        }

        if ($resumeEnabled && ! $resetCheckpoint) {
            $this->maybeSeekToCheckpoint($handle, $csvPath, $provider);
        }

        // Determine Filter
        $requestedConsole = $this->option('console-id');
        $targetConsoles = [];

        if ($requestedConsole) {
            if (isset(self::CONSOLE_MAPPINGS[$requestedConsole])) {
                $targetConsoles[$requestedConsole] = self::CONSOLE_MAPPINGS[$requestedConsole];
            } else {
                $targetConsoles[$requestedConsole] = [
                    'region' => 'UNK',
                    'currency' => 'USD',
                    'country' => 'US',
                    'name' => $requestedConsole,
                ];
            }
            $this->info("Filtering for: $requestedConsole");
        } else {
            $targetConsoles = self::CONSOLE_MAPPINGS;
            $this->info('Importing ALL mapped consoles ('.count($targetConsoles).')');
        }

        // Iterate
        $processed = 0;
        $errors = 0;
        $buffer = [];
        $lastCheckpointRows = 0;
        $limit = $this->option('limit') ? (int) $this->option('limit') : null;

        // Get file size for progress bar
        $fileSize = filesize($csvPath);
        $bar = $this->output->createProgressBar($fileSize);
        $bar->setFormat(" %current%/%max% [%bar%] %percent:3s%% %elapsed:6s%/%estimated:-6s% %memory:6s%\n %message%");
        $bar->start();

        while (($row = fgetcsv($handle)) !== false) {
            // Update progress bar by bytes
            $currentPos = ftell($handle);
            $bar->setProgress($currentPos);

            if ($limit && $processed >= $limit) {
                break;
            }

            if (count($row) < 3) {
                continue;
            }

            $pcId = $row[0];
            $consoleName = $row[1];

            if (! array_key_exists($consoleName, $targetConsoles)) {
                $bar->setMessage("Skipping: $consoleName");

                continue;
            }

            $meta = $targetConsoles[$consoleName];
            $bar->setMessage("Processing: $consoleName ({$meta['name']})");

            try {
                // Buffer the request logic
                // We actually need to fetch the product details from API to get the full data (UPC, etc)

                // Add to buffer for parallel processing
                $buffer[$pcId] = ['meta' => $meta, 'row' => $row];

                $processed++;
                // $this->output->write("\rQueued: {$pcId} ({$processed})");

                if (count($buffer) >= self::BATCH_SIZE) {
                    $this->processBuffer($buffer, $sourceId);
                    $buffer = [];

                    // Save checkpoint after successful batch processing
                    if ($resumeEnabled && ($processed - $lastCheckpointRows) >= self::CHECKPOINT_INTERVAL) {
                        $pos = ftell($handle);
                        $this->storeCheckpoint($csvPath, $provider, ['pos' => $pos, 'processed' => $processed]);
                        $lastCheckpointRows = $processed;
                        $this->info(" Checkpoint saved at $processed records.");
                    }
                }

            } catch (\Exception $e) {
                $errors++;
                Log::error("Failed to import PC ID $pcId", ['error' => $e->getMessage()]);
            }
        }

        // Flush remaining
        if (! empty($buffer)) {
            $this->processBuffer($buffer, $sourceId);
        }

        $bar->finish();
        fclose($handle);

        if ($resumeEnabled) {
            $this->forgetCheckpoint($csvPath, $provider);
        }

        $this->newLine();
        $this->info("Import Complete. Processed: $processed, Errors: $errors");

        return Command::SUCCESS;
    }

    private function processBuffer(array $buffer, int $sourceId): void
    {
        // 1. Fetch from API in parallel
        $responses = Http::pool(function (Pool $pool) use ($buffer) {
            $token = config('services.price_charting.token');
            foreach ($buffer as $id => $data) {
                $pool->as((string) $id)->get('https://www.pricecharting.com/api/product', [
                    't' => $token,
                    'id' => $id,
                ]);
            }
        });

        $apiResults = [];
        foreach ($responses as $id => $response) {
            if ($response->ok()) {
                $product = $response->json();
                $meta = $buffer[$id]['meta'];
                $apiResults[] = ['product' => $product, 'meta' => $meta];
            } else {
                Log::error("Failed to fetch PriceCharting ID $id: ".$response->status());
            }
        }

        if (empty($apiResults)) {
            return;
        }

        // 2. Process DB writes in transaction
        DB::transaction(function () use ($apiResults, $sourceId) {
            foreach ($apiResults as $item) {
                $product = $item['product'];
                $meta = $item['meta'];
                $pcId = (string) $product['id'];

                $cleanName = $product['product-name'];
                $cleanName = preg_replace('/\s*\[(PAL|JP|JPN|NTSC|EU|UK)\]\s*/i', '', $cleanName);
                $cleanName = preg_replace('/^(JP|PAL|NTSC)\s+/i', '', $cleanName);
                $cleanName = trim($cleanName);
                $slug = Str::slug($cleanName);

                $payload = array_merge($product, [
                    'console_id' => $product['console-name'] ?? $meta['name'],
                    'imported_region' => $meta['region'],
                    'imported_currency' => $meta['currency'],
                    'original_name' => $product['product-name'],
                    'clean_name' => $cleanName,
                ]);

                // 1. Ensure Product Exists
                $productModel = Product::firstOrCreate(
                    ['slug' => $slug],
                    [
                        'name' => $cleanName,
                        'title' => $cleanName,
                        'normalized_title' => $slug,
                        'type' => 'video_game',
                    ]
                );

                // 2. Ensure Video Game Title Exists
                $titleModel = VideoGameTitle::firstOrCreate(
                    ['product_id' => $productModel->id, 'slug' => $slug],
                    ['name' => $cleanName]
                );

                // 3. Upsert Source (with Link)
                DB::table('video_game_title_sources')->updateOrInsert(
                    [
                        'provider' => 'price_charting',
                        'external_id' => $pcId,
                    ],
                    [
                        'video_game_title_id' => $titleModel->id,
                        'video_game_source_id' => $sourceId,
                        'provider_item_id' => $pcId,
                        'name' => $cleanName,
                        'platform' => json_encode([$product['console-name'] ?? $meta['name']]),
                        'raw_payload' => json_encode($payload),
                        'updated_at' => now(),
                    ]
                );

                // 4. Ensure Video Game Exists
                // Map multiple sources (PAL/NTSC) to the same canonical VideoGame if they share a Title
                $gameModel = VideoGame::firstOrCreate(
                    ['video_game_title_id' => $titleModel->id],
                    [
                        'provider' => 'price_charting',
                        'external_id' => $pcId,
                        'name' => $cleanName,
                    ]
                );

                // 5. Upsert Prices
                $this->upsertPrices($gameModel->id, $product, $meta);
            }
        });

        // $this->output->write("."); // Removed dot output as we use progress bar now
    }

    private function upsertPrices(int $gameId, array $product, array $meta): void
    {
        $pricePoints = [
            'loose-price' => 'loose',
            'cib-price' => 'cib',
            'new-price' => 'new',
            'graded-price' => 'graded',
            'box-only-price' => 'box_only',
            'manual-only-price' => 'manual_only',
        ];

        $now = now();

        foreach ($pricePoints as $apiField => $condition) {
            if (isset($product[$apiField]) && $product[$apiField] > 0) {
                DB::table('video_game_prices')->updateOrInsert(
                    [
                        'video_game_id' => $gameId,
                        'retailer' => 'price_charting',
                        'condition' => $condition,
                        'currency' => $meta['currency'],
                    ],
                    [
                        'amount_minor' => (int) $product[$apiField],
                        'recorded_at' => $now,
                        'tax_inclusive' => false,
                        'country_code' => $meta['country'],
                        'sales_volume' => $product['sales-volume'] ?? null,
                        'is_retail_buy' => false,
                        'is_active' => true,
                        'bucket' => 'raw',
                        'aggregation_count' => 1,
                        'updated_at' => $now,
                    ]
                );
            }
        }
    }

    private function ensureSourceExists(): int
    {
        $source = DB::table('video_game_sources')->where('provider', 'price_charting')->first();
        if (! $source) {
            return DB::table('video_game_sources')->insertGetId([
                'provider' => 'price_charting',
                'items_count' => 0,
                'metadata' => json_encode(['name' => 'Price Charting', 'url' => 'https://www.pricecharting.com']),
                'created_at' => now(),
                'updated_at' => now(),
            ]);
        }

        return $source->id;
    }

    // Checkpoint methods
    private function checkpointPath(string $file, string $provider): string
    {
        $dir = storage_path('app/.checkpoints');
        if (! file_exists($dir)) {
            mkdir($dir, 0755, true);
        }
        $key = sha1($provider.'|'.realpath($file));

        return $dir.'/import-'.$key.'.json';
    }

    private function loadCheckpoint(string $file, string $provider): ?array
    {
        $path = $this->checkpointPath($file, $provider);
        if (! file_exists($path)) {
            return null;
        }

        return json_decode(file_get_contents($path), true);
    }

    private function storeCheckpoint(string $file, string $provider, array $data): void
    {
        $path = $this->checkpointPath($file, $provider);
        file_put_contents($path, json_encode($data));
    }

    private function forgetCheckpoint(string $file, string $provider): void
    {
        $path = $this->checkpointPath($file, $provider);
        if (file_exists($path)) {
            unlink($path);
        }
    }

    private function maybeSeekToCheckpoint($handle, string $file, string $provider): void
    {
        $checkpoint = $this->loadCheckpoint($file, $provider);
        if ($checkpoint && isset($checkpoint['pos'])) {
            fseek($handle, $checkpoint['pos']);
            $this->info('Resuming from offset: '.$checkpoint['pos']);
        }
    }
}
