<?php

declare(strict_types=1);

namespace App\Console\Commands;

use App\Models\Image;
use App\Models\Product;
use App\Models\VideoGameSource;
use App\Models\VideoGameTitle;
use App\Models\VideoGameTitleSource;
use App\Services\Tgdb\TgdbClient;
use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Log;
use Illuminate\Support\Str;

class ImportTgdbMirror extends Command
{
    protected $signature = 'tgdb:import-mirror 
                            {--platform-id= : Optional platform ID to only import specific console}
                            {--resume=1 : Resume from last checkpoint}
                            {--reset-checkpoint : Ignore existing checkpoint}
                            {--limit=0 : Optional record limit}
                            {--low-memory : Use smaller batch sizes for 1GB RAM servers}';

    protected $description = 'Mirror the entire TheGamesDB catalog locally for media backfilling';

    private array $productCache = [];
    private array $titleCache = [];
    private array $sourceCache = [];
    
    private array $videoGameBatch = [];
    private array $videoGameTitleSourceBatch = [];
    private array $imageBatch = [];

    private int $batchSize = 5000;
    private int $recordBufferSize = 10000;
    private const MAX_SAFE_PARAMS = 65000;

    public function handle(TgdbClient $client): int
    {
        if ($this->option('low-memory')) {
            $this->batchSize = 1000;
            $this->recordBufferSize = 2000;
            $this->info('Low memory mode enabled.');
        }

        $this->info('Starting TheGamesDB Mirror...');

        // Disable query log and optimizations
        DB::disableQueryLog();
        if (DB::getDriverName() === 'pgsql') {
            DB::statement('SET synchronous_commit = OFF');
            DB::statement('SET CONSTRAINTS ALL DEFERRED');
        }

        $sourceId = $this->ensureSourceExists();
        $this->sourceCache['tgdb'] = $sourceId;

        $platformsData = $client->getPlatforms();
        $platforms = $platformsData['data']['platforms'] ?? [];

        if (empty($platforms)) {
            $this->error('No platforms found or API error.');
            return Command::FAILURE;
        }

        if ($reqId = $this->option('platform-id')) {
            $platforms = array_filter($platforms, fn ($p) => $p['id'] == $reqId);
        }

        $resumeEnabled = (int)$this->option('resume') !== 0;
        $resetCheckpoint = (bool)$this->option('reset-checkpoint');
        $checkpoint = ($resumeEnabled && !$resetCheckpoint) ? $this->loadCheckpoint() : null;

        if ($resetCheckpoint) {
            $this->forgetCheckpoint();
        }

        $bar = $this->output->createProgressBar(count($platforms));
        $bar->setFormat(' %current%/%max% [%bar%] %percent:3s%% %elapsed:6s% %message%');
        $bar->start();

        foreach ($platforms as $platform) {
            $pid = (int)$platform['id'];
            
            if ($checkpoint && isset($checkpoint['last_platform_id']) && $pid < $checkpoint['last_platform_id']) {
                $bar->advance();
                continue;
            }

            $bar->setMessage("Platform: {$platform['name']}");
            $this->processPlatform($client, $platform, $sourceId, $checkpoint);
            
            // Clear checkpoint after platform finished or update it
            $this->storeCheckpoint(['last_platform_id' => $pid, 'last_page' => 0]);
            $checkpoint = null; // Clear so next platform starts from page 1

            $bar->advance();
            $this->flushBatches();
        }

        $bar->finish();
        $this->forgetCheckpoint();
        $this->newLine();
        $this->info('Mirror Complete.');

        return Command::SUCCESS;
    }

    private function ensureSourceExists(): int
    {
        $source = VideoGameSource::firstOrCreate(
            ['provider' => 'tgdb'],
            [
                'name' => 'TheGamesDB',
                'base_url' => 'https://thegamesdb.net',
                'items_count' => 0,
            ]
        );

        return $source->id;
    }

    private function processPlatform(TgdbClient $client, array $platform, int $sourceId, ?array $checkpoint): void
    {
        $pid = (int)$platform['id'];
        $pname = $platform['name'];
        $page = ($checkpoint && isset($checkpoint['last_platform_id']) && $pid == $checkpoint['last_platform_id']) 
            ? ($checkpoint['last_page'] ?: 1) 
            : 1;

        if ($page > 1) {
            $this->warn(" Resuming platform {$pname} from page {$page}...");
        }

        try {
            $response = $client->getGamesByPlatform($pid, $page);
            $this->processGamePage($response, $client, $sourceId, $pname);
            
            $hasMore = isset($response['pages']['next']);
            
            while ($hasMore) {
                $page++;
                $response = $client->getGamesByPlatform($pid, $page);
                $this->processGamePage($response, $client, $sourceId, $pname);
                
                $this->storeCheckpoint(['last_platform_id' => $pid, 'last_page' => $page]);

                $hasMore = isset($response['pages']['next']);
                
                if ($this->option('limit') && count($this->productCache) >= $this->option('limit')) {
                    $hasMore = false;
                }
            }
        } catch (\Exception $e) {
            Log::error("TGDB Error Platform $pid Page $page: ".$e->getMessage());
        }
    }

    private function processGamePage(array $response, TgdbClient $client, int $sourceId, string $pname): void
    {
        $gamesDict = $response['data']['games'] ?? [];
        if (empty($gamesDict)) {
            return;
        }

        $games = array_values($gamesDict);
        $gameIds = array_column($games, 'id');

        try {
            $imagesResponse = $client->getImages($gameIds);
            $imagesDict = $imagesResponse['data']['images'] ?? [];
            $baseUrl = $imagesResponse['data']['base_url']['original'] ?? 'https://cdn.thegamesdb.net/images/original/';

            $this->processGameRecordsBatch($games, $imagesDict, $baseUrl, $sourceId, $pname);
        } catch (\Exception $e) {
            Log::error('TGDB Image Batch Error', ['exception' => $e]);
        }
    }

    private function processGameRecordsBatch(array $records, array $imagesDict, string $baseUrl, int $sourceId, string $platformName): void
    {
        $now = now();
        $provider = 'tgdb';

        // 1. Buffer Products & Titles
        $productRowsBySlug = [];
        foreach ($records as $record) {
            $name = $record['game_title'];
            $slug = Str::slug($name);
            if ($slug === '') $slug = 'tgdb-' . $record['id'];

            if (!isset($productRowsBySlug[$slug])) {
                $productRowsBySlug[$slug] = [
                    'name' => $name,
                    'normalized_title' => $slug,
                    'synopsis' => $record['overview'] ?? null,
                ];
            }
        }

        // 2. Insert Products
        $productRows = [];
        foreach ($productRowsBySlug as $slug => $row) {
            $productRows[] = [
                'slug' => $slug,
                'name' => $row['name'],
                'title' => $row['name'],
                'normalized_title' => $row['normalized_title'],
                'synopsis' => $row['synopsis'],
                'type' => 'video_game',
                'created_at' => $now,
                'updated_at' => $now,
            ];
        }
        foreach (array_chunk($productRows, $this->batchSize) as $chunk) {
            DB::table('products')->insertOrIgnore($chunk);
        }

        // 3. Resolve Product IDs
        $slugs = array_keys($productRowsBySlug);
        $productIdBySlug = DB::table('products')->whereIn('slug', $slugs)->pluck('id', 'slug')->all();

        // 4. Insert Titles
        $titleRows = [];
        foreach ($productRowsBySlug as $slug => $row) {
            $productId = $productIdBySlug[$slug] ?? null;
            if (!$productId) continue;

            $titleRows[] = [
                'product_id' => $productId,
                'name' => $row['name'],
                'normalized_title' => $row['normalized_title'],
                'slug' => $slug,
                'providers' => json_encode([$provider]),
                'created_at' => $now,
                'updated_at' => $now,
            ];
        }
        foreach (array_chunk($titleRows, $this->batchSize) as $chunk) {
            DB::table('video_game_titles')->insertOrIgnore($chunk);
        }

        // 5. Resolve Title IDs
        $titleBySlug = DB::table('video_game_titles')
            ->whereIn('slug', $slugs)
            ->get(['id', 'slug'])
            ->keyBy('slug')
            ->all();

        // 6. Enqueue Sources and Video Games
        foreach ($records as $record) {
            $gid = (string)$record['id'];
            $name = $record['game_title'];
            $slug = Str::slug($name);
            if ($slug === '') $slug = 'tgdb-' . $gid;

            $title = $titleBySlug[$slug] ?? null;
            if (!$title) continue;

            $this->videoGameTitleSourceBatch[] = [
                'video_game_title_id' => $title->id,
                'video_game_source_id' => $sourceId,
                'external_id' => (int)$gid,
                'provider_item_id' => $gid,
                'provider' => $provider,
                'slug' => $slug,
                'name' => $name,
                'release_date' => $record['release_date'] ?? null,
                'platform' => json_encode([$platformName]),
                'raw_payload' => json_encode($record),
                'created_at' => $now,
                'updated_at' => $now,
            ];

            $this->videoGameBatch[] = [
                'video_game_title_id' => $title->id,
                'provider' => $provider,
                'external_id' => (int)$gid,
                'slug' => $slug,
                'name' => $name,
                'release_date' => $record['release_date'] ?? null,
                'attributes' => json_encode([
                    'platform' => [$platformName],
                    'release_date' => $record['release_date'] ?? null,
                ]),
                'created_at' => $now,
                'updated_at' => $now,
            ];

            // Media
            if (isset($imagesDict[$gid])) {
                foreach ($imagesDict[$gid] as $img) {
                    $url = $baseUrl . $img['filename'];
                    $this->imageBatch[] = [
                        'imageable_type' => VideoGameTitleSource::class,
                        'provider_item_id' => $gid, // Temporary key for resolution
                        'url' => $url,
                        'provider' => $provider,
                        'is_thumbnail' => ($img['type'] === 'boxart' && ($img['side'] ?? '') === 'front'),
                        'metadata' => json_encode($img),
                        'created_at' => $now,
                        'updated_at' => $now,
                    ];
                }
            }

            if (count($this->videoGameTitleSourceBatch) >= $this->recordBufferSize) {
                $this->flushBatches();
            }
        }
    }

    private function flushBatches(): void
    {
        if (empty($this->videoGameTitleSourceBatch)) return;

        $now = now();

        // 1. Upsert Title Sources
        DB::table('video_game_title_sources')->upsert(
            $this->videoGameTitleSourceBatch,
            ['video_game_source_id', 'provider_item_id'],
            ['video_game_title_id', 'name', 'release_date', 'platform', 'raw_payload', 'updated_at']
        );

        // 2. Upsert Video Games
        DB::table('video_games')->upsert(
            $this->videoGameBatch,
            ['provider', 'external_id'],
            ['video_game_title_id', 'name', 'release_date', 'attributes', 'updated_at']
        );

        // 3. Handle Images
        if (!empty($this->imageBatch)) {
            // Need to map provider_item_id to title_source_id
            $itemIds = array_unique(array_column($this->imageBatch, 'provider_item_id'));
            $sourceIdMap = DB::table('video_game_title_sources')
                ->where('video_game_source_id', $this->sourceCache['tgdb'])
                ->whereIn('provider_item_id', $itemIds)
                ->pluck('id', 'provider_item_id')
                ->all();

            $finalImageRows = [];
            foreach ($this->imageBatch as $img) {
                $sourceId = $sourceIdMap[$img['provider_item_id']] ?? null;
                if (!$sourceId) continue;

                unset($img['provider_item_id']);
                $img['imageable_id'] = $sourceId;
                $finalImageRows[] = $img;
            }

            if (!empty($finalImageRows)) {
                DB::table('images')->upsert(
                    $finalImageRows,
                    ['imageable_type', 'imageable_id', 'url'],
                    ['is_thumbnail', 'metadata', 'updated_at']
                );
            }
        }

        $this->videoGameTitleSourceBatch = [];
        $this->videoGameBatch = [];
        $this->imageBatch = [];
    private function checkpointPath(): string
    {
        $dir = storage_path('app/checkpoints');
        if (!is_dir($dir)) mkdir($dir, 0755, true);
        return $dir . '/tgdb-mirror-checkpoint.json';
    }

    private function loadCheckpoint(): ?array
    {
        $path = $this->checkpointPath();
        if (!file_exists($path)) return null;

        $data = json_decode(file_get_contents($path), true);
        return is_array($data) ? $data : null;
    }

    private function storeCheckpoint(array $data): void
    {
        file_put_contents($this->checkpointPath(), json_encode($data));
    }

    private function forgetCheckpoint(): void
    {
        $path = $this->checkpointPath();
        if (file_exists($path)) unlink($path);
    }
}
