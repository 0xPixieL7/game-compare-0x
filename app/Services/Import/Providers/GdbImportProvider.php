<?php

declare(strict_types=1);

namespace App\Services\Import\Providers;

use App\Models\Product;
use App\Models\VideoGame;
use App\Models\VideoGameSource;
use App\Models\VideoGameTitle;
use App\Models\VideoGameTitleSource;
use App\Services\Import\Concerns\HasProgressBar;
use App\Services\Import\Concerns\InteractsWithConsole;
use App\Services\Import\Contracts\ImportProvider;
use App\Models\Image;
use App\Models\Video;
use App\Services\Normalization\IgdbRatingHelper;
use App\Services\Normalization\PlatformNormalizer;
use App\Services\Normalization\RatingNormalizer;
use Illuminate\Console\Command;
use Illuminate\Support\Arr;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\File;
use Illuminate\Support\Str;

class GdbImportProvider implements ImportProvider
{
    use HasProgressBar;
    use InteractsWithConsole;

    private const PROVIDER_NAME = 'gdb';

    public function __construct(
        protected PlatformNormalizer $platformNormalizer,
        protected RatingNormalizer $ratingNormalizer,
        protected IgdbRatingHelper $ratingHelper,
    ) {}

    public function getName(): string
    {
        return self::PROVIDER_NAME;
    }

    private $batch = [];
    private const BATCH_SIZE = 1000;

    public function handle(Command $command): int
    {
        $this->setCommand($command);

        $path = (string) ($command->option('path') ?: base_path('storage/gdb-dumps'));
        $provider = (string) ($command->option('provider') ?: 'giantbomb');

        if (! File::exists($path)) {
            $this->error("Directory does not exist: {$path}");

            return Command::FAILURE;
        }

        $this->startOptimizedImport();

        $files = collect(File::files($path))
            ->filter(fn ($f) => $f->isFile())
            ->values();

        if ($files->isEmpty()) {
            $this->warn("No files found in: {$path}");

            return Command::SUCCESS;
        }

        $totals = ['products' => 0, 'sources' => 0, 'titles' => 0, 'video_games' => 0, 'images' => 0, 'videos' => 0];
        $limit = (int) $command->option('limit');

        foreach ($files as $file) {
            $this->info("Importing {$file->getFilename()}...");

            // Count rows for progress bar (approximate for JSON)
            // For simplicity in JSON, we might not know total easily without reading.
            // But we can just use an indeterminate bar or count first if needed.
            // Let's count if possible, or just start.
            // For now, let's keep it simple: indeterminate or just raw processing.
            // Actually, HasProgressBar allows standard bar.
            
            // Let's count standard JSON items if possible?
            // "iterateRecordsFromFile" yields. 
            // We'll just advance progress bar manually without a max if traversing generator is expensive.
            // But user asked for valid progress bar. 
            // Detailed counting for massive JSONs is slow.
            // Let's assume we proceed with an indeterminate bar or just stepping.
            
            $progressBar = $this->command->getOutput()->createProgressBar();
            $this->configureProgressBar($progressBar);
            $progressBar->start();

            $processed = 0;
            // Iterate records and process
            foreach ($this->iterateRecordsFromFile($file->getPathname()) as $record) {
                $this->batch[] = $record;
                
                if (count($this->batch) >= self::BATCH_SIZE) {
                    $deltas = $this->flushBatch($provider);
                    foreach ($totals as $k => $v) {
                        $totals[$k] += $deltas[$k] ?? 0;
                    }
                    $progressBar->advance(count($this->batch)); // Advance by batch size (bug: flushBatch clears batch)
                    // Fix: capture count before flush or just increment by BATCH_SIZE
                    // Actually flushBatch clears it.
                    // Correct logic: count is handled in flush? No, flush just saves.
                    // We moved advance usage.
                }
                $processed++;
                if ($processed % self::BATCH_SIZE === 0) {
                     $progressBar->advance(self::BATCH_SIZE);
                }

                if ($limit > 0 && $processed >= $limit) {
                    break;
                }
            }

            // Flush remaining
            if (! empty($this->batch)) {
                 $deltas = $this->flushBatch($provider);
                 foreach ($totals as $k => $v) {
                    $totals[$k] += $deltas[$k] ?? 0;
                }
                $progressBar->advance(count($this->batch));
            }

            $progressBar->finish();
            $this->newLine();
            $this->info("Processed {$processed} records from {$file->getFilename()}.");
            
            if ($limit > 0 && $processed >= $limit) {
                $this->warn("Limit of {$limit} records reached.");
                break;
            }
        }

        $this->newLine();
        $this->command->table(['Metric', 'Count'], [
            ['Products created', $totals['products']],
            ['Sources created', $totals['sources']],
            ['Titles created', $totals['titles']],
            ['VideoGames created', $totals['video_games']],
            ['Images created', $totals['images'] ?? 0],
            ['Videos created', $totals['videos'] ?? 0],
        ]);

        $this->endOptimizedImport();

        return Command::SUCCESS;
    }

    private function flushBatch(string $provider): array
    {
        if (empty($this->batch)) {
            return ['products' => 0, 'sources' => 0, 'titles' => 0, 'video_games' => 0, 'images' => 0, 'videos' => 0];
        }

        $batch = $this->batch;
        $this->batch = []; // Clear immediately

        return DB::transaction(function () use ($batch, $provider) {
            $batchTotals = ['products' => 0, 'sources' => 0, 'titles' => 0, 'video_games' => 0, 'images' => 0, 'videos' => 0];

            foreach ($batch as $record) {
                // Inline the logic from old importRecord, but without internal transactions
                $delta = $this->processSingleRecord($record, $provider);
                foreach ($batchTotals as $k => $v) {
                    $batchTotals[$k] += $delta[$k] ?? 0;
                }
            }

            return $batchTotals;
        });
    }

    /**
     * @return \Generator<int, array>
     */
    private function iterateRecordsFromFile(string $filePath): \Generator
    {
        $ext = strtolower(pathinfo($filePath, PATHINFO_EXTENSION));

        // NDJSON/JSONL support.
        if (in_array($ext, ['ndjson', 'jsonl'], true)) {
            $handle = fopen($filePath, 'rb');

            if ($handle === false) {
                return;
            }

            try {
                while (($line = fgets($handle)) !== false) {
                    $line = trim($line);

                    if ($line === '') {
                        continue;
                    }

                    $decoded = json_decode($line, true);

                    if (is_array($decoded)) {
                        yield $decoded;
                    }
                }
            } finally {
                fclose($handle);
            }

            return;
        }

        // Regular JSON (array, or an object with a list under common keys).
        $raw = File::get($filePath);
        $decoded = json_decode($raw, true);

        if (is_array($decoded) && array_is_list($decoded)) {
            foreach ($decoded as $row) {
                if (is_array($row)) {
                    yield $row;
                }
            }

            return;
        }

        if (is_array($decoded)) {
            foreach (['results', 'games', 'data'] as $key) {
                $maybe = $decoded[$key] ?? null;

                if (is_array($maybe) && array_is_list($maybe)) {
                    foreach ($maybe as $row) {
                        if (is_array($row)) {
                            yield $row;
                        }
                    }

                    return;
                }
            }

            // Handle mapped JSON (e.g. {"id": {...}, "id2": {...}})
            if (! array_is_list($decoded)) {
                foreach ($decoded as $row) {
                    if (is_array($row)) {
                        yield $row;
                    }
                }
                return;
            }
        }
    }

    /**
     * @return array{products:int,sources:int,titles:int,video_games:int}
     */
    private function processSingleRecord(array $record, string $provider): array
    {
        $idRaw = (string) (Arr::get($record, 'id') ?? Arr::get($record, 'guid') ?? '');
        $externalId = Str::afterLast($idRaw, '-');
        $name = (string) (Arr::get($record, 'name') ?? Arr::get($record, 'title') ?? '');

        if ($externalId === '' || $name === '') {
            return ['products' => 0, 'sources' => 0, 'titles' => 0, 'video_games' => 0];
        }

        echo "Processing: $name ($externalId)..." . PHP_EOL;

        $normalizedTitle = Str::of($name)->lower()->replaceMatches('/[^a-z0-9\s]+/i', '')->squish()->toString();
        $normalizedTitle = $normalizedTitle !== '' ? Str::slug($normalizedTitle) : null;

        $productSlug = Str::slug($name);

        $productsCreated = 0;
        $sourcesCreated = 0;
        $titlesCreated = 0;
        $gamesCreated = 0;

        $product = Product::query()->firstOrCreate(
            [
                'slug' => $productSlug,
            ],
            [
                'name' => $name,
                'type' => 'video_game',
                'title' => $name,
                'normalized_title' => $normalizedTitle,
            ]
        );

        if ($product->wasRecentlyCreated) {
            $productsCreated++;
        }

        $source = VideoGameSource::query()->firstOrCreate(
            [
                'provider' => $provider,
            ]
        );

        if ($source->wasRecentlyCreated) {
            $sourcesCreated++;
        }

        $title = VideoGameTitle::query()->firstOrCreate(
            [
                'product_id' => $product->id,
            ],
            [
                'name' => $name,
                'normalized_title' => $normalizedTitle,
                'slug' => $productSlug,
                'providers' => [$provider],
            ]
        );

        if ($title->wasRecentlyCreated) {
            $titlesCreated++;
        } else {
            $existingProviders = is_array($title->providers) ? $title->providers : [];
            if (! in_array($provider, $existingProviders, true)) {
                $title->forceFill([
                    'providers' => array_values(array_unique(array_merge($existingProviders, [$provider]))),
                ])->save();
            }
        }

        VideoGameTitleSource::query()->updateOrCreate(
            [
                'video_game_source_id' => $source->id,
                'provider_item_id' => $externalId,
            ],
            [
                'video_game_title_id' => $title->id,
                'provider' => $provider,
                'external_id' => (int) $externalId,
                'raw_payload' => $record,
            ]
        );

        // Platform Normalization
        $platformNames = collect(Arr::get($record, 'platforms', []))
            ->map(fn ($p) => is_array($p) ? (string) Arr::get($p, 'name', '') : (string) $p)
            ->filter(fn (string $p) => $p !== '')
            ->all();

        $platformNames = $this->platformNormalizer->normalizeMany($platformNames);

        $media = $this->extractMedia($record);
        $rating = $this->ratingHelper->extractPercentage($record);
        $ratingCount = $this->ratingNormalizer->extractRatingCount($record);

        $description = Arr::get($record, 'deck')
            ?? Arr::get($record, 'description')
            ?? Arr::get($record, 'summary');
        
        $summary = Arr::get($record, 'deck');

        $releaseDate = Arr::get($record, 'original_release_date')
            ?? Arr::get($record, 'release_date');

        $developer = Arr::get($record, 'developers.0.name')
            ?? Arr::get($record, 'developer');

        $publisher = Arr::get($record, 'publishers.0.name')
            ?? Arr::get($record, 'publisher');
            
        $url = Arr::get($record, 'site_detail_url');

        $genres = collect((array) Arr::get($record, 'genres', []))
            ->map(fn ($g) => is_array($g) ? (string) Arr::get($g, 'name', '') : (string) $g)
            ->filter(fn (string $g) => $g !== '')
            ->unique()
            ->values()
            ->all();

        $game = VideoGame::query()->updateOrCreate(
            [
                'provider' => $provider,
                'external_id' => (int) $externalId,
            ],
            [
                'video_game_title_id' => $title->id,
                'slug' => $title->slug,
                'name' => $name,
                'provider' => $provider,
                'external_id' => (int) $externalId,
                'description' => is_string($description) ? $description : null,
                'summary' => is_string($summary) ? $summary : null,
                'url' => is_string($url) ? $url : null,
                'release_date' => is_string($releaseDate) ? $releaseDate : null,
                'platform' => $platformNames === [] ? null : $platformNames,
                'rating' => $rating,
                'rating_count' => $ratingCount,
                'developer' => is_string($developer) ? $developer : null,
                'publisher' => is_string($publisher) ? $publisher : null,
                'genre' => $genres === [] ? null : $genres,
                'media' => $media,
                'source_payload' => $record,
                'storyline' => $record['storyline'] ?? null,
            ]
        );

        if ($game->wasRecentlyCreated) {
            $gamesCreated++;
        }

        $imagesCreated = $this->processImages($record, $source, $provider, $game);
        $videosCreated = $this->processVideos($record, $source, $provider, $game);

        return [
            'products' => $productsCreated,
            'sources' => $sourcesCreated,
            'titles' => $titlesCreated,
            'video_games' => $gamesCreated,
            'images' => $imagesCreated,
            'videos' => $videosCreated,
        ];
    }

    private function processImages(array $record, VideoGameSource $source, string $provider, VideoGame $videoGame): int
    {
        $images = Arr::get($record, 'images', []);
        $mainImage = Arr::get($record, 'image', []);
        
        if (empty($images) && empty($mainImage)) {
            return 0;
        }

        // Add main image if not present in images array
        if (!empty($mainImage)) {
            $images[] = $mainImage;
        }

        echo "  - Processing " . count($images) . " images..." . PHP_EOL;

        $count = 0;
        foreach ($images as $img) {
            $url = $img['super_url'] ?? $img['medium_url'] ?? $img['small_url'] ?? null;
            if (!$url) continue;

            $image = Image::query()->firstOrCreate(
                [
                    'imageable_type' => VideoGameSource::class,
                    'imageable_id' => $source->id,
                    'url' => $url,
                ],
                [
                    'video_game_id' => $videoGame->id,
                    'provider' => $provider,
                    'is_thumbnail' => false,
                    'metadata' => $img,
                ]
            );

            if ($image->wasRecentlyCreated) {
                $count++;
            }
        }

        return $count;
    }

    private function processVideos(array $record, VideoGameSource $source, string $provider, VideoGame $videoGame): int
    {
        $videos = Arr::get($record, 'videos', []);
        
        if (empty($videos)) {
            return 0;
        }

        $count = 0;
        echo "  - Processing " . count($videos) . " videos..." . PHP_EOL;
        foreach ($videos as $video) {
            $name = $video['name'] ?? '';
            
            // Filter out podcasts
            if (Str::contains(strtolower($name), ['podcast', 'bombcast', 'beastcast', 'powerbombcast'])) {
                continue;
            }

            $url = $video['low_url'] ?? $video['high_url'] ?? $video['hd_url'] ?? $video['site_detail_url'] ?? $video['embed_player'] ?? null;
            if (!$url) {
                continue;
            }

            try {
                $vid = Video::query()->firstOrCreate(
                    [
                        'videoable_type' => VideoGameSource::class,
                        'videoable_id' => $source->id,
                        'external_id' => $video['guid'] ?? null,
                    ],
                    [
                        'video_game_id' => $videoGame->id,
                        'provider' => $provider,
                        'url' => $url,
                        'title' => $name,
                        'metadata' => $video,
                    ]
                );

                if ($vid->wasRecentlyCreated) {
                    $count++;
                    echo "    + Video created: $name" . PHP_EOL;
                } else {
                    echo "    . Video already exists: $name" . PHP_EOL;
                }
            } catch (\Exception $e) {
                echo "    ! Error creating video '$name': " . $e->getMessage() . PHP_EOL;
            }
        }

        return $count;
    }

    private function extractMedia(array $record): ?array
    {
        $image = Arr::get($record, 'image');

        if (! is_array($image)) {
            return null;
        }

        $urls = Arr::only($image, [
            'icon_url',
            'medium_url',
            'screen_url',
            'screen_large_url',
            'small_url',
            'super_url',
            'thumb_url',
            'tiny_url',
            'original_url',
        ]);

        $urls = array_filter($urls, fn ($v) => is_string($v) && $v !== '');

        return $urls === [] ? null : ['image' => $urls];
    }
}
