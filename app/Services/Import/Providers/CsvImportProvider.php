<?php

declare(strict_types=1);

namespace App\Services\Import\Providers;

use App\DTOs\ImageDTO;
use App\DTOs\MediaDTO;
use App\DTOs\ProductDTO;
use App\DTOs\VideoDTO;
use App\DTOs\VideoGameDTO;
use App\DTOs\VideoGameTitleSourceDTO;
use App\Services\Import\Concerns\HasProgressBar;
use App\Services\Import\Concerns\InteractsWithConsole;
use App\Services\Import\Contracts\ImportProvider;
use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;

class CsvImportProvider implements ImportProvider
{
    use HasProgressBar;
    use InteractsWithConsole;

    private const PROVIDER_NAME = 'csv';

    public function getName(): string
    {
        return self::PROVIDER_NAME;
    }

    public function handle(Command $command): int
    {
        $this->setCommand($command);

        $basePath = (string) ($command->argument('path') ?? storage_path('sqlite_exports'));

        if (! is_dir($basePath)) {
            $this->error("Directory not found: $basePath");

            return Command::FAILURE;
        }

        $this->info("Starting import from: $basePath");

        // 1. Import Products
        $this->importProducts("$basePath/products.csv");

        // 2. Import Video Game Title Sources (Mapping)
        $this->importVideoGameTitleSources("$basePath/video_game_titles.csv");

        // 3. Import Video Games
        $this->importVideoGames("$basePath/video_games.csv");

        // 4. Import Images
        $this->importImages("$basePath/game_images.csv");

        // 5. Import Videos
        $this->importVideos("$basePath/game_videos.csv");

        // 6. Import Media (Spatie)
        $this->importMedia("$basePath/media.csv");

        $this->info('Import completed successfully.');

        return Command::SUCCESS;
    }

    private function importProducts(string $filePath): void
    {
        if (! file_exists($filePath)) {
            $this->warn("File not found: $filePath");

            return;
        }

        $this->info('Importing Products...');
        $this->processCsv($filePath, function (array $chunk) {
            $dataToInsert = [];
            $titlesToInsert = [];
            foreach ($chunk as $row) {
                $dto = ProductDTO::fromCsv($row);
                $dataToInsert[] = $dto->toArray();

                // Auto-create VideoGameTitle
                $titlesToInsert[] = [
                    'id' => $dto->id,
                    'product_id' => $dto->id,
                    'name' => $dto->name,
                    'normalized_title' => \Illuminate\Support\Str::slug($dto->name),
                    'slug' => $dto->slug,
                    'providers' => json_encode([]),
                    'created_at' => $dto->created_at,
                    'updated_at' => $dto->updated_at,
                ];
            }

            if (! empty($dataToInsert)) {
                DB::table('products')->upsert(
                    $dataToInsert,
                    ['id'],
                    ['name', 'slug', 'updated_at']
                );

                DB::table('video_game_titles')->upsert(
                    $titlesToInsert,
                    ['id'],
                    ['product_id', 'name', 'updated_at']
                );
            }
        });
    }

    private function importVideoGames(string $filePath): void
    {
        if (! file_exists($filePath)) {
            $this->warn("File not found: $filePath");

            return;
        }

        $this->info('Importing Video Games...');
        $this->processCsv($filePath, function (array $chunk) {
            $dataToInsert = [];
            foreach ($chunk as $row) {
                $dto = VideoGameDTO::fromCsv($row);
                $attributes = $dto->attributes;

                // Determine provider
                $provider = 'unknown';
                if (isset($attributes['original_metadata']['sources'])) {
                    $sources = $attributes['original_metadata']['sources'];
                    $provider = array_key_first($sources) ?? 'unknown';
                }

                $videoGameTitleId = (int) ($row['product_id'] ?? 0);

                $dataToInsert[] = [
                    'id' => $row['id'],
                    'video_game_title_id' => $videoGameTitleId,
                    'external_id' => $dto->external_id,
                    'provider' => $provider,
                    'attributes' => json_encode($dto->attributes),
                    'created_at' => now(),
                    'updated_at' => now(),
                ];
            }

            if (! empty($dataToInsert)) {
                DB::table('video_games')->upsert(
                    $dataToInsert,
                    ['id'],
                    ['video_game_title_id', 'external_id', 'provider', 'attributes', 'updated_at']
                );
            }
        });
    }

    private function importVideoGameTitleSources(string $filePath): void
    {
        if (! file_exists($filePath)) {
            $this->warn("File not found: $filePath");

            return;
        }

        if (DB::table('video_game_sources')->where('id', 1)->doesntExist()) {
            DB::table('video_game_sources')->insert([
                'id' => 1,
                'provider' => 'igdb',
                'created_at' => now(),
                'updated_at' => now(),
            ]);
        }

        $this->info('Importing Video Game Title Sources...');
        $this->processCsv($filePath, function (array $chunk) {
            $dataToInsert = [];
            foreach ($chunk as $row) {
                // Assuming mapping CSV is correct and matches DTO/table structure
                // But VideoGameTitleSourceDTO might expect specific fields.
                // We'll trust existing logic mapping row to DTO.
                $dto = VideoGameTitleSourceDTO::fromCsv($row);
                $data = $dto->toArray();

                $data['provider'] = 'igdb';

                $dataToInsert[] = $data;
            }

            if (! empty($dataToInsert)) {
                DB::table('video_game_title_sources')->upsert(
                    $dataToInsert,
                    ['id'],
                    ['updated_at', 'raw_payload', 'provider']
                );
            }
        });
    }

    private function importImages(string $filePath): void
    {
        if (! file_exists($filePath)) {
            $this->warn("File not found: $filePath");

            return;
        }

        $this->info('Importing Images...');
        $this->processCsv($filePath, function (array $chunk) {
            $dataToInsert = [];
            foreach ($chunk as $row) {
                $dto = ImageDTO::fromCsv($row);
                $dataToInsert[] = $dto->toArray();
            }

            if (! empty($dataToInsert)) {
                // Adjust upsert columns based on unique constraints or intent
                // Typically upsert on id
                DB::table('images')->upsert(
                    $dataToInsert,
                    ['id'],
                    ['url', 'width', 'height', 'is_thumbnail', 'updated_at']
                );
            }
        });
    }

    private function importVideos(string $filePath): void
    {
        if (! file_exists($filePath)) {
            $this->warn("File not found: $filePath");

            return;
        }

        $this->info('Importing Videos...');
        $this->processCsv($filePath, function (array $chunk) {
            $dataToInsert = [];
            foreach ($chunk as $row) {
                $dto = VideoDTO::fromCsv($row);
                $dataToInsert[] = $dto->toArray();
            }

            if (! empty($dataToInsert)) {
                DB::table('videos')->upsert(
                    $dataToInsert,
                    ['id'],
                    ['url', 'video_id', 'provider', 'updated_at']
                );
            }
        });
    }

    private function importMedia(string $filePath): void
    {
        if (! file_exists($filePath)) {
            $this->warn("File not found: $filePath");

            return;
        }

        $this->info('Importing Spatie Media...');
        $this->processCsv($filePath, function (array $chunk) {
            $dataToInsert = [];
            foreach ($chunk as $row) {
                $dto = MediaDTO::fromCsv($row);
                $dataToInsert[] = $dto->toArray();
            }

            if (! empty($dataToInsert)) {
                DB::table('media')->upsert(
                    $dataToInsert,
                    ['id'],
                    ['updated_at', 'manipulations', 'custom_properties']
                );
            }
        });
    }

    private function processCsv(string $filePath, callable $callback, int $chunkSize = 2000): void
    {
        $limit = (int) $this->command->option('limit');

        // 1. Count lines for progress bar
        $totalLines = 0;
        if (file_exists($filePath)) {
            // Fast line counting for Mac/Linux
            $output = [];
            $exitCode = 0;
            exec('wc -l < '.escapeshellarg($filePath), $output, $exitCode);
            if ($exitCode === 0 && isset($output[0])) {
                $totalLines = (int) trim($output[0]);
            }
        }

        $handle = fopen($filePath, 'r');
        if ($handle === false) {
            return;
        }

        $header = fgetcsv($handle);
        // Header line counts as one, so subtract one from content lines if counting accurate
        if ($totalLines > 0) {
            $totalLines--;
        }

        $progressBar = $this->command->getOutput()->createProgressBar($totalLines > 0 ? $totalLines : 0);
        $this->configureProgressBar($progressBar);
        $progressBar->start();

        $chunk = [];
        $count = 0;

        while (($data = fgetcsv($handle)) !== false) {
            if (count($header) !== count($data)) {
                continue;
            }

            $row = array_combine($header, $data);
            $chunk[] = $row;
            $count++;

            $progressBar->advance();

            if ($count >= $chunkSize) {
                $callback($chunk);
                $chunk = [];
                $count = 0;
            }

            if ($limit > 0 && $progressBar->getProgress() >= $limit) {
                break;
            }
        }

        if (! empty($chunk)) {
            $callback($chunk);
        }

        $progressBar->finish();
        fclose($handle);
        $this->newLine();
    }
}
