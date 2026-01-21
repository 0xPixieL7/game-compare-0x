<?php

declare(strict_types=1);

namespace App\Console\Commands;

use App\Models\VideoGame;
use Illuminate\Console\Command;
use Illuminate\Support\Str;
use League\Csv\Reader;
use League\Csv\Statement;

class ImportCrossreferenceResultsCommand extends Command
{
    protected $signature = 'import:crossreference-results {--csv=storage/crossreference-results.csv} {--giantbomb=giant_bomb_games_detailed.json} {--nexarda=nexarda_product_catalogue.json}';

    protected $description = 'Import crossreference-results.csv into video_game_prices, mapping sources, source_ids, prices, currencies, and cross-referencing media.';

    public function handle(): int
    {
        $csvPath = $this->option('csv');
        $giantBombPath = $this->option('giantbomb');
        $nexardaPath = $this->option('nexarda');

        if (! file_exists($csvPath)) {
            $this->error("CSV file not found: $csvPath");

            return 1;
        }

        $csv = Reader::createFromPath($csvPath);
        $csv->setHeaderOffset(0);
        $records = iterator_to_array((new Statement)->process($csv));

        $giantBomb = $this->loadJson($giantBombPath);
        $nexarda = $this->loadJson($nexardaPath);

        $chunkSize = 500;
        $chunks = array_chunk($records, $chunkSize);
        $total = count($records);

        $this->info('Dispatching '.count($chunks)." chunks of size $chunkSize (total: $total rows)...");

        $jobs = [];
        foreach ($chunks as $chunk) {
            $jobs[] = new \App\Jobs\ImportCrossreferenceChunkJob($chunk, $giantBomb, $nexarda);
        }
        \Bus::batch($jobs)
            ->then(function () use ($total) {
                $this->info("All chunks processed. Imported $total rows.");
            })
            ->catch(function (\Throwable $e) {
                $this->error('Batch failed: '.$e->getMessage());
            })
            ->dispatch();

        $this->info('Import jobs dispatched. Monitor your queue for progress.');

        return 0;
    }

    protected function parsePrice(string $price): int
    {
        // Remove $ and convert to cents
        $price = str_replace(['$', ','], '', $price);

        return (int) round(floatval($price) * 100);
    }

    protected function loadJson(string $path): array
    {
        if (! file_exists($path)) {
            return [];
        }
        $json = file_get_contents($path);

        return json_decode($json, true) ?? [];
    }

    protected function resolveVideoGameId(array $row): ?int
    {
        // Try IGDB, then PriceCharting, then Nexarda
        if (! empty($row['igdb_id'])) {
            $game = VideoGame::where('external_id', $row['igdb_id'])->first();
            if ($game) {
                return $game->id;
            }
        }
        if (! empty($row['pc_id'])) {
            $game = VideoGame::where('external_id', $row['pc_id'])->first();
            if ($game) {
                return $game->id;
            }
        }
        if (! empty($row['igdb_external_id'])) {
            $game = VideoGame::where('external_id', $row['igdb_external_id'])->first();
            if ($game) {
                return $game->id;
            }
        }

        return null;
    }

    protected function crossReferenceMedia(array $row, array $giantBomb, array $nexarda): array
    {
        $media = [];
        // GiantBomb lookup by igdb_slug or name
        if (! empty($row['igdb_slug']) && isset($giantBomb[$row['igdb_slug']])) {
            $media['giantbomb'] = $giantBomb[$row['igdb_slug']];
        } elseif (! empty($row['igdb_name'])) {
            foreach ($giantBomb as $slug => $data) {
                if (Str::lower($data['name'] ?? '') === Str::lower($row['igdb_name'])) {
                    $media['giantbomb'] = $data;
                    break;
                }
            }
        }
        // Nexarda lookup by external_id
        if (! empty($row['igdb_external_id']) && isset($nexarda[$row['igdb_external_id']])) {
            $media['nexarda'] = $nexarda[$row['igdb_external_id']];
        }

        return $media;
    }
}
