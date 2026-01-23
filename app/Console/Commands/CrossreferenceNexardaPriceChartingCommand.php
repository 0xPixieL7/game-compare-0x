<?php

declare(strict_types=1);

namespace App\Console\Commands;

use App\Models\VideoGame;
use Illuminate\Console\Command;
use Illuminate\Support\Str;

class CrossreferenceNexardaPriceChartingCommand extends Command
{
    protected $signature = 'crossreference:nexarda-price-charting
                            {--nexarda=rust/nexarda_product_catalogue.json.ndjson}
                            {--pricecharting=storage/price-charting/price-guide-from-price-charting.csv}
                            {--output=storage/crossreference-nexarda-pricecharting.csv}';

    protected $description = 'Crossreference Nexarda NDJSON, PriceCharting CSV, and DB for all currencies, filling missing media from DB.';

    public function handle(): int
    {
        $nexardaRel = $this->option('nexarda');
        if (empty($nexardaRel) || is_bool($nexardaRel)) {
            $nexardaRel = 'rust/nexarda_product_catalogue.json.ndjson';
        }

        $priceChartingRel = $this->option('pricecharting');
        if (empty($priceChartingRel) || is_bool($priceChartingRel)) {
            $priceChartingRel = 'storage/price-charting/price-guide-from-price-charting.csv';
        }

        $outputRel = $this->option('output');
        if (empty($outputRel) || is_bool($outputRel)) {
            $outputRel = 'storage/crossreference-nexarda-pricecharting.csv';
        }

        $nexardaPath = base_path($nexardaRel);
        $priceChartingPath = base_path($priceChartingRel);
        $outputPath = base_path($outputRel);

        if (! is_file($nexardaPath)) {
            $this->error("Nexarda file not found at: {$nexardaPath}");

            return self::FAILURE;
        }

        if (! is_file($priceChartingPath)) {
            $this->error("PriceCharting file not found at: {$priceChartingPath}");

            return self::FAILURE;
        }

        $this->info('Loading Nexarda NDJSON...');
        $nexardaGames = $this->loadNexarda($nexardaPath);
        $this->info('Loading PriceCharting CSV...');
        $priceChartingGames = $this->loadPriceCharting($priceChartingPath);
        $this->info('Loading DB games (this might take a few minutes)...');
        $dbGames = $this->loadDbGames();

        $allSlugs = array_unique(array_merge(
            array_keys($nexardaGames),
            array_keys($priceChartingGames),
            array_keys($dbGames)
        ));

        $rows = [];
        foreach ($allSlugs as $slug) {
            $nexarda = $nexardaGames[$slug] ?? null;
            $priceCharting = $priceChartingGames[$slug] ?? null;
            $db = $dbGames[$slug] ?? null;

            $name = $nexarda['name'] ?? $priceCharting['name'] ?? $db['name'] ?? $slug;
            $media = $priceCharting['media'] ?? $db['media'] ?? null;

            $currencies = array_unique(array_merge(
                isset($nexarda['prices']) ? array_keys($nexarda['prices']) : [],
                isset($priceCharting['prices']) ? array_keys($priceCharting['prices']) : [],
                isset($db['prices']) ? array_keys($db['prices']) : []
            ));

            foreach ($currencies as $currency) {
                $row = [
                    'slug' => $slug,
                    'name' => $name,
                    'currency' => $currency,
                    'nexarda_price' => $nexarda['prices'][$currency] ?? null,
                    'pricecharting_price' => $priceCharting['prices'][$currency] ?? null,
                    'db_price' => $db['prices'][$currency] ?? null,
                    'media' => $media,
                ];
                $rows[] = $row;
            }
        }

        $this->info('Writing output CSV...');
        $this->writeCsv($outputPath, $rows);
        $this->info('Done. Output: '.$outputPath);

        return self::SUCCESS;
    }

    private function loadNexarda(string $path): array
    {
        $games = [];
        $handle = fopen($path, 'r');
        if (! $handle) {
            return [];
        }

        while (($line = fgets($handle)) !== false) {
            $line = trim($line);
            if (empty($line)) {
                continue;
            }

            $row = json_decode($line, true);
            if (! isset($row['slug'])) {
                continue;
            }
            $games[$row['slug']] = [
                'name' => $row['name'],
                'prices' => $row['prices'],
            ];
        }
        fclose($handle);

        return $games;
    }

    private function loadPriceCharting(string $path): array
    {
        $games = [];
        $handle = fopen($path, 'r');
        $header = fgetcsv($handle);
        while (($data = fgetcsv($handle)) !== false) {
            $row = array_combine($header, $data);
            if (! isset($row['product-name'])) {
                continue;
            }
            $slug = Str::slug($row['product-name']);
            $prices = [];
            // Try to extract price columns (e.g., loose-price, cib-price, new-price, or price_{CUR})
            foreach ($row as $col => $val) {
                if (preg_match('/^(loose|cib|new)?-?price(_([A-Z]{3}))?$/', $col, $m)) {
                    $cur = $m[3] ?? 'USD';
                    $prices[$cur] = is_numeric($val) ? (float) $val : (float) str_replace(['$', ','], '', $val);
                }
            }
            $games[$slug] = [
                'name' => $row['product-name'],
                'prices' => $prices,
                'media' => $row['media'] ?? null,
            ];
        }
        fclose($handle);

        return $games;
    }

    private function loadDbGames(): array
    {
        $games = [];

        // Ensure we don't timeout on the large select
        if (config('database.default') === 'pgsql') {
            \DB::statement("SET statement_timeout = '600s'"); // 10 minutes
        }

        // Using cursor and only selecting necessary columns to avoid memory bloat and timeouts
        // We select 'source_payload' to be empty but we select id, slug, name
        // Relationships are also constrained to only select needed columns
        $query = VideoGame::select(['id', 'slug', 'name'])
            ->with([
                'prices' => function ($q) {
                    $q->select(['id', 'video_game_id', 'currency', 'amount_minor']);
                },
                'images' => function ($q) {
                    $q->select(['id', 'video_game_id', 'url']);
                },
            ]);

        foreach ($query->cursor() as $game) {
            $slug = $game->slug;
            if (! $slug) {
                continue;
            }

            $prices = [];
            foreach ($game->prices as $price) {
                $prices[$price->currency] = $price->amount_minor / 100;
            }
            $games[$slug] = [
                'name' => $game->name,
                'prices' => $prices,
                'media' => $game->images->url ?? null,
            ];
        }

        return $games;
    }

    private function writeCsv(string $path, array $rows): void
    {
        $fp = fopen($path, 'w');
        fputcsv($fp, ['slug', 'name', 'currency', 'nexarda_price', 'pricecharting_price', 'db_price', 'media']);
        foreach ($rows as $row) {
            fputcsv($fp, $row);
        }
        fclose($fp);
    }
}
