<?php

declare(strict_types=1);

namespace App\Services;

use Illuminate\Support\Facades\File;
use Symfony\Component\Console\Helper\ProgressBar;
use Symfony\Component\Console\Output\ConsoleOutput;

class DumpDataLoader
{
    private static ?array $gameNames = null;

    public static function getGameName(): string
    {
        if (self::$gameNames === null) {
            self::loadGames();
        }

        if (empty(self::$gameNames)) {
            return fake()->words(2, true);
        }

        return self::$gameNames[array_rand(self::$gameNames)];
    }

    private static function loadGames(): void
    {
        $igdbPath = storage_path('igdb-dumps/1767852000_games.csv');
        $gbPath = base_path('giant_bomb_games_detailed.json');
        $nexardaPath = base_path('nexarda_product_catalogue.json');

        if (File::exists($igdbPath)) {
            self::loadFromIgdbCsv($igdbPath);
            return;
        }

        // Fallbacks
        if (File::exists($gbPath)) {
            self::loadFromGiantBombJson($gbPath);
            return;
        }

        if (File::exists($nexardaPath)) {
            self::loadFromNexardaJson($nexardaPath);
            return;
        }

        self::$gameNames = [];
    }

    private static function loadFromIgdbCsv(string $path): void
    {
        $handle = fopen($path, 'r');
        if ($handle === false) {
            self::$gameNames = [];
            return;
        }

        // Count lines for progress bar (rough estimate or `wc -l`)
        $totalLines = 0;
        if (app()->runningInConsole()) {
            $totalLines = (int) exec("wc -l " . escapeshellarg($path));
        }

        $headers = fgetcsv($handle);
        $nameIndex = array_search('name', $headers);
        
        if ($nameIndex === false) {
            // Default to 1 if header missing or mismatch
            $nameIndex = 1; 
        }

        $names = [];
        $count = 0;

        $output = null;
        $progressBar = null;

        if (app()->runningInConsole()) {
            $output = new ConsoleOutput();
            $output->writeln("<info>Loading games from IGDB dump...</info>");
            $progressBar = new ProgressBar($output, $totalLines);
            $progressBar->start();
        }

        while (($data = fgetcsv($handle)) !== false) {
            if (isset($data[$nameIndex])) {
                $names[] = $data[$nameIndex];
            }
            
            $count++;
            if ($progressBar && $count % 1000 === 0) {
                $progressBar->advance(1000);
            }
        }

        if ($progressBar) {
            $progressBar->finish();
            $output->writeln(""); // Newline
        }

        fclose($handle);
        self::$gameNames = $names;
    }

    private static function loadFromGiantBombJson(string $path): void
    {
        // Simple implementation for fallback
        $json = json_decode(File::get($path), true);
        $names = [];
        if (is_array($json)) {
             // Structure depends on dump, assuming typical list
             foreach ($json as $item) {
                 if (isset($item['name'])) {
                     $names[] = $item['name'];
                 }
             }
        }
        self::$gameNames = $names;
    }

    private static function loadFromNexardaJson(string $path): void
    {
        $json = json_decode(File::get($path), true);
        $names = [];
        if (is_array($json)) {
             foreach ($json as $item) {
                 if (isset($item['name'])) {
                     $names[] = $item['name'];
                 }
             }
        }
        self::$gameNames = $names;
    }
}
