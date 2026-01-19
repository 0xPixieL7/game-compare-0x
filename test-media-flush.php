<?php

declare(strict_types=1);

require __DIR__.'/vendor/autoload.php';

$app = require_once __DIR__.'/bootstrap/app.php';
$app->make(\Illuminate\Contracts\Console\Kernel::class)->bootstrap();

use Illuminate\Support\Facades\DB;

echo "Testing media import with flush fix...\n\n";

// Get the command instance
$command = new \App\Console\Commands\ImportIgdbDumpsCommand;

// Mock the output interface
$output = new \Symfony\Component\Console\Output\ConsoleOutput;
$input = new \Symfony\Component\Console\Input\ArrayInput([]);

// Set up the command
$reflection = new ReflectionClass($command);
$outputProperty = $reflection->getProperty('output');
$outputProperty->setAccessible(true);
$outputProperty->setValue($command, $output);

$inputProperty = $reflection->getProperty('input');
$inputProperty->setAccessible(true);
$inputProperty->setValue($command, $input);

// Process covers
echo "Processing covers CSV...\n";
$startTime = microtime(true);

$method = $reflection->getMethod('processMediaIfPresent');
$method->setAccessible(true);

$addImageMethod = $reflection->getMethod('addImageMedia');
$addImageMethod->setAccessible(true);

$processed = $method->invoke($command, 'storage/igdb-dumps', 'covers', function ($g, $row) use ($command, $addImageMethod) {
    $addImageMethod->invoke($command, $g, $row, 'cover_images', true);
}, 'igdb');

$duration = round(microtime(true) - $startTime, 2);

echo "\n\nResults:\n";
echo "  CSV rows processed: $processed\n";
echo "  Duration: {$duration}s\n";

$dbCount = DB::table('images')
    ->where('imageable_type', \App\Models\VideoGame::class)
    ->whereJsonContains('collection_names', 'cover_images')
    ->count();

echo "  Covers in database: $dbCount\n";
echo "  Expected unique games: ~45,936\n";
echo '  Success rate: '.round(($dbCount / 45936) * 100, 1)."%\n";

if ($dbCount > 40000) {
    echo "\n✅ SUCCESS! Flush fix is working!\n";
} else {
    echo "\n❌ FAILED! Only saved $dbCount / 45936 covers\n";
}
