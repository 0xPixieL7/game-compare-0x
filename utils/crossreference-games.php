<?php

require __DIR__.'/vendor/autoload.php';

$app = require_once __DIR__.'/bootstrap/app.php';
$kernel = $app->make(Illuminate\Contracts\Console\Kernel::class);
$kernel->bootstrap();

use Illuminate\Support\Facades\DB;

echo "Game Crossreference Tool (Optimized)\n";
echo str_repeat('=', 80)."\n\n";

// Normalize game names for matching
function normalizeGameName($name)
{
    $name = strtolower($name);
    $name = preg_replace('/[^a-z0-9]+/', ' ', $name);
    $name = trim($name);

    return $name;
}

// Load IGDB games from database first (smaller dataset)
echo "Loading IGDB games from database...\n";
$igdbGames = DB::table('video_game_title_sources')
    ->join('video_game_titles', 'video_game_title_sources.video_game_title_id', '=', 'video_game_titles.id')
    ->select([
        'video_game_titles.id',
        'video_game_titles.name',
        'video_game_title_sources.platform',
        'video_game_title_sources.slug',
        'video_game_title_sources.external_id',
    ])
    ->get();

echo '  Loaded '.number_format($igdbGames->count())." games from IGDB\n\n";

// Create lookup index
echo "Building IGDB lookup index...\n";
$igdbIndex = [];
foreach ($igdbGames as $game) {
    $normalized = normalizeGameName($game->name);
    if (! isset($igdbIndex[$normalized])) {
        $igdbIndex[$normalized] = [];
    }
    $igdbIndex[$normalized][] = $game;
}
echo '  Indexed '.number_format(count($igdbIndex))." unique IGDB game names\n\n";

// Now stream through Price Charting CSV and match
$csvPath = '/Users/lowkey/Desktop/game-compare/storage/price-charting/price-guide-from-price-charting.csv';
echo "Processing Price Charting CSV...\n";

$exactMatches = [];
$noMatch = 0;
$csvCount = 0;

$handle = fopen($csvPath, 'r');
$header = fgetcsv($handle); // Skip header

$startTime = microtime(true);

while (($row = fgetcsv($handle)) !== false) {
    $csvCount++;

    if ($csvCount % 10000 === 0) {
        $elapsed = microtime(true) - $startTime;
        $rate = $csvCount / $elapsed;
        $remaining = (115518 - $csvCount) / $rate;
        echo sprintf(
            "  Processed %s games (%.0f games/s, ~%.0fs remaining)...\r",
            number_format($csvCount),
            $rate,
            $remaining
        );
    }

    $pcGame = [
        'id' => $row[0],
        'console' => $row[1],
        'name' => $row[2],
        'price' => $row[3],
    ];

    $normalized = normalizeGameName($pcGame['name']);

    // Check for exact match only (no fuzzy matching for speed)
    if (isset($igdbIndex[$normalized])) {
        $exactMatches[] = [
            'pc_id' => $pcGame['id'],
            'pc_name' => $pcGame['name'],
            'pc_console' => $pcGame['console'],
            'pc_price' => $pcGame['price'],
            'igdb_matches' => $igdbIndex[$normalized],
        ];
    } else {
        $noMatch++;
    }
}
fclose($handle);

echo "\n\n";

// Report results
echo "Crossreference Results\n";
echo str_repeat('=', 80)."\n\n";

echo 'Total Price Charting Games: '.number_format($csvCount)."\n";
echo 'Total IGDB Games: '.number_format($igdbGames->count())."\n";
echo 'Unique IGDB Game Names: '.number_format(count($igdbIndex))."\n\n";

echo 'Exact Matches: '.number_format(count($exactMatches)).' ('.round(count($exactMatches) / $csvCount * 100, 2)."%)\n";
echo 'No Match: '.number_format($noMatch).' ('.round($noMatch / $csvCount * 100, 2)."%)\n\n";

// Calculate total match pairs (some PC games match multiple IGDB games)
$totalMatchPairs = 0;
foreach ($exactMatches as $match) {
    $totalMatchPairs += count($match['igdb_matches']);
}
echo 'Total Match Pairs (PC → IGDB): '.number_format($totalMatchPairs)."\n\n";

// Show sample exact matches
echo "\nSample Exact Matches (first 20):\n";
echo str_repeat('-', 80)."\n";
foreach (array_slice($exactMatches, 0, 20) as $i => $match) {
    $num = $i + 1;
    $igdbCount = count($match['igdb_matches']);
    echo "{$num}. {$match['pc_name']} [{$match['pc_console']}] - Price: {$match['pc_price']}\n";
    echo "   → Matched with {$igdbCount} IGDB game(s):\n";
    foreach ($match['igdb_matches'] as $igdb) {
        $platform = json_decode($igdb->platform ?? '[]', true);
        $platformStr = is_array($platform) ? implode(', ', array_slice($platform, 0, 3)) : 'N/A';
        echo "      - {$igdb->name} (Platform: {$platformStr})\n";
    }
    echo "\n";
}

// Analyze platform distribution
echo "\nTop Consoles in Matched Games:\n";
echo str_repeat('-', 80)."\n";
$consoleStats = [];
foreach ($exactMatches as $match) {
    $console = $match['pc_console'];
    if (! isset($consoleStats[$console])) {
        $consoleStats[$console] = 0;
    }
    $consoleStats[$console]++;
}
arsort($consoleStats);

foreach (array_slice($consoleStats, 0, 15) as $console => $count) {
    echo sprintf("  %-30s %s games (%.1f%%)\n",
        $console,
        number_format($count),
        $count / count($exactMatches) * 100
    );
}

// Export results to CSV
$outputPath = '/Users/lowkey/Desktop/game-compare/storage/crossreference-results.csv';
echo "\n\nExporting results to CSV: {$outputPath}\n";

$output = fopen($outputPath, 'w');
fputcsv($output, [
    'pc_id',
    'pc_name',
    'pc_console',
    'pc_price',
    'igdb_id',
    'igdb_name',
    'igdb_platform',
    'igdb_slug',
    'igdb_external_id',
]);

foreach ($exactMatches as $match) {
    foreach ($match['igdb_matches'] as $igdb) {
        $platform = json_decode($igdb->platform ?? '[]', true);
        $platformStr = is_array($platform) ? implode('|', $platform) : '';

        fputcsv($output, [
            $match['pc_id'],
            $match['pc_name'],
            $match['pc_console'],
            $match['pc_price'],
            $igdb->id,
            $igdb->name,
            $platformStr,
            $igdb->slug,
            $igdb->external_id,
        ]);
    }
}

fclose($output);

echo "✓ Results exported successfully!\n\n";

// Summary statistics
echo "Summary Statistics\n";
echo str_repeat('=', 80)."\n";
$matchRate = count($exactMatches) / $csvCount * 100;
$reverseRate = count($exactMatches) / count($igdbIndex) * 100;

echo 'Forward Coverage: '.round($matchRate, 2)."% of Price Charting games have exact matches in IGDB\n";
echo '  → '.number_format(count($exactMatches)).' matched / '.number_format($csvCount)." total\n";
echo '  → '.number_format($noMatch)." Price Charting games NOT in IGDB\n\n";

echo 'Reverse Coverage: '.round($reverseRate, 2)."% of IGDB games have matches in Price Charting\n";
echo '  → '.number_format(count($exactMatches)).' matched / '.number_format(count($igdbIndex))." unique IGDB names\n\n";

echo "Match Quality:\n";
echo '  → Average matches per PC game: '.round($totalMatchPairs / count($exactMatches), 2)." IGDB games\n";

// Find games with multiple IGDB matches
$multiMatch = array_filter($exactMatches, fn ($m) => count($m['igdb_matches']) > 1);
echo '  → PC games matching multiple IGDB entries: '.number_format(count($multiMatch)).' ('.round(count($multiMatch) / count($exactMatches) * 100, 2)."%)\n\n";

$elapsed = microtime(true) - $startTime;
echo 'Total processing time: '.round($elapsed, 2)."s\n";
