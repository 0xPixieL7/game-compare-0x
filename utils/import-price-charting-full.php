<?php

require __DIR__.'/vendor/autoload.php';

$app = require_once __DIR__.'/bootstrap/app.php';
$kernel = $app->make(Illuminate\Contracts\Console\Kernel::class);
$kernel->bootstrap();

use Illuminate\Support\Facades\DB;

echo "Full Price Charting Import (115k games)\n";
echo str_repeat('=', 80)."\n\n";

// Regional mapping configuration
$regionalConfig = [
    'PAL' => ['currency' => 'EUR', 'region' => 'EU', 'country_code' => 'GB'],
    'Japan' => ['currency' => 'JPY', 'region' => 'JP', 'country_code' => 'JP'],
    'Japanese' => ['currency' => 'JPY', 'region' => 'JP', 'country_code' => 'JP'],
    'NTSC' => ['currency' => 'USD', 'region' => 'US', 'country_code' => 'US'],
    'EUR' => ['currency' => 'EUR', 'region' => 'EU', 'country_code' => 'EU'],
    'UK' => ['currency' => 'GBP', 'region' => 'GB', 'country_code' => 'GB'],
];

/**
 * Extract regional variant from game name
 */
function extractRegionalVariant(string $gameName): array
{
    global $regionalConfig;

    $cleanName = $gameName;
    $region = null;
    $currency = 'USD'; // Default to USD
    $countryCode = 'US'; // Default to US

    // Check for bracketed regional markers [PAL], [Japan], etc.
    if (preg_match('/\[(PAL|Japan|Japanese|NTSC|EUR|UK)\]/i', $gameName, $matches)) {
        $marker = $matches[1];
        $region = $regionalConfig[$marker] ?? null;
        if ($region) {
            $currency = $region['currency'];
            $countryCode = $region['country_code'];
        }
        // Remove the marker from name
        $cleanName = trim(preg_replace('/\s*\['.preg_quote($marker, '/').'\]\s*/i', '', $gameName));
    }

    return [
        'clean_name' => $cleanName,
        'currency' => $currency,
        'country_code' => $countryCode,
        'region_marker' => $region ? $matches[1] : null,
    ];
}

/**
 * Parse Price Charting price string to float
 */
function parsePriceCharting(string $priceStr): ?float
{
    $priceStr = trim($priceStr);
    if (empty($priceStr)) {
        return null;
    }

    $priceStr = str_replace(['$', ','], '', $priceStr);
    $price = floatval($priceStr);

    return $price > 0 ? $price : null;
}

// Step 1: Create or find Price Charting source
echo "Step 1: Setting up Price Charting source...\n";
$source = DB::table('video_game_sources')
    ->where('provider', 'price_charting')
    ->first();

if (! $source) {
    $sourceId = DB::table('video_game_sources')->insertGetId([
        'provider' => 'price_charting',
        'items_count' => 0,
        'metadata' => json_encode([
            'name' => 'Price Charting',
            'description' => 'Video game pricing data from PriceCharting.com',
            'url' => 'https://www.pricecharting.com',
        ]),
        'created_at' => now(),
        'updated_at' => now(),
    ]);
    echo "  ✓ Created Price Charting source (ID: {$sourceId})\n\n";
} else {
    $sourceId = $source->id;
    echo "  ✓ Found existing Price Charting source (ID: {$sourceId})\n\n";
}

// Step 2: Load full Price Charting CSV
echo "Step 2: Loading full Price Charting CSV (115k games)...\n";
$csvPath = '/Users/lowkey/Desktop/game-compare/storage/price-charting/price-guide-from-price-charting.csv';
$handle = fopen($csvPath, 'r');
$header = fgetcsv($handle); // id,console-name,product-name,loose-price

$allGames = [];
$lineNum = 0;

while (($row = fgetcsv($handle)) !== false) {
    $lineNum++;

    if (count($row) < 4) {
        continue; // Skip malformed rows
    }

    $allGames[] = [
        'pc_id' => $row[0],
        'console' => $row[1],
        'product_name' => $row[2],
        'loose_price' => $row[3],
    ];
}
fclose($handle);

echo '  ✓ Loaded '.number_format(count($allGames))." games from Price Charting\n\n";

// Step 3: Check existing entries
echo "Step 3: Checking for existing Price Charting entries...\n";
$existingPcIds = DB::table('video_game_title_sources')
    ->where('provider', 'price_charting')
    ->pluck('external_id')
    ->toArray();

echo '  ✓ Found '.number_format(count($existingPcIds))." existing entries\n\n";

// Step 4: Process games and group by clean name for variant detection
echo "Step 4: Processing games and detecting regional variants...\n";

$gamesByCleanName = [];
$regionalStats = [];

foreach ($allGames as $game) {
    $regional = extractRegionalVariant($game['product_name']);
    $cleanName = $regional['clean_name'];

    // Group by clean name for variant detection
    if (! isset($gamesByCleanName[$cleanName])) {
        $gamesByCleanName[$cleanName] = [];
    }

    $gamesByCleanName[$cleanName][] = array_merge($game, [
        'clean_name' => $cleanName,
        'currency' => $regional['currency'],
        'country_code' => $regional['country_code'],
        'region_marker' => $regional['region_marker'],
    ]);

    // Track regional stats
    if ($regional['region_marker']) {
        $regionalStats[$regional['region_marker']] = ($regionalStats[$regional['region_marker']] ?? 0) + 1;
    }
}

echo '  ✓ Processed '.number_format(count($allGames))." games\n";
echo '  ✓ Found '.number_format(count($gamesByCleanName))." unique game names\n";
echo "  ✓ Regional variants detected:\n";
foreach ($regionalStats as $marker => $count) {
    echo "     - {$marker}: ".number_format($count)." games\n";
}
echo "\n";

// Step 5: Prepare entries for import
echo "Step 5: Preparing entries for import...\n";

$newTitleSources = [];
$newPrices = [];
$skippedDuplicates = 0;
$skippedNoPrice = 0;
$gamesNeedingMedia = [];

foreach ($allGames as $game) {
    // Skip duplicates
    if (in_array($game['pc_id'], $existingPcIds)) {
        $skippedDuplicates++;

        continue;
    }

    $regional = extractRegionalVariant($game['product_name']);
    $price = parsePriceCharting($game['loose_price']);

    // Create title source entry (even without price - we'll backfill media later)
    $newTitleSources[] = [
        'video_game_title_id' => null, // Will need matching or creation
        'video_game_source_id' => $sourceId,
        'provider' => 'price_charting',
        'external_id' => $game['pc_id'],
        'slug' => null,
        'name' => $game['product_name'],
        'description' => null,
        'release_date' => null,
        'provider_item_id' => $game['pc_id'],
        'platform' => json_encode([$game['console']]),
        'rating' => null,
        'rating_count' => null,
        'developer' => null,
        'publisher' => null,
        'genre' => null,
        'raw_payload' => json_encode([
            'price_charting_id' => $game['pc_id'],
            'price_charting_name' => $game['product_name'],
            'price_charting_console' => $game['console'],
            'price_charting_price' => $game['loose_price'],
            'price_usd' => $price,
            'clean_name' => $regional['clean_name'],
            'region_marker' => $regional['region_marker'],
            'currency' => $regional['currency'],
            'country_code' => $regional['country_code'],
            'needs_media_backfill' => true,
            'import_source' => 'full_price_charting_import',
            'import_date' => now()->toDateTimeString(),
        ]),
        'created_at' => now(),
        'updated_at' => now(),
    ];

    // Track games needing media backfill
    $gamesNeedingMedia[] = [
        'pc_id' => $game['pc_id'],
        'name' => $regional['clean_name'],
        'console' => $game['console'],
    ];
}

echo '  ✓ Prepared '.number_format(count($newTitleSources))." new entries\n";
echo "  ⚠ Skipped {$skippedDuplicates} duplicates\n";
echo '  ℹ '.number_format(count($gamesNeedingMedia))." games need media backfill\n\n";

// Step 6: Import to video_game_title_sources
if (count($newTitleSources) > 0) {
    echo "Step 6: Importing to video_game_title_sources...\n";

    $batchSize = 500;
    $imported = 0;
    $batches = array_chunk($newTitleSources, $batchSize);

    foreach ($batches as $i => $batch) {
        DB::table('video_game_title_sources')->insert($batch);
        $imported += count($batch);
        $batchNum = $i + 1;
        $totalBatches = count($batches);
        echo "  Batch {$batchNum}/{$totalBatches} ({$imported}/".count($newTitleSources).")...\r";
    }

    echo "\n  ✓ Imported ".number_format($imported)." entries\n\n";

    // Update source metadata
    DB::table('video_game_sources')
        ->where('id', $sourceId)
        ->update([
            'items_count' => $imported + count($existingPcIds),
            'updated_at' => now(),
        ]);
} else {
    echo "Step 6: No new entries to import\n\n";
}

// Step 7: Save games needing media backfill to JSON
echo "Step 7: Saving media backfill queue...\n";
$mediaBackfillPath = '/Users/lowkey/Desktop/game-compare/storage/media-backfill-queue.json';
file_put_contents($mediaBackfillPath, json_encode([
    'generated_at' => now()->toDateTimeString(),
    'total_games' => count($gamesNeedingMedia),
    'games' => array_slice($gamesNeedingMedia, 0, 1000), // First 1000 for initial backfill
], JSON_PRETTY_PRINT));

echo '  ✓ Saved '.number_format(min(1000, count($gamesNeedingMedia)))." games to backfill queue\n";
echo "  ℹ File: {$mediaBackfillPath}\n\n";

// Summary
echo "Import Summary\n";
echo str_repeat('=', 80)."\n";
echo 'Total Price Charting Games: '.number_format(count($allGames))."\n";
echo 'New Entries Imported: '.number_format(count($newTitleSources))."\n";
echo 'Unique Game Names: '.number_format(count($gamesByCleanName))."\n";
echo 'Games Needing Media: '.number_format(count($gamesNeedingMedia))."\n";
echo "\nRegional Variants:\n";
foreach ($regionalStats as $marker => $count) {
    echo "  {$marker}: ".number_format($count)." games\n";
}
echo "\n✓ Full import complete!\n";
echo "\nNext Steps:\n";
echo "1. Run media backfill script to fetch covers from IGDB/RAWG/TGDB\n";
echo "2. Match games to existing IGDB titles where possible\n";
echo "3. Create video_game_titles for unmatched games\n";
