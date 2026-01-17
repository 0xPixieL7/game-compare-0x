<?php

/**
 * CSV Cleaning Script
 * Removes malformed rows from transformed CSVs
 */

declare(strict_types=1);

function cleanCsv(string $inputFile, string $outputFile, int $expectedColumnCount): int
{
    $validRows = 0;
    $skippedRows = 0;

    $input = fopen($inputFile, 'r');
    $output = fopen($outputFile, 'w');

    // Copy header
    $headers = fgetcsv($input, 0, ',', '"', '\\');
    fputcsv($output, $headers, ',', '"', '\\');

    // Process rows
    while (($row = fgetcsv($input, 0, ',', '"', '\\')) !== false) {
        if (count($row) === $expectedColumnCount) {
            fputcsv($output, $row, ',', '"', '\\');
            $validRows++;
        } else {
            $skippedRows++;
            echo "Skipped row with " . count($row) . " columns (expected $expectedColumnCount)\n";
        }
    }

    fclose($input);
    fclose($output);

    echo "✅ $inputFile: $validRows valid rows, $skippedRows skipped\n";

    return $validRows;
}

echo "🧹 Cleaning transformed CSVs...\n\n";

$basePath = __DIR__;

// Clean products (9 columns)
cleanCsv(
    "$basePath/products_TRANSFORMED.csv",
    "$basePath/products_CLEAN.csv",
    9
);

// Get valid product IDs
$validProductIds = [];
$handle = fopen("$basePath/products_CLEAN.csv", 'r');
fgetcsv($handle, 0, ',', '"', '\\'); // Skip header
while (($row = fgetcsv($handle, 0, ',', '"', '\\')) !== false) {
    $validProductIds[(int)$row[0]] = true; // id is first column
}
fclose($handle);

echo "✅ Found " . count($validProductIds) . " valid product IDs\n";

// Clean video_game_titles (8 columns) AND remove orphaned rows
$input = fopen("$basePath/video_game_titles_TRANSFORMED.csv", 'r');
$output = fopen("$basePath/video_game_titles_CLEAN.csv", 'w');

$headers = fgetcsv($input, 0, ',', '"', '\\');
fputcsv($output, $headers, ',', '"', '\\');

// Find product_id column index
$productIdIndex = array_search('product_id', $headers);

$validRows = 0;
$skippedOrphans = 0;
$skippedMalformed = 0;

while (($row = fgetcsv($input, 0, ',', '"', '\\')) !== false) {
    if (count($row) !== 8) {
        $skippedMalformed++;
        continue;
    }

    $productId = (int)$row[$productIdIndex];
    if (!isset($validProductIds[$productId])) {
        $skippedOrphans++;
        continue;
    }

    fputcsv($output, $row, ',', '"', '\\');
    $validRows++;
}

fclose($input);
fclose($output);

echo "✅ video_game_titles_TRANSFORMED.csv: $validRows valid rows, $skippedMalformed malformed, $skippedOrphans orphaned\n";

// Replace originals with cleaned versions
rename("$basePath/products_CLEAN.csv", "$basePath/products_TRANSFORMED.csv");
rename("$basePath/video_game_titles_CLEAN.csv", "$basePath/video_game_titles_TRANSFORMED.csv");

echo "\n✅ Cleaning complete! Ready to import.\n";
