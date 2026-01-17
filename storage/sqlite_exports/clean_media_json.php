<?php

/**
 * Clean malformed JSON in media.csv
 * Fixes double-escaped quotes and re-encodes JSON properly
 */

$inputFile = __DIR__ . '/media.csv';
$outputFile = __DIR__ . '/media_CLEANED.csv';
$backupFile = __DIR__ . '/media_BACKUP.csv';

echo "Starting media.csv JSON cleanup...\n";

// Backup original file
if (!copy($inputFile, $backupFile)) {
    die("ERROR: Could not create backup file\n");
}
echo "✓ Created backup: media_BACKUP.csv\n";

// Open files
$input = fopen($inputFile, 'r');
$output = fopen($outputFile, 'w');

if (!$input || !$output) {
    die("ERROR: Could not open files\n");
}

// Read header
$headers = fgetcsv($input);
fputcsv($output, $headers);

// Find JSON column indices
$jsonColumns = ['manipulations', 'custom_properties', 'generated_conversions', 'responsive_images'];
$jsonIndices = [];

foreach ($headers as $index => $header) {
    if (in_array($header, $jsonColumns)) {
        $jsonIndices[$index] = $header;
    }
}

echo "✓ Found JSON columns: " . implode(', ', $jsonIndices) . "\n";

$processed = 0;
$fixed = 0;
$errors = 0;

while (($row = fgetcsv($input)) !== false) {
    $processed++;
    $rowFixed = false;
    
    foreach ($jsonIndices as $index => $columnName) {
        if (isset($row[$index]) && !empty($row[$index])) {
            $original = $row[$index];
            
            // Fix double-escaped quotes from CSV export
            $fixed = str_replace('""', '"', $original);
            
            // Try to decode and re-encode
            $decoded = json_decode($fixed, true);
            
            if (json_last_error() === JSON_ERROR_NONE) {
                $row[$index] = json_encode($decoded, JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE);
                if ($row[$index] !== $original) {
                    $rowFixed = true;
                }
            } else {
                // Log error but continue
                if ($errors < 10) { // Only show first 10 errors
                    echo "  WARNING: Row $processed, column $columnName - JSON decode failed: " . json_last_error_msg() . "\n";
                }
                $errors++;
                $row[$index] = $fixed; // Use partially fixed version
            }
        }
    }
    
    if ($rowFixed) {
        $fixed++;
    }
    
    fputcsv($output, $row);
    
    if ($processed % 1000 === 0) {
        echo "  Processed $processed rows...\n";
    }
}

fclose($input);
fclose($output);

echo "\n✓ Cleanup complete!\n";
echo "  Total rows: $processed\n";
echo "  Rows with fixes: $fixed\n";
if ($errors > 0) {
    echo "  Rows with errors: $errors\n";
}

// Replace original with cleaned version
if (rename($outputFile, $inputFile)) {
    echo "\n✓ Replaced media.csv with cleaned version\n";
    echo "  Original backed up to: media_BACKUP.csv\n";
} else {
    echo "\n✓ Cleaned file saved as: media_CLEANED.csv\n";
    echo "  You can manually replace media.csv if needed\n";
}

echo "\nDone!\n";
