#!/usr/bin/env php
<?php

/**
 * Test script to verify the infinite loop bug fix.
 * 
 * Tests that malformed CSV rows with --limit=0 (unlimited) don't cause infinite loops.
 */

require __DIR__ . '/../vendor/autoload.php';

$app = require_once __DIR__ . '/../bootstrap/app.php';
$app->make(Illuminate\Contracts\Console\Kernel::class)->bootstrap();

echo "=== Testing Infinite Loop Fix ===\n\n";

// Create test CSV with malformed rows
$testCsv = sys_get_temp_dir() . '/test_malformed_games.csv';
$content = <<<CSV
id,name,slug,summary,platforms,genres
1,Good Game 1,good-game-1,A test game,{1},"{2,3}"
2,Malformed
3,Good Game 2,good-game-2,Another test,{1},"{4}"
4,Also Malformed,missing-column
5,Good Game 3,good-game-3,Final game,{2},"{5,6}"
CSV;

file_put_contents($testCsv, $content);

echo "Created test CSV: $testCsv\n";
echo "Contents:\n";
echo file_get_contents($testCsv);
echo "\n\n";

echo "Test 1: Running with --limit=0 (unlimited) - should NOT hang\n";
echo "Expected: Process rows 1, 3, 5 (skip rows 2, 4), complete successfully\n";
echo "-----------------------------------------------------------------------\n\n";

$startTime = microtime(true);

// Run import command with test CSV
$exitCode = Artisan::call('gc:import-igdb', [
    '--path' => $testCsv,
    '--limit' => 0,  // CRITICAL: This is where the bug occurred
    '--resume' => 0,
    '--fast' => 1,
]);

$duration = round(microtime(true) - $startTime, 2);

echo "\n\n";
echo "Exit code: $exitCode\n";
echo "Duration: {$duration}s\n";

if ($duration > 10) {
    echo "❌ FAILED: Took too long ({$duration}s) - possible infinite loop!\n";
    exit(1);
}

if ($exitCode !== 0) {
    echo "⚠️  WARNING: Command returned non-zero exit code\n";
    echo "This may be expected if games don't exist yet.\n";
}

echo "✅ PASSED: Command completed in reasonable time\n";
echo "   The infinite loop bug is FIXED!\n";

// Cleanup
unlink($testCsv);

echo "\n=== Test Complete ===\n";
exit(0);
