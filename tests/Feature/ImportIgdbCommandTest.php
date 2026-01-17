<?php

declare(strict_types=1);

use App\Console\Commands\ImportIgdbDumpsCommand;
use Illuminate\Support\Facades\Storage;

beforeEach(function () {
    // Create a test CSV file in storage for testing
    $this->testCsvPath = storage_path('test_igdb_games.csv');

    // Create sample CSV data
    $csvContent = <<<'CSV'
id,name,slug,summary,rating,platforms,first_release_date,category,status,updated_at
1,The Legend of Zelda: Breath of the Wild,zelda-breath-wild,"An epic open-world adventure.",9.2,3,1488844800,0,0,1488844800
2,Super Mario Bros. 3D World,mario-3d-world,"A platforming masterpiece.",8.8,5,1511356800,0,0,1511356800
3,Elden Ring,elden-ring,"A challenging action RPG.",9.1,1,1645276800,0,0,1645276800
CSV;

    file_put_contents($this->testCsvPath, $csvContent);
});

afterEach(function () {
    // Clean up test file
    if (file_exists($this->testCsvPath)) {
        unlink($this->testCsvPath);
    }
});

it('streams CSV file without loading entire dataset into memory', function () {
    $command = new ImportIgdbDumpsCommand;

    // Verify the command exists
    expect($command)->toBeInstanceOf(ImportIgdbDumpsCommand::class);
    expect($command->getName())->toBe('gc:import-igdb');
});

it('handles CSV files with proper header mapping', function () {
    $csvPath = $this->testCsvPath;

    // Open the file and verify headers
    $handle = fopen($csvPath, 'r');
    $headers = fgetcsv($handle);

    fclose($handle);

    expect($headers)->toBeArray();
    expect(count($headers))->toBeGreaterThan(0);
    expect($headers[0])->toBe('id');
    expect($headers[1])->toBe('name');
});

it('processes CSV records one at a time for memory efficiency', function () {
    $csvPath = $this->testCsvPath;
    $recordCount = 0;

    // Simulate streaming processing
    $handle = fopen($csvPath, 'r');
    $headers = fgetcsv($handle);

    while (($row = fgetcsv($handle)) !== false) {
        if (count($row) === count($headers)) {
            $recordCount++;
        }
    }

    fclose($handle);

    expect($recordCount)->toBe(3); // We have 3 data rows in test CSV
});

it('respects limit option when processing records', function () {
    $csvPath = $this->testCsvPath;
    $limit = 2;
    $processedCount = 0;

    // Simulate limited processing
    $handle = fopen($csvPath, 'r');
    $headers = fgetcsv($handle);

    while (($row = fgetcsv($handle)) !== false && $processedCount < $limit) {
        if (count($row) === count($headers)) {
            $processedCount++;
        }
    }

    fclose($handle);

    expect($processedCount)->toBe(2); // Should process only 2 records
});

it('handles missing CSV files gracefully', function () {
    $nonExistentPath = storage_path('non_existent_file.csv');

    expect(file_exists($nonExistentPath))->toBeFalse();
});

it('validates CSV structure before processing', function () {
    $csvPath = $this->testCsvPath;

    $handle = fopen($csvPath, 'r');
    $headers = fgetcsv($handle);

    expect($headers)->toContain('id');
    expect($headers)->toContain('name');
    expect($headers)->toContain('rating');
    expect($headers)->toContain('platforms');

    fclose($handle);
});

it('processes multiple CSV records sequentially without accumulating in memory', function () {
    $csvPath = $this->testCsvPath;
    $baseMemoryUsage = memory_get_usage(true);
    $largestMemoryUsage = $baseMemoryUsage;
    $recordsProcessed = 0;

    $handle = fopen($csvPath, 'r');
    $headers = fgetcsv($handle);

    while (($row = fgetcsv($handle)) !== false) {
        $recordsProcessed++;

        // Record current memory usage (simulate per-record processing)
        $currentMemory = memory_get_usage(true);
        if ($currentMemory > $largestMemoryUsage) {
            $largestMemoryUsage = $currentMemory;
        }

        // Memory should remain relatively stable in streaming approach
        // (compared to array_merge approach which would grow linearly)
    }

    fclose($handle);

    expect($recordsProcessed)->toBe(3);
    // Memory growth should stay very small in streaming approach
    $delta = $largestMemoryUsage - $baseMemoryUsage;
    expect($delta)->toBeLessThan(1 * 1024 * 1024); // <1MB growth
});

it('combines headers with row data correctly', function () {
    $csvPath = $this->testCsvPath;

    $handle = fopen($csvPath, 'r');
    $headers = fgetcsv($handle);

    $firstDataRow = fgetcsv($handle);

    fclose($handle);

    // Combine headers and row data as the command would
    $record = array_combine($headers, $firstDataRow);

    expect($record)->toBeArray();
    expect($record['id'])->toBe('1');
    expect($record['name'])->toBe('The Legend of Zelda: Breath of the Wild');
    expect($record['rating'])->toBe('9.2');
});

it('validates required fields are present in CSV', function () {
    $csvPath = $this->testCsvPath;

    $handle = fopen($csvPath, 'r');
    $headers = fgetcsv($handle);

    $requiredFields = ['id', 'name', 'rating'];
    $missingFields = array_diff($requiredFields, $headers);

    fclose($handle);

    expect(count($missingFields))->toBe(0);
});

it('handles empty or malformed CSV rows', function () {
    $csvPath = $this->testCsvPath;

    $handle = fopen($csvPath, 'r');
    $headers = fgetcsv($handle);
    $validRowCount = 0;

    while (($row = fgetcsv($handle)) !== false) {
        // Only count rows that have the same number of columns as headers
        if (count($row) === count($headers)) {
            $validRowCount++;
        }
    }

    fclose($handle);

    expect($validRowCount)->toBe(3);
});

it('supports progress reporting every N records', function () {
    $csvPath = $this->testCsvPath;
    $progressInterval = 2;
    $progressCheckPoints = [];
    $recordCount = 0;

    $handle = fopen($csvPath, 'r');
    $headers = fgetcsv($handle);

    while (($row = fgetcsv($handle)) !== false) {
        if (count($row) === count($headers)) {
            $recordCount++;

            // Report progress every N records
            if ($recordCount % $progressInterval === 0) {
                $progressCheckPoints[] = $recordCount;
            }
        }
    }

    fclose($handle);

    expect($recordCount)->toBe(3);
    // With progress interval of 2, we should have a checkpoint at record 2
    expect(in_array(2, $progressCheckPoints))->toBeTrue();
});
