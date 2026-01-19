<?php

declare(strict_types=1);

namespace App\Services\Import\Concerns;

use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\File;
use Illuminate\Support\Facades\Log;

trait CanOptimizeImport
{
    /**
     * PostgreSQL tables to optimize.
     */
    protected array $optimizedTables = [
        'video_games',
        'video_game_titles',
        'video_game_title_sources',
        'products',
        'video_game_prices',
        'images',
        'videos',
        'video_game_websites',
        'video_game_external_links',
        'video_game_alternative_names',
        'video_game_platforms',
        'video_game_platform_families',
    ];

    /**
     * Performance timing data.
     *
     * @var array<string, float>
     */
    protected array $performanceTimings = [];

    /**
     * Start high-performance import session.
     */
    protected function startOptimizedImport(): void
    {
        $this->recordTiming('optimization_start');

        DB::disableQueryLog();

        if (DB::getDriverName() === 'pgsql') {
            try {
                DB::statement('SET synchronous_commit = OFF');
                DB::statement('SET CONSTRAINTS ALL DEFERRED');

                /* Skipping UNLOGGED optimization due to foreign key constraints
                foreach ($this->optimizedTables as $table) {
                    // Using try-catch per table in case some tables don't exist yet
                    try {
                        DB::statement("ALTER TABLE {$table} SET UNLOGGED");
                    } catch (\Throwable $e) {
                        Log::warning("Could not set table {$table} to UNLOGGED: ".$e->getMessage());
                    }
                }
                */
            } catch (\Throwable $e) {
                Log::error('Failed to apply Postgres optimizations: '.$e->getMessage());
            }
        }

        $this->recordTiming('optimization_complete');
    }

    /**
     * End high-performance import session and restore logging.
     */
    protected function endOptimizedImport(): void
    {
        $this->recordTiming('restore_start');

        if (DB::getDriverName() === 'pgsql') {
            foreach ($this->optimizedTables as $table) {
                try {
                    DB::statement("ALTER TABLE {$table} SET LOGGED");
                } catch (\Throwable $e) {
                    Log::error("Failed to restore logging for table {$table}: ".$e->getMessage());
                }
            }
        }

        $this->recordTiming('restore_complete');
    }

    /**
     * Bulk insert using PostgreSQL COPY for maximum performance.
     *
     * Falls back to insertOrIgnore for non-PostgreSQL databases.
     *
     * PERFORMANCE: PostgreSQL COPY is 50-100x faster than INSERT statements.
     *
     * @param  string  $table  Table name
     * @param  array<int, array<string, mixed>>  $rows  Rows to insert
     * @param  array<int, string>|null  $columns  Column names (auto-detected if null)
     * @param  bool  $ignoreConflicts  Use temp table + ON CONFLICT for duplicate handling
     * @param  array<int, string>|null  $uniqueBy  Unique columns for conflict handling (required for upsert)
     * @param  array<int, string>|null  $updateColumns  Columns to update on conflict (null = all except uniqueBy)
     * @return int Number of rows inserted
     */
    protected function bulkInsertOptimized(
        string $table,
        array $rows,
        ?array $columns = null,
        bool $ignoreConflicts = true,
        ?array $uniqueBy = null,
        ?array $updateColumns = null
    ): int {
        if ($rows === []) {
            return 0;
        }

        $startTime = microtime(true);

        // Auto-detect columns from first row if not provided
        if ($columns === null) {
            $firstRow = reset($rows);
            if (! is_array($firstRow)) {
                return 0;
            }
            $columns = array_keys($firstRow);
        }

        // Use PostgreSQL COPY for maximum performance
        if (DB::getDriverName() === 'pgsql') {
            try {
                if ($ignoreConflicts || $uniqueBy !== null) {
                    // Use temp table strategy for conflict handling
                    $inserted = $this->bulkInsertViaCopyWithConflictHandling($table, $rows, $columns, $uniqueBy, $updateColumns);
                } else {
                    // Direct COPY (fastest, but will fail on duplicates)
                    $inserted = $this->bulkInsertViaCopy($table, $rows, $columns);
                }

                $this->recordTiming("copy_{$table}", $startTime);

                return $inserted;
            } catch (\Throwable $e) {
                // CRITICAL: Log COPY failure and reconnect to avoid connection state corruption
                Log::error("PostgreSQL COPY failed for {$table}, falling back to INSERT (performance will degrade)", [
                    'table' => $table,
                    'error' => $e->getMessage(),
                    'row_count' => count($rows),
                ]);

                // Reconnect to ensure clean connection state after failed COPY
                try {
                    DB::reconnect();
                } catch (\Throwable $reconnectError) {
                    Log::error('Failed to reconnect after COPY failure: '.$reconnectError->getMessage());
                }
            }
        }

        // Fallback to standard insertOrIgnore or upsert
        if ($uniqueBy !== null) {
            DB::table($table)->upsert($rows, $uniqueBy, $updateColumns);
        } else {
            DB::table($table)->insertOrIgnore($rows);
        }

        $this->recordTiming("insert_{$table}", $startTime);

        return count($rows);
    }

    /**
     * Insert data using PostgreSQL COPY with conflict handling via temp table.
     *
     * Strategy: COPY into temp table, then INSERT...ON CONFLICT from temp to target.
     * This is still much faster than individual INSERT statements while handling duplicates.
     *
     * @param  string  $table  Table name
     * @param  array<int, array<string, mixed>>  $rows  Rows to insert
     * @param  array<int, string>  $columns  Column names
     * @param  array<int, string>|null  $uniqueBy  Unique columns for conflict handling
     * @param  array<int, string>|null  $updateColumns  Columns to update on conflict
     * @return int Number of rows inserted
     */
    protected function bulkInsertViaCopyWithConflictHandling(
        string $table,
        array $rows,
        array $columns,
        ?array $uniqueBy = null,
        ?array $updateColumns = null
    ): int {
        // Create temp table with same structure
        $tempTable = "temp_{$table}_".uniqid();

        try {
            // Create temp table (automatically dropped at session end)
            DB::statement("CREATE TEMP TABLE {$tempTable} (LIKE {$table} INCLUDING DEFAULTS)");

            // COPY data into temp table (no conflict issues here)
            $this->bulkInsertViaCopy($tempTable, $rows, $columns);

            // Insert from temp to target with conflict handling
            $columnList = implode(', ', array_map(fn ($col) => "\"{$col}\"", $columns));

            if ($uniqueBy !== null) {
                $uniqueList = implode(', ', array_map(fn ($col) => "\"{$col}\"", $uniqueBy));

                if ($updateColumns === null) {
                    $updateColumns = array_diff($columns, $uniqueBy, ['created_at', 'id']);
                }

                $updateList = implode(', ', array_map(fn ($col) => "\"{$col}\" = EXCLUDED.\"{$col}\"", $updateColumns));

                $sql = "INSERT INTO {$table} ({$columnList}) SELECT {$columnList} FROM {$tempTable} ON CONFLICT ({$uniqueList}) DO UPDATE SET {$updateList}";
            } else {
                $sql = "INSERT INTO {$table} ({$columnList}) SELECT {$columnList} FROM {$tempTable} ON CONFLICT DO NOTHING";
            }

            DB::statement($sql);

            // Get row count
            $inserted = DB::table($tempTable)->count();

            return $inserted;
        } finally {
            // Clean up temp table
            try {
                DB::statement("DROP TABLE IF EXISTS {$tempTable}");
            } catch (\Throwable $e) {
                // Temp tables auto-drop on session end anyway
                Log::debug("Failed to drop temp table {$tempTable}: ".$e->getMessage());
            }
        }
    }

    /**
     * Insert data using PostgreSQL COPY command.
     *
     * COPY is PostgreSQL's bulk loading mechanism and is significantly faster than INSERT.
     * Does NOT handle duplicates - will fail if conflicts occur.
     *
     * @param  string  $table  Table name
     * @param  array<int, array<string, mixed>>  $rows  Rows to insert
     * @param  array<int, string>  $columns  Column names
     * @return int Number of rows inserted
     */
    protected function bulkInsertViaCopy(string $table, array $rows, array $columns): int
    {
        // Create temporary CSV file
        $tempFile = tempnam(sys_get_temp_dir(), 'copy_');
        if ($tempFile === false) {
            throw new \RuntimeException('Failed to create temporary file for COPY operation');
        }

        $handle = null;
        $nullMarker = '__NULL__';

        try {
            $handle = fopen($tempFile, 'w');
            if ($handle === false) {
                throw new \RuntimeException('Failed to open temporary file for writing');
            }

            // Write data in PostgreSQL TEXT format
            foreach ($rows as $row) {
                $values = [];
                foreach ($columns as $column) {
                    $value = $row[$column] ?? null;

                    // Handle special PostgreSQL types
                    if ($value === null) {
                        $values[] = $nullMarker;
                    } elseif (is_bool($value)) {
                        $values[] = $value ? 't' : 'f';
                    } elseif (is_array($value) || is_object($value)) {
                        $json = json_encode($value, JSON_THROW_ON_ERROR);
                        // Escape backslashes and tabs in JSON
                        $values[] = str_replace(['\\', "\t", "\n", "\r"], ['\\\\', '\\t', '\\n', '\\r'], $json);
                    } else {
                        // Escape special characters for TEXT format
                        // Backslash must be escaped first!
                        $escaped = str_replace(
                            ['\\', "\t", "\n", "\r"],
                            ['\\\\', '\\t', '\\n', '\\r'],
                            (string) $value
                        );
                        $values[] = $escaped;
                    }
                }

                fwrite($handle, implode("\t", $values)."\n");
            }

            fclose($handle);
            $handle = null; // Mark as closed

            $pdo = DB::getPdo();
            if (! $pdo instanceof \PDO) {
                throw new \RuntimeException('Failed to get PDO connection');
            }

            // Use the custom null marker to avoid \N issues
            $columnList = implode(', ', array_map(fn ($col) => "\"{$col}\"", $columns));
            $pdo->pgsqlCopyFromFile($table, $tempFile, "\t", $nullMarker, $columnList);

            return count($rows);
        } finally {
            // Ensure file handle is closed even if exception occurs
            if ($handle !== null && is_resource($handle)) {
                @fclose($handle);
            }

            // Clean up temporary file
            if (file_exists($tempFile)) {
                @unlink($tempFile);
            }
        }
    }

    /**
     * Record performance timing checkpoint.
     *
     * @param  string  $label  Checkpoint label
     * @param  float|null  $startTime  Start time (if measuring duration)
     */
    protected function recordTiming(string $label, ?float $startTime = null): void
    {
        if ($startTime !== null) {
            $this->performanceTimings[$label] = round((microtime(true) - $startTime) * 1000, 2);
        } else {
            $this->performanceTimings[$label] = microtime(true);
        }
    }

    /**
     * Get performance timing report.
     *
     * @return array<string, float>
     */
    protected function getPerformanceReport(): array
    {
        return $this->performanceTimings;
    }

    /**
     * Output performance timing report to console.
     */
    protected function outputPerformanceReport(): void
    {
        if ($this->performanceTimings === []) {
            return;
        }

        $this->newLine();
        $this->info('=== Performance Report ===');

        $tableData = [];
        foreach ($this->performanceTimings as $label => $value) {
            if (str_contains($label, 'start') || str_contains($label, 'complete')) {
                continue; // Skip timestamp markers
            }

            $tableData[] = [
                'Operation' => $label,
                'Duration' => $value > 1000 ? round($value / 1000, 2).'s' : $value.'ms',
            ];
        }

        if ($tableData !== []) {
            $this->table(['Operation', 'Duration'], $tableData);
        }
    }
}
