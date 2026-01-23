<?php

declare(strict_types=1);

namespace App\Services;

use Illuminate\Support\Collection;
use Illuminate\Support\Facades\DB;

class DatabaseOptimizationService
{
    public function analyzeTablePerformance(): array
    {
        $results = [];

        // Analyze key tables for performance
        $tables = [
            'products',
            'video_game_titles',
            'video_game_title_sources',
            'video_game_prices',
            'video_games',
            'video_game_sources',
            'users',
        ];

        foreach ($tables as $table) {
            if (! $this->tableExists($table)) {
                continue;
            }

            $results[$table] = [
                'row_count' => $this->getTableRowCount($table),
                'table_size' => $this->getTableSize($table),
                'indexes' => $this->getTableIndexes($table),
                'query_performance' => $this->analyzeQueryPerformance($table),
            ];
        }

        return $results;
    }

    public function getSlowQueries(int $limit = 10): Collection
    {
        if (DB::getDriverName() === 'pgsql') {
            // PostgreSQL slow query analysis
            return collect(DB::select("
                SELECT
                    query,
                    calls,
                    total_time,
                    mean_time,
                    rows
                FROM pg_stat_statements
                WHERE query NOT LIKE '%pg_stat_statements%'
                ORDER BY total_time DESC
                LIMIT ?
            ", [$limit]));
        }

        return collect();
    }

    public function optimizeTableQueries(string $table): array
    {
        $suggestions = [];

        switch ($table) {
            case 'video_game_prices':
                $suggestions = [
                    'Ensure indexes on video_game_id, currency for price lookups',
                    'Add composite index for (video_game_id, currency, amount_minor) for best price queries',
                    'Consider partitioning by recorded_at for large datasets',
                    'Add partial index for active prices only',
                ];
                break;

            case 'video_game_title_sources':
                $suggestions = [
                    'Optimize text search with GIN index on name field',
                    'Add composite index for (video_game_title_id, video_game_source_id)',
                    'Consider JSONB indexing for raw_payload queries',
                ];
                break;

            case 'products':
                $suggestions = [
                    'Add full-text search index for name/title fields',
                    'Optimize type-based filtering with dedicated index',
                    'Consider covering indexes for common SELECT patterns',
                ];
                break;

            default:
                $suggestions = ['No specific optimizations available for this table'];
        }

        return $suggestions;
    }

    public function benchmarkQueries(): array
    {
        $benchmarks = [];

        // Benchmark common query patterns
        $queries = [
            'products_by_type' => [
                'sql' => "SELECT COUNT(*) FROM products WHERE type = 'video_game'",
                'description' => 'Count products by type',
            ],
            'price_search' => [
                'sql' => "SELECT COUNT(*) FROM video_game_prices WHERE currency = 'USD'",
                'description' => 'Count prices by currency',
            ],
            'title_search' => [
                'sql' => "SELECT COUNT(*) FROM video_game_titles WHERE name ILIKE '%mario%'",
                'description' => 'Text search on game titles',
            ],
        ];

        foreach ($queries as $key => $query) {
            $start = microtime(true);
            try {
                $result = DB::select($query['sql']);
                $execution_time = (microtime(true) - $start) * 1000; // Convert to milliseconds

                $benchmarks[$key] = [
                    'description' => $query['description'],
                    'execution_time_ms' => round($execution_time, 2),
                    'result_count' => $result[0]->count ?? 0,
                    'status' => 'success',
                ];
            } catch (\Exception $e) {
                $benchmarks[$key] = [
                    'description' => $query['description'],
                    'execution_time_ms' => null,
                    'result_count' => null,
                    'status' => 'error',
                    'error' => $e->getMessage(),
                ];
            }
        }

        return $benchmarks;
    }

    public function getCacheStatistics(): array
    {
        if (DB::getDriverName() === 'pgsql') {
            $stats = DB::select("
                SELECT
                    schemaname,
                    tablename,
                    attname as column_name,
                    n_distinct,
                    correlation
                FROM pg_stats
                WHERE schemaname = 'public'
                AND tablename IN ('products', 'video_game_prices', 'video_game_titles')
                ORDER BY tablename, attname
            ");

            return collect($stats)->groupBy('tablename')->toArray();
        }

        return [];
    }

    public function refreshTitleSourcesMaterializedView(): void
    {
        $this->refreshView('video_game_title_sources_mv');
    }

    public function refreshAllMaterializedViews(): void
    {
        $views = [
            'video_game_title_sources_mv',
            'video_games_ranked_mv',
            'video_games_genre_ranked_mv',
            'video_games_upcoming_mv',
            'video_games_toplists_mv',
        ];

        foreach ($views as $view) {
            $this->refreshView($view);
        }
    }

    private function refreshView(string $view): void
    {
        if (DB::getDriverName() !== 'pgsql') {
            return;
        }

        try {
            DB::statement("REFRESH MATERIALIZED VIEW CONCURRENTLY public.{$view}");
        } catch (\Throwable $e) {
            DB::statement("REFRESH MATERIALIZED VIEW public.{$view}");
        }
    }

    public function suggestOptimizations(): array
    {
        $suggestions = [];
        $analysis = $this->analyzeTablePerformance();

        foreach ($analysis as $table => $data) {
            if ($data['row_count'] > 10000) {
                $suggestions[] = [
                    'table' => $table,
                    'type' => 'large_table',
                    'suggestion' => "Table {$table} has {$data['row_count']} rows. Consider partitioning or archiving old data.",
                    'priority' => 'medium',
                ];
            }

            if (count($data['indexes']) < 3 && $data['row_count'] > 1000) {
                $suggestions[] = [
                    'table' => $table,
                    'type' => 'missing_indexes',
                    'suggestion' => "Table {$table} may benefit from additional indexes for better query performance.",
                    'priority' => 'high',
                ];
            }
        }

        return $suggestions;
    }

    private function tableExists(string $table): bool
    {
        try {
            return DB::getSchemaBuilder()->hasTable($table);
        } catch (\Exception $e) {
            return false;
        }
    }

    private function getTableRowCount(string $table): int
    {
        try {
            $result = DB::select("SELECT COUNT(*) as count FROM {$table}");

            return (int) $result[0]->count;
        } catch (\Exception $e) {
            return 0;
        }
    }

    private function getTableSize(string $table): array
    {
        if (DB::getDriverName() === 'pgsql') {
            try {
                $result = DB::select('
                    SELECT
                        pg_size_pretty(pg_total_relation_size(?)) as total_size,
                        pg_size_pretty(pg_relation_size(?)) as table_size,
                        pg_size_pretty(pg_indexes_size(?)) as index_size
                ', [$table, $table, $table]);

                return [
                    'total_size' => $result[0]->total_size ?? 'Unknown',
                    'table_size' => $result[0]->table_size ?? 'Unknown',
                    'index_size' => $result[0]->index_size ?? 'Unknown',
                ];
            } catch (\Exception $e) {
                return ['error' => $e->getMessage()];
            }
        }

        return ['message' => 'Size analysis only available for PostgreSQL'];
    }

    private function getTableIndexes(string $table): array
    {
        if (DB::getDriverName() === 'pgsql') {
            try {
                $indexes = DB::select("
                    SELECT
                        indexname,
                        indexdef
                    FROM pg_indexes
                    WHERE tablename = ?
                    AND schemaname = 'public'
                    ORDER BY indexname
                ", [$table]);

                return collect($indexes)->pluck('indexname')->toArray();
            } catch (\Exception $e) {
                return [];
            }
        }

        return ['message' => 'Index analysis only available for PostgreSQL'];
    }

    private function analyzeQueryPerformance(string $table): array
    {
        // Simple query performance test
        $start = microtime(true);

        try {
            DB::select("SELECT COUNT(*) FROM {$table}");
            $execution_time = (microtime(true) - $start) * 1000;

            return [
                'simple_count_ms' => round($execution_time, 2),
                'performance_rating' => $this->getPerformanceRating($execution_time),
            ];
        } catch (\Exception $e) {
            return [
                'error' => $e->getMessage(),
                'performance_rating' => 'error',
            ];
        }
    }

    private function getPerformanceRating(float $timeMs): string
    {
        if ($timeMs < 10) {
            return 'excellent';
        } elseif ($timeMs < 50) {
            return 'good';
        } elseif ($timeMs < 200) {
            return 'fair';
        } else {
            return 'poor';
        }
    }
}
