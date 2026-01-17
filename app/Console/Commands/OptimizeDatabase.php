<?php

declare(strict_types=1);

namespace App\Console\Commands;

use App\Services\DatabaseOptimizationService;
use Illuminate\Console\Command;
use Illuminate\Support\Facades\Artisan;

class OptimizeDatabase extends Command
{
    protected $signature = 'db:optimize
                          {--analyze : Only analyze performance without applying optimizations}
                          {--apply : Apply the comprehensive optimization migration}
                          {--benchmark : Run performance benchmarks}
                          {--suggestions : Get optimization suggestions}
                          {--refresh-mv : Refresh the materialized view for title source lookups}';

    protected $description = 'Optimize database performance with comprehensive indexes and analysis';

    public function handle(DatabaseOptimizationService $optimizationService): int
    {
        $this->info('🚀 Database Optimization Tool');
        $this->newLine();

        if ($this->option('analyze')) {
            $this->analyzePerformance($optimizationService);
        }

        if ($this->option('apply')) {
            $this->applyOptimizations();
        }

        if ($this->option('benchmark')) {
            $this->runBenchmarks($optimizationService);
        }

        if ($this->option('refresh-mv')) {
            $this->refreshMaterializedView($optimizationService);
        }

        if ($this->option('suggestions')) {
            $this->showSuggestions($optimizationService);
        }

        // If no specific options, show a menu
        if (! $this->hasAnyOption()) {
            $this->showInteractiveMenu($optimizationService);
        }

        return Command::SUCCESS;
    }

    private function analyzePerformance(DatabaseOptimizationService $service): void
    {
        $this->info('📊 Analyzing Database Performance...');
        $this->newLine();

        $analysis = $service->analyzeTablePerformance();

        $headers = ['Table', 'Rows', 'Performance', 'Indexes', 'Size'];
        $rows = [];

        foreach ($analysis as $table => $data) {
            $rows[] = [
                $table,
                number_format($data['row_count']),
                $data['query_performance']['performance_rating'] ?? 'unknown',
                is_array($data['indexes']) ? count($data['indexes']) : 'N/A',
                $data['table_size']['total_size'] ?? 'N/A',
            ];
        }

        $this->table($headers, $rows);
    }

    private function applyOptimizations(): void
    {
        $this->info('⚡ Applying Database Optimizations...');

        if (! $this->confirm('This will run the comprehensive database optimization migration. Continue?')) {
            $this->warn('Optimization cancelled.');

            return;
        }

        try {
            $this->info('Running migration...');
            Artisan::call('migrate', [
                '--path' => 'database/migrations/2026_01_15_163145_comprehensive_database_optimizations.php',
                '--force' => true,
            ]);

            $this->info('✅ Database optimizations applied successfully!');
            $this->info('Indexes created for:');
            $this->line('• Products table - type, name, title, slug indexes');
            $this->line('• Video game prices - currency, amount, retailer, composite indexes');
            $this->line('• Video game titles - name, slug, product composite indexes');
            $this->line('• Video game sources - provider, name, active filtering');
            $this->line('• Users table - email verification, name, timestamps');
            $this->line('• PostgreSQL full-text search indexes (if applicable)');
            $this->line('• Partial indexes for active records only');

        } catch (\Exception $e) {
            $this->error('❌ Error applying optimizations: '.$e->getMessage());
        }
    }

    private function runBenchmarks(DatabaseOptimizationService $service): void
    {
        $this->info('🏃 Running Performance Benchmarks...');
        $this->newLine();

        $benchmarks = $service->benchmarkQueries();

        $headers = ['Query', 'Description', 'Time (ms)', 'Result Count', 'Status'];
        $rows = [];

        foreach ($benchmarks as $name => $benchmark) {
            $timeFormatted = $benchmark['execution_time_ms']
                ? $benchmark['execution_time_ms'].' ms'
                : 'N/A';

            $resultCount = $benchmark['result_count'] !== null
                ? number_format($benchmark['result_count'])
                : 'N/A';

            $status = $benchmark['status'] === 'success' ? '✅' : '❌';

            $rows[] = [
                $name,
                $benchmark['description'],
                $timeFormatted,
                $resultCount,
                $status,
            ];
        }

        $this->table($headers, $rows);

        // Performance summary
        $successful = collect($benchmarks)->where('status', 'success');
        if ($successful->count() > 0) {
            $avgTime = $successful->avg('execution_time_ms');
            $this->info('Average query execution time: '.round($avgTime, 2).' ms');

            if ($avgTime < 10) {
                $this->info('🎉 Excellent performance!');
            } elseif ($avgTime < 50) {
                $this->info('✅ Good performance');
            } else {
                $this->warn('⚠️ Consider optimization - queries are slow');
            }
        }
    }

    private function refreshMaterializedView(DatabaseOptimizationService $service): void
    {
        $this->info('♻️ Refreshing materialized view...');
        $service->refreshTitleSourcesMaterializedView();
        $this->info('✅ Materialized view refresh triggered.');
    }

    private function showSuggestions(DatabaseOptimizationService $service): void
    {
        $this->info('💡 Optimization Suggestions');
        $this->newLine();

        $suggestions = $service->suggestOptimizations();

        if (empty($suggestions)) {
            $this->info('✅ No optimization suggestions at this time. Your database looks good!');

            return;
        }

        foreach ($suggestions as $suggestion) {
            $icon = match ($suggestion['priority']) {
                'high' => '🔴',
                'medium' => '🟡',
                'low' => '🟢',
                default => '📌',
            };

            $this->line($icon.' '.$suggestion['suggestion']);
        }
    }

    private function showInteractiveMenu(DatabaseOptimizationService $service): void
    {
        $choice = $this->choice(
            'What would you like to do?',
            [
                'analyze' => 'Analyze current performance',
                'benchmark' => 'Run performance benchmarks',
                'suggestions' => 'Get optimization suggestions',
                'apply' => 'Apply comprehensive optimizations',
                'all' => 'Run all analysis steps',
            ],
            'analyze'
        );

        match ($choice) {
            'analyze' => $this->analyzePerformance($service),
            'benchmark' => $this->runBenchmarks($service),
            'suggestions' => $this->showSuggestions($service),
            'apply' => $this->applyOptimizations(),
            'all' => [
                $this->analyzePerformance($service),
                $this->newLine(),
                $this->runBenchmarks($service),
                $this->newLine(),
                $this->showSuggestions($service),
            ],
        };
    }

    private function hasAnyOption(): bool
    {
        return $this->option('analyze') ||
               $this->option('apply') ||
               $this->option('benchmark') ||
               $this->option('refresh-mv') ||
               $this->option('suggestions');
    }
}
