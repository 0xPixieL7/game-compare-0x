<?php

declare(strict_types=1);

namespace App\Services\Import\Concerns;

use Illuminate\Support\Facades\DB;
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
        'videos'
    ];

    /**
     * Start high-performance import session.
     */
    protected function startOptimizedImport(): void
    {
        DB::disableQueryLog();

        if (DB::getDriverName() === 'pgsql') {
            try {
                DB::statement('SET synchronous_commit = OFF');
                DB::statement('SET CONSTRAINTS ALL DEFERRED');
                
                foreach ($this->optimizedTables as $table) {
                    // Using try-catch per table in case some tables don't exist yet
                    try {
                        DB::statement("ALTER TABLE {$table} SET UNLOGGED");
                    } catch (\Throwable $e) {
                        Log::warning("Could not set table {$table} to UNLOGGED: " . $e->getMessage());
                    }
                }
            } catch (\Throwable $e) {
                Log::error("Failed to apply Postgres optimizations: " . $e->getMessage());
            }
        }
    }

    /**
     * End high-performance import session and restore logging.
     */
    protected function endOptimizedImport(): void
    {
        if (DB::getDriverName() === 'pgsql') {
            foreach ($this->optimizedTables as $table) {
                try {
                    DB::statement("ALTER TABLE {$table} SET LOGGED");
                } catch (\Throwable $e) {
                    Log::error("Failed to restore logging for table {$table}: " . $e->getMessage());
                }
            }
        }
    }
}
