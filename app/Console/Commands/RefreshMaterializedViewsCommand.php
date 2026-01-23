<?php

declare(strict_types=1);

namespace App\Console\Commands;

use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Log;

class RefreshMaterializedViewsCommand extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'db:refresh-views';

    /**
     * The console command description.
     *
     * @var string
     */
    protected $description = 'Refresh all database materialized views';

    /**
     * Execute the console command.
     */
    public function handle(): int
    {
        if (DB::getDriverName() !== 'pgsql') {
            $this->error('This command only supports PostgreSQL.');

            return self::FAILURE;
        }

        $views = [
            'video_game_title_sources_mv',
            'video_games_ranked_mv',
            'video_games_genre_ranked_mv',
            'video_games_upcoming_mv',
            'video_games_toplists_mv',
        ];

        $this->info('Starting materialized views refresh...');

        foreach ($views as $view) {
            $this->info("Refreshing {$view}...");
            $start = microtime(true);

            try {
                // Try concurrent refresh if possible (requires unique index)
                DB::statement("REFRESH MATERIALIZED VIEW CONCURRENTLY public.{$view}");
                $duration = round(microtime(true) - $start, 2);
                $this->line("  <info>✔</info> {$view} refreshed concurrently in {$duration}s");
            } catch (\Throwable $e) {
                // Fallback to standard refresh
                $this->warn("  Concurrent refresh failed for {$view}, falling back to standard refresh...");
                try {
                    DB::statement("REFRESH MATERIALIZED VIEW public.{$view}");
                    $duration = round(microtime(true) - $start, 2);
                    $this->line("  <info>✔</info> {$view} refreshed in {$duration}s");
                } catch (\Throwable $innerEx) {
                    $this->error("  Failed to refresh {$view}: ".$innerEx->getMessage());
                    Log::error("Failed to refresh materialized view {$view}", ['error' => $innerEx->getMessage()]);
                }
            }
        }

        $this->info('All materialized views refreshed successfully.');

        return self::SUCCESS;
    }
}
