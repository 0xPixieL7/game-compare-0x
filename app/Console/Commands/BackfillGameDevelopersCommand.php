<?php

namespace App\Console\Commands;

use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;

class BackfillGameDevelopersCommand extends Command
{
    protected $signature = 'games:backfill-developers 
        {--provider=rawg : Provider to backfill from}
        {--dry-run : Preview without saving}';

    protected $description = 'Backfill developer and publisher data from source_payload for all games';

    public function handle()
    {
        $provider = $this->option('provider');
        $dryRun = $this->option('dry-run');

        $this->info("🔧 Backfilling developer/publisher data from {$provider}...");
        $this->newLine();

        // Get all games with source_payload but missing developer/publisher
        $games = DB::table('video_games')
            ->where('provider', $provider)
            ->whereNotNull('source_payload')
            ->where(function ($query) {
                $query->whereNull('developer')
                    ->orWhereNull('publisher');
            })
            ->get();

        $this->info("Found {$games->count()} games with missing developer/publisher data");
        $this->newLine();

        $stats = ['updated' => 0, 'skipped' => 0, 'errors' => 0];
        $progressBar = $this->output->createProgressBar($games->count());
        $progressBar->start();

        foreach ($games as $game) {
            try {
                $payload = json_decode($game->source_payload, true);
                
                if (!$payload) {
                    $stats['skipped']++;
                    $progressBar->advance();
                    continue;
                }

                // Extract developers and publishers
                $developers = array_map(fn($d) => $d['name'] ?? 'Unknown', $payload['developers'] ?? []);
                $publishers = array_map(fn($p) => $p['name'] ?? 'Unknown', $payload['publishers'] ?? []);

                // Skip if both are empty
                if (empty($developers) && empty($publishers)) {
                    $stats['skipped']++;
                    $progressBar->advance();
                    continue;
                }

                if (!$dryRun) {
                    DB::table('video_games')
                        ->where('id', $game->id)
                        ->update([
                            'developer' => !empty($developers) ? json_encode($developers) : null,
                            'publisher' => !empty($publishers) ? json_encode($publishers) : null,
                            'updated_at' => now(),
                        ]);
                }

                $stats['updated']++;
            } catch (\Throwable $e) {
                $stats['errors']++;
                $this->newLine();
                $this->error("Error processing game {$game->id}: {$e->getMessage()}");
            }

            $progressBar->advance();
        }

        $progressBar->finish();
        $this->newLine(2);

        // Summary
        $this->info('=== Summary ===');
        $this->table(
            ['Metric', 'Count'],
            [
                ['Games Processed', $games->count()],
                ['Updated', $stats['updated']],
                ['Skipped (no data)', $stats['skipped']],
                ['Errors', $stats['errors']],
            ]
        );

        if ($dryRun) {
            $this->warn('This was a dry run. Run without --dry-run to apply changes.');
        }

        return self::SUCCESS;
    }
}
