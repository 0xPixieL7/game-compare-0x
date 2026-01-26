<?php

declare(strict_types=1);

namespace App\Console\Commands;

use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Redis;

class MonitorEnrichmentCommand extends Command
{
    protected $signature = 'games:monitor-enrichment';

    protected $description = 'Monitor enrichment progress in real-time';

    public function handle(): void
    {
        $this->info('Starting enrichment monitor... (Ctrl+C to stop)');
        $this->newLine();

        $startTime = time();
        $lastProcessed = 0;

        while (true) {
            // 1. Get Cursor Status
            $cursor = DB::table('command_cursors')->where('command_name', 'games:enrich-all')->first();

            if (! $cursor) {
                $this->warn('No active enrichment cursor found.');
                sleep(2);

                continue;
            }

            $meta = json_decode($cursor->metadata ?? '{}', true);
            $processed = $meta['processed_count'] ?? 0;
            $total = $cursor->total_items;
            $percent = $total > 0 ? round(($processed / $total) * 100, 2) : 0;

            // 2. Get Queue Status
            $pendingJobs = 0;
            try {
                $pendingJobs = Redis::connection()->llen('queues:default');
            } catch (\Exception $e) {
                // Redis might not be connected
            }

            // 3. Calculate Speed
            $speed = 0;
            if ($lastProcessed > 0) {
                $diff = $processed - $lastProcessed;
                $speed = $diff / 2; // 2 second interval
            }
            $lastProcessed = $processed;

            // Clear screen (ANSI)
            $this->output->write("\033[2J\033[;H");

            $this->info('📊 Enrichment Monitor');
            $this->info('---------------------');
            $this->line('Total Games:    '.number_format($total));
            $this->line('Dispatched:     '.number_format($processed)." ({$percent}%)");
            $this->line('Pending Jobs:   '.number_format($pendingJobs));
            $this->newLine();

            $this->info('⚡ Performance');
            $this->info('---------------------');
            $this->line('Dispatch Speed: '.number_format($speed, 1).' games/sec');
            $this->line('Runtime:        '.gmdate('H:i:s', time() - $startTime));

            $this->newLine();
            $this->comment('Last updated: '.now()->toTimeString());

            sleep(2);
        }
    }
}
