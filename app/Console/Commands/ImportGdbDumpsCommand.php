<?php

declare(strict_types=1);

namespace App\Console\Commands;

use App\Services\Import\Providers\GdbImportProvider;
use Illuminate\Console\Command;

class ImportGdbDumpsCommand extends Command
{
    protected $signature = 'gc:import-gdb {--path= : Directory path containing gdb dump files} {--provider=giantbomb : Provider key to store on video_game_sources.provider} {--limit=0 : Limit number of records to process per file}';

    protected $description = 'Import GiantBomb (GDB) dump files from storage/gdb-dumps into products/video_game_titles/video_games.';

    public function handle(GdbImportProvider $provider): int
    {
        return $provider->handle($this);
    }
}
