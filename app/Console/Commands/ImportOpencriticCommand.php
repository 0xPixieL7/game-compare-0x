<?php

namespace App\Console\Commands;

use App\Services\Import\Providers\OpenCriticImportProvider;
use Illuminate\Console\Command;

class ImportOpencriticCommand extends Command
{
    protected $signature = 'gc:import-opencritic {--limit=0 : Limit number of games to process} {--force : Force update existing scores}';

    protected $description = 'Fetch OpenCritic scores for games (using name matching)';

    public function handle(OpenCriticImportProvider $provider): int
    {
        return $provider->handle($this);
    }
}
