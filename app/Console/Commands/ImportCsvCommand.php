<?php

namespace App\Console\Commands;

use App\Services\Import\Providers\CsvImportProvider;
use Illuminate\Console\Command;

class ImportCsvCommand extends Command
{
    protected $signature = 'app:import-csv {path? : The directory containing the CSV exports} {--limit=0 : Limit number of records to process per file}';

    protected $description = 'Import video games and media from CSV exports using DTOs';

    public function handle(CsvImportProvider $provider): int
    {
        return $provider->handle($this);
    }
}
