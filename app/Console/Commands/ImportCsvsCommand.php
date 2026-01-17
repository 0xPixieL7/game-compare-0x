<?php

namespace App\Console\Commands;

use App\Services\Import\CsvImportService;
use Illuminate\Console\Command;

class ImportCsvsCommand extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'import:csvs';

    /**
     * The console command description.
     *
     * @var string
     */
    protected $description = 'Import data from SQLite CSV exports synchronously';

    /**
     * Execute the console command.
     */
    public function handle(CsvImportService $importer)
    {
        $this->info('Starting CSV Import...');
        $importer->run();
        $this->info('Import finished successfully.');
    }
}
