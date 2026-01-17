<?php

declare(strict_types=1);

namespace App\Services\Import\Contracts;

use Illuminate\Console\Command;

interface ImportProvider
{
    /**
     * Get the unique name/key of the provider.
     */
    public function getName(): string;

    /**
     * Execute the import process.
     */
    public function handle(Command $command): int;
}
