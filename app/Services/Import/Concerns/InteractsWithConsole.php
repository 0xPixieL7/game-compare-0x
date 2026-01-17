<?php

declare(strict_types=1);

namespace App\Services\Import\Concerns;

use Illuminate\Console\Command;

trait InteractsWithConsole
{
    protected ?Command $command = null;

    /**
     * Set the console command context.
     */
    public function setCommand(Command $command): self
    {
        $this->command = $command;

        return $this;
    }

    /**
     * Output info message to console.
     */
    protected function info(string $message): void
    {
        $this->command?->info($message);
    }

    /**
     * Output error message to console.
     */
    protected function error(string $message): void
    {
        $this->command?->error($message);
    }

    /**
     * Output warning message to console.
     */
    protected function warn(string $message): void
    {
        $this->command?->warn($message);
    }

    /**
     * Output line to console.
     */
    protected function line(string $message): void
    {
        $this->command?->line($message);
    }

    /**
     * Output new line to console.
     */
    protected function newLine(int $count = 1): void
    {
        $this->command?->newLine($count);
    }
}
