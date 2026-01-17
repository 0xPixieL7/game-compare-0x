<?php

declare(strict_types=1);

namespace App\Services\Import\Concerns;

use Symfony\Component\Console\Helper\ProgressBar;

trait HasProgressBar
{
    /**
     * Configure a progress bar with standard formatting.
     */
    protected function configureProgressBar(ProgressBar $bar, bool $redraw = false, bool $byteProgress = false): void
    {
        if ($byteProgress) {
            $bar->setFormat(' %current%/%max% bytes [%bar%] %percent:3s%%');
            $bar->setRedrawFrequency(1);

            return;
        }

        $format = $redraw ? ' %current%/%max% [%bar%] %percent:3s%%' : 'normal';
        $bar->setFormat($format);
        $bar->setRedrawFrequency(1);
    }
}
