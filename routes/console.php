<?php

use Illuminate\Foundation\Inspiring;
use Illuminate\Support\Facades\Artisan;
use Illuminate\Support\Facades\Schedule;

Artisan::command('inspire', function () {
    $this->comment(Inspiring::quote());
})->purpose('Display an inspiring quote');

Schedule::command('rates:sync')->hourly()->withoutOverlapping();
Schedule::command('prices:rebase-btc')->hourlyAt(10)->withoutOverlapping();

