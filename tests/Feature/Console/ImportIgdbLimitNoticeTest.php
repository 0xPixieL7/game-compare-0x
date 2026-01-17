<?php

declare(strict_types=1);

use Illuminate\Support\Facades\File;

use function Pest\Laravel\artisan;

it('warns when running the IGDB import with a limit', function () {
    $dir = storage_path('framework/testing/igdb-dumps');
    File::ensureDirectoryExists($dir);

    $file = $dir.'/test_games.csv';
    file_put_contents($file, implode("\n", [
        'id,name,slug',
        '1,Test Game,test-game',
        '2,Another Game,another-game',
        '',
    ]));

    artisan('gc:import-igdb', [
        '--path' => $file,
        '--provider' => 'igdb',
        '--limit' => 1,
        '--resume' => 0,
    ])
        ->expectsOutputToContain('NOTE: This was a limited run')
        ->assertExitCode(0);
});
