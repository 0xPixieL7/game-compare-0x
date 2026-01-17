<?php

declare(strict_types=1);

use App\Models\VideoGame;
use Illuminate\Support\Facades\File;

use function Pest\Laravel\artisan;

afterEach(function (): void {
    $dir = base_path('storage/igdb-dumps-test');

    if (File::exists($dir)) {
        File::deleteDirectory($dir);
    }
});

it('resumes games CSV import from a saved checkpoint', function () {
    $dir = base_path('storage/igdb-dumps-test');

    File::ensureDirectoryExists($dir);

    File::put($dir.'/9999999999_platforms.csv', <<<'CSV'
id,name,abbreviation,logo_url
6,PC (Microsoft Windows),PC,
CSV);

    File::put($dir.'/9999999999_games.csv', <<<'CSV'
id,name,slug,summary,platforms,genres,first_release_date,total_rating,total_rating_count
1,Example Game 1,example-game-1,"Example summary.","{6}","{5}",1488844800,90.0,100
2,Example Game 2,example-game-2,"Example summary.","{6}","{5}",1488844800,91.0,101
3,Example Game 3,example-game-3,"Example summary.","{6}","{5}",1488844800,92.0,102
CSV);

    // First run processes only 2 records (limit), but should persist a checkpoint.
    artisan('gc:import-igdb', ['--path' => $dir, '--provider' => 'igdb', '--limit' => 2])->assertExitCode(0);

    expect(VideoGame::query()->count())->toBe(2);

    $checkpointFiles = File::glob($dir.'/.checkpoints/*.json') ?: [];
    expect($checkpointFiles)->not->toBeEmpty();

    // Second run (unlimited) should resume and process the remaining record.
    artisan('gc:import-igdb', ['--path' => $dir, '--provider' => 'igdb', '--limit' => 0])->assertExitCode(0);

    expect(VideoGame::query()->count())->toBe(3);

    // After fully consuming the file, the checkpoint should be cleared.
    $checkpointFilesAfter = File::glob($dir.'/.checkpoints/*.json') ?: [];
    expect($checkpointFilesAfter)->toBeEmpty();
});
