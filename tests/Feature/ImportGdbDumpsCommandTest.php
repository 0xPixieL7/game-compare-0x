<?php

declare(strict_types=1);

use App\Models\Product;
use App\Models\VideoGame;
use App\Models\VideoGameSource;
use App\Models\VideoGameTitle;
use Illuminate\Support\Facades\File;

use function Pest\Laravel\artisan;

it('imports gdb dumps from a directory and is idempotent', function () {
    $dir = base_path('storage/gdb-dumps-test');

    File::ensureDirectoryExists($dir);

    $payload = [
        [
            'id' => 1001,
            'name' => 'Example Game',
            'platforms' => [
                ['name' => 'PC'],
                ['name' => 'PlayStation 5'],
            ],
            'image' => [
                'thumb_url' => 'https://example.test/thumb.jpg',
                'small_url' => 'https://example.test/small.jpg',
            ],
            'aggregated_rating' => 88,
        ],
    ];

    File::put($dir.'/dump.json', json_encode($payload));

    artisan('gc:import-gdb', ['--path' => $dir, '--provider' => 'giantbomb'])->assertExitCode(0);

    expect(Product::query()->count())->toBe(1);
    expect(VideoGameSource::query()->count())->toBe(1);
    expect(VideoGameTitle::query()->count())->toBe(1);
    expect(VideoGame::query()->count())->toBe(1);

    // Re-run should not create duplicates.
    artisan('gc:import-gdb', ['--path' => $dir, '--provider' => 'giantbomb'])->assertExitCode(0);

    expect(Product::query()->count())->toBe(1);
    expect(VideoGameSource::query()->count())->toBe(1);
    expect(VideoGameTitle::query()->count())->toBe(1);
    expect(VideoGame::query()->count())->toBe(1);

    $product = Product::query()->firstOrFail();

    $game = VideoGame::query()->firstOrFail();

    expect($product->videoGames()->count())->toBe(1);
    expect($game->platform)->toContain('PC');
    expect($game->platform)->toContain('PlayStation 5');
    expect((float) $game->rating)->toEqualWithDelta(88.0, 0.0001);
});
