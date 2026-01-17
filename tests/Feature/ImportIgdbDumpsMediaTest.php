<?php

declare(strict_types=1);

use App\Models\Image;
use App\Models\VideoGame;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\File;

use function Pest\Laravel\artisan;

afterEach(function (): void {
    $dir = base_path('storage/igdb-dumps-test');

    if (File::exists($dir)) {
        File::deleteDirectory($dir);
    }
});

it('merges screenshot media across multiple batch flushes without losing variants', function (): void {
    $dir = base_path('storage/igdb-dumps-test');
    File::ensureDirectoryExists($dir);

    File::put($dir.'/9999999999_platforms.csv', <<<'CSV'
id,name,abbreviation,logo_url
6,PC (Microsoft Windows),PC,
CSV);

    $gamesRows = [
        'id,name,slug,summary,platforms,genres,first_release_date,total_rating,total_rating_count',
    ];

    for ($i = 1; $i <= 501; $i++) {
        $gamesRows[] = implode(',', [
            (string) $i,
            'Example Game '.$i,
            'example-game-'.$i,
            '"Example summary."',
            '"{6}"',
            '"{5}"',
            '1488844800',
            '90.0',
            '100',
        ]);
    }

    File::put($dir.'/9999999999_games.csv', implode("\n", $gamesRows)."\n");

    $screenshotRows = [
        'id,game,image_id,url,width,height',
    ];

    // 501 distinct games => triggers the internal media batch flush at 500 unique game IDs.
    for ($i = 1; $i <= 501; $i++) {
        $imageId = 'img'.$i;
        $screenshotRows[] = implode(',', [
            (string) $i,
            (string) $i,
            $imageId,
            '"//images.igdb.com/igdb/image/upload/t_thumb/'.$imageId.'.jpg"',
            '100',
            '100',
        ]);
    }

    // Add a *second* screenshot for game #1 after the flush boundary.
    $screenshotRows[] = implode(',', [
        '9999',
        '1',
        'img1b',
        '"//images.igdb.com/igdb/image/upload/t_thumb/img1b.jpg"',
        '101',
        '101',
    ]);

    File::put($dir.'/9999999999_screenshots.csv', implode("\n", $screenshotRows)."\n");

    // Sanity-check our test fixture: the first screenshot row should point at game=1.
    $handle = fopen($dir.'/9999999999_screenshots.csv', 'r');
    expect($handle)->not->toBeFalse();
    $headers = fgetcsv($handle);
    $firstRow = fgetcsv($handle);
    fclose($handle);
    expect($headers)->toBeArray();
    expect($firstRow)->toBeArray();
    $combined = array_combine($headers, $firstRow);
    expect($combined)->toBeArray();
    expect($combined['game'] ?? null)->toBe('1');

    artisan('gc:import-igdb', ['--path' => $dir, '--provider' => 'igdb', '--resume' => 0])->assertExitCode(0);

    expect(VideoGame::query()->count())->toBe(501);
    expect(DB::table('video_game_title_sources')->count())->toBeGreaterThan(0);
    expect(Image::query()->count())->toBeGreaterThan(0);

    $game1 = VideoGame::query()->where('slug', 'example-game-1')->firstOrFail();

    $minVideoGameIdWithImages = Image::query()->whereNotNull('video_game_id')->min('video_game_id');
    $maxVideoGameIdWithImages = Image::query()->whereNotNull('video_game_id')->max('video_game_id');
    expect($minVideoGameIdWithImages)->not->toBeNull();
    expect($maxVideoGameIdWithImages)->not->toBeNull();
    $sourceId = DB::table('video_game_sources')->where('provider', 'igdb')->value('id');
    expect($sourceId)->not->toBeNull();

    $titleIdForExternal1 = DB::table('video_game_title_sources')
        ->where('video_game_source_id', $sourceId)
        ->where('provider_item_id', '1')
        ->value('video_game_title_id');
    expect($titleIdForExternal1)->not->toBeNull();

    // Reproduce the command's preloadGameIdMappings() logic to validate external_id=1 is present.
    $titleMappings = DB::table('video_game_title_sources')
        ->where('video_game_source_id', $sourceId)
        ->pluck('video_game_title_id', 'provider_item_id')
        ->toArray();
    expect($titleMappings)->not->toBeEmpty();
    expect(isset($titleMappings[1]) || isset($titleMappings['1']))->toBeTrue();
    $titleIdFromMap = $titleMappings[1] ?? $titleMappings['1'] ?? null;
    expect((int) $titleIdFromMap)->toBe((int) $titleIdForExternal1);

    $titleIds = array_unique(array_values($titleMappings));
    $videoGameMappings = DB::table('video_games')
        ->whereIn('video_game_title_id', $titleIds)
        ->pluck('id', 'video_game_title_id')
        ->toArray();
    expect($videoGameMappings)->not->toBeEmpty();
    expect(isset($videoGameMappings[(int) $titleIdForExternal1]) || isset($videoGameMappings[(string) $titleIdForExternal1]))->toBeTrue();

    $videoGameIdForTitle = DB::table('video_games')->where('video_game_title_id', $titleIdForExternal1)->value('id');
    expect((int) $videoGameIdForTitle)->toBe($game1->id);

    expect(Image::query()->where('video_game_id', $game1->id)->exists())->toBeTrue();
    $image = Image::query()->where('video_game_id', $game1->id)->firstOrFail();

    $details = $image->metadata['all_details'] ?? [];
    expect($details)->toBeArray();

    $imageIds = array_values(array_unique(array_values(array_filter(array_map(
        static fn ($detail) => is_array($detail) ? ($detail['image_id'] ?? null) : null,
        $details
    ), static fn ($v) => is_string($v) && $v !== ''))));

    expect($imageIds)->toContain('img1');
    expect($imageIds)->toContain('img1b');
});

it('stores artworks in the artworks collection (not screenshots)', function (): void {
    $dir = base_path('storage/igdb-dumps-test');
    File::ensureDirectoryExists($dir);

    File::put($dir.'/9999999999_platforms.csv', <<<'CSV'
id,name,abbreviation,logo_url
6,PC (Microsoft Windows),PC,
CSV);

    File::put($dir.'/9999999999_games.csv', <<<'CSV'
id,name,slug,summary,platforms,genres,first_release_date,total_rating,total_rating_count
1,Example Game 1,example-game-1,"Example summary.","{6}","{5}",1488844800,90.0,100
CSV);

    File::put($dir.'/9999999999_artworks.csv', <<<'CSV'
id,game,image_id,url,width,height
1,1,art1,"//images.igdb.com/igdb/image/upload/t_thumb/art1.jpg",100,100
CSV);

    artisan('gc:import-igdb', ['--path' => $dir, '--provider' => 'igdb', '--resume' => 0])->assertExitCode(0);

    $game1 = VideoGame::query()->where('slug', 'example-game-1')->firstOrFail();
    $image = Image::query()->where('video_game_id', $game1->id)->firstOrFail();

    $collections = $image->metadata['collections'] ?? [];
    expect($collections)->toBeArray();
    expect($collections)->toContain('artworks');

    $details = $image->metadata['all_details'] ?? [];
    expect($details)->toBeArray();

    $detailCollections = array_values(array_unique(array_values(array_filter(array_map(
        static fn ($detail) => is_array($detail) ? ($detail['collection'] ?? null) : null,
        $details
    ), static fn ($v) => is_string($v) && $v !== ''))));

    expect($detailCollections)->toContain('artworks');
});
