<?php

declare(strict_types=1);

use App\Models\VideoGame;
use Illuminate\Support\Facades\File;
use Illuminate\Support\Facades\Http;

use function Pest\Laravel\artisan;

afterEach(function (): void {
    $dir = base_path('storage/igdb-dumps-test');

    if (File::exists($dir)) {
        File::deleteDirectory($dir);
    }
});

it('imports IGDB CSV platforms and genres as arrays', function () {
    $dir = base_path('storage/igdb-dumps-test');

    File::ensureDirectoryExists($dir);

    // Include a schema artifact to ensure the importer never selects it.
    File::put($dir.'/games_schema.json', json_encode(['schema' => true]));

    // Minimal platform reference dump.
    $platformsCsv = <<<'CSV'
id,name,abbreviation,logo_url
6,PC (Microsoft Windows),PC,
48,PlayStation 4,PS4,
CSV;

    File::put($dir.'/9999999999_platforms.csv', $platformsCsv);

    // Minimal games dump. IGDB CSV commonly represents arrays as "{...}".
    $gamesCsv = <<<'CSV'
id,name,slug,summary,platforms,genres,first_release_date,total_rating,total_rating_count
1,Example Game,example-game,"Example summary.","{6,48}","{5,12}",1488844800,90.0,100
CSV;

    File::put($dir.'/9999999999_games.csv', $gamesCsv);

    artisan('gc:import-igdb', ['--path' => $dir, '--provider' => 'igdb', '--limit' => 1])->assertExitCode(0);

    $game = VideoGame::query()->firstOrFail();

    expect($game->platform)->toBeArray();
    expect($game->platform)->toContain('PC');
    expect($game->platform)->toContain('PlayStation 4');

    expect($game->genre)->toBeArray();
    expect($game->genre)->toEqual([5, 12]);
});

it('accepts --path pointing directly to a games dump file', function () {
    $dir = base_path('storage/igdb-dumps-test');

    File::ensureDirectoryExists($dir);

    $platformsCsv = <<<'CSV'
id,name,abbreviation,logo_url
6,PC (Microsoft Windows),PC,
48,PlayStation 4,PS4,
CSV;

    File::put($dir.'/9999999999_platforms.csv', $platformsCsv);

    $gamesCsv = <<<'CSV'
id,name,slug,summary,platforms,genres,first_release_date,total_rating,total_rating_count
1,Example Game,example-game,"Example summary.","{6,48}","{5,12}",1488844800,90.0,100
CSV;

    $gamesFile = $dir.'/9999999999_games.csv';
    File::put($gamesFile, $gamesCsv);

    artisan('gc:import-igdb', ['--path' => $gamesFile, '--provider' => 'igdb', '--limit' => 1])->assertExitCode(0);

    $game = VideoGame::query()->firstOrFail();

    expect($game->platform)->toBeArray();
    expect($game->platform)->toContain('PC');
    expect($game->platform)->toContain('PlayStation 4');

    expect($game->genre)->toBeArray();
    expect($game->genre)->toEqual([5, 12]);
});

it('maps genre IDs and involved companies into video_games fields when reference dumps exist', function () {
    $dir = base_path('storage/igdb-dumps-test');

    File::ensureDirectoryExists($dir);

    File::put($dir.'/9999999999_platforms.csv', <<<'CSV'
id,name,abbreviation,logo_url
6,PC (Microsoft Windows),PC,
CSV);

    File::put($dir.'/9999999999_genres.csv', <<<'CSV'
id,name
5,Shooter
12,Role-playing (RPG)
CSV);

    File::put($dir.'/9999999999_companies.csv', <<<'CSV'
id,name
2001,Naughty Dog
2002,Sony Interactive Entertainment
CSV);

    File::put($dir.'/9999999999_involved_companies.csv', <<<'CSV'
id,company,developer,publisher
1001,2001,1,0
1002,2002,0,1
CSV);

    File::put($dir.'/9999999999_games.csv', <<<'CSV'
id,name,slug,summary,platforms,genres,involved_companies,first_release_date,total_rating,total_rating_count
1,Example Game,example-game,"Example summary.","{6}","{5,12}","{1001,1002}",1488844800,90.0,100
CSV);

    artisan('gc:import-igdb', ['--path' => $dir, '--provider' => 'igdb', '--limit' => 1])->assertExitCode(0);

    $game = VideoGame::query()->firstOrFail();

    expect($game->genre)->toBeArray();
    expect($game->genre)->toContain('Shooter');
    expect($game->genre)->toContain('Role-playing (RPG)');

    expect($game->developer)->toBe('Naughty Dog');
    expect($game->publisher)->toBe('Sony Interactive Entertainment');
});

it('auto-fetches reference dumps when timestamped reference CSVs are empty', function () {
    $dir = base_path('storage/igdb-dumps-test');

    File::ensureDirectoryExists($dir);

    // Header-only reference dumps (what we have in the repo right now).
    File::put($dir.'/9999999999_platforms.csv', "id,name,abbreviation,logo_url\n");
    File::put($dir.'/9999999999_genres.csv', "id,name\n");
    File::put($dir.'/9999999999_companies.csv', "id,name\n");
    File::put($dir.'/9999999999_involved_companies.csv', "id,company,developer,publisher\n");

    File::put($dir.'/9999999999_games.csv', <<<'CSV'
id,name,slug,summary,platforms,genres,involved_companies,first_release_date,total_rating,total_rating_count
1,Example Game,example-game,"Example summary.","{6}","{5}","{1001}",1488844800,90.0,100
CSV);

    config()->set('services.igdb.client_id', 'test-client-id');
    config()->set('services.igdb.client_secret', 'test-client-secret');

    Http::fake(function ($request) {
        $url = $request->url();

        if ($url === 'https://id.twitch.tv/oauth2/token') {
            return Http::response(['access_token' => 'test-token'], 200);
        }

        if (str_starts_with($url, 'https://api.igdb.com/v4/dumps/')) {
            $endpoint = basename(parse_url($url, PHP_URL_PATH) ?? '');

            $files = [
                'platforms' => ['file' => '9999999999_platforms.csv', 's3' => 'https://s3.test/platforms.csv', 'schema' => ['id', 'name', 'abbreviation', 'logo_url']],
                'genres' => ['file' => '9999999999_genres.csv', 's3' => 'https://s3.test/genres.csv', 'schema' => ['id', 'name']],
                'companies' => ['file' => '9999999999_companies.csv', 's3' => 'https://s3.test/companies.csv', 'schema' => ['id', 'name']],
                'involved_companies' => ['file' => '9999999999_involved_companies.csv', 's3' => 'https://s3.test/involved_companies.csv', 'schema' => ['id', 'company', 'developer', 'publisher']],
            ];

            if (! isset($files[$endpoint])) {
                return Http::response(['error' => 'unknown endpoint'], 404);
            }

            return Http::response([
                'endpoint' => $endpoint,
                'file_name' => $files[$endpoint]['file'],
                'size_bytes' => 42,
                'updated_at' => 0,
                'schema_version' => 1,
                's3_url' => $files[$endpoint]['s3'],
                'schema' => ['columns' => $files[$endpoint]['schema']],
            ], 200);
        }

        if ($url === 'https://s3.test/platforms.csv') {
            return Http::response("id,name,abbreviation,logo_url\n6,PC (Microsoft Windows),PC,\n", 200);
        }

        if ($url === 'https://s3.test/genres.csv') {
            return Http::response("id,name\n5,Shooter\n", 200);
        }

        if ($url === 'https://s3.test/companies.csv') {
            return Http::response("id,name\n2001,Naughty Dog\n", 200);
        }

        if ($url === 'https://s3.test/involved_companies.csv') {
            return Http::response("id,company,developer,publisher\n1001,2001,1,0\n", 200);
        }

        return Http::response(['error' => 'unexpected request', 'url' => $url], 500);
    });

    artisan('gc:import-igdb', ['--path' => $dir, '--provider' => 'igdb', '--limit' => 1])->assertExitCode(0);

    $game = VideoGame::query()->firstOrFail();

    expect($game->platform)->toBeArray();
    expect($game->platform)->toContain('PC');

    expect($game->genre)->toBeArray();
    expect($game->genre)->toContain('Shooter');

    expect($game->developer)->toBe('Naughty Dog');
});
