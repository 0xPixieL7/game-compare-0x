<?php

declare(strict_types=1);

use App\Models\Product;
use App\Models\SkuRegion;
use App\Models\VideoGame;
use App\Models\VideoGameSource;
use App\Services\Provider\ProviderDiscoveryService;
use Illuminate\Support\Facades\Artisan;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\File;

test('csv import normalizes thegamesdb -> tgdb for sources + title mappings', function () {
    // Hard isolate: remove any prior rows that could trip unique/provider checks.
    DB::table('video_game_title_sources')->where('video_game_title_id', 900010)->delete();
    DB::table('video_games')->where('video_game_title_id', 900010)->delete();
    DB::table('video_game_titles')->where('id', 900010)->delete();
    DB::table('products')->where('id', 900010)->delete();
    DB::table('video_game_sources')->whereIn('provider', ['tgdb', 'thegamesdb'])->delete();
    DB::table('video_game_sources')->whereIn('id', [900001, 900002])->delete();

    $basePath = storage_path('framework/testing/csv-import');
    File::ensureDirectoryExists($basePath);

    File::put($basePath.'/video_game_sources.csv', implode("\n", [
        'id,provider_key,display_name,category,slug,metadata,created_at,updated_at',
        '900001,igdb,IGDB,metadata,igdb,{},2026-01-01 00:00:00,2026-01-01 00:00:00',
        '900002,thegamesdb,TheGamesDB,metadata,thegamesdb,{},2026-01-01 00:00:00,2026-01-01 00:00:00',
        '',
    ]));

    File::put($basePath.'/products_TRANSFORMED.csv', implode("\n", [
        'id,type,name,slug,platform,category,title,normalized_title,synopsis,release_date,popularity_score,rating,external_ids,metadata,created_at,updated_at',
        '900010,video_game,Test Game,test-game,PC,Game,Test Game,test-game,Test synopsis,2026-01-01,0.5,80,{"tgdb":777},{"sources":{"tgdb":{"overview":"From legacy export"}}},2026-01-01 00:00:00,2026-01-01 00:00:00',
        '',
    ]));

    File::put($basePath.'/video_game_titles_TRANSFORMED.csv', implode("\n", [
        'id,product_id,name,normalized_title,slug,providers,created_at,updated_at',
        '900010,900010,Test Game,test-game,test-game,["tgdb"],2026-01-01 00:00:00,2026-01-01 00:00:00',
        '',
    ]));

    File::put($basePath.'/video_games_TRANSFORMED.csv', implode("\n", [
        'id,video_game_title_id,slug,provider,external_id,name,description,summary,storyline,url,release_date,platform,rating,rating_count,developer,publisher,genre,media,source_payload,created_at,updated_at',
        '900010,900010,test-game,tgdb,777,Test Game,,Test synopsis,,,'
            .'2026-01-01,["PC"],,,,[],{},{},' // genre/media/source_payload are JSON
            .'2026-01-01 00:00:00,2026-01-01 00:00:00',
        '',
    ]));

    File::put($basePath.'/video_game_title_sources_TRANSFORMED.csv', implode("\n", [
        'video_game_title_id,video_game_source_id,provider,external_id,slug,name,description,release_date,provider_item_id,platform,rating,rating_count,developer,publisher,genre,raw_payload,created_at,updated_at',
        // Intentionally legacy provider name: ImportTransformedCsvs must normalize this to tgdb.
        '900010,900002,thegamesdb,777,test-game,Test Game,From legacy export,2026-01-01,777,,,,,,[],{},2026-01-01 00:00:00,2026-01-01 00:00:00',
        '',
    ]));

    Artisan::call('import:transformed-csvs', ['--path' => $basePath, '--table' => 'video_game_sources', '--batch' => 50]);
    Artisan::call('import:transformed-csvs', ['--path' => $basePath, '--table' => 'products', '--batch' => 50]);
    Artisan::call('import:transformed-csvs', ['--path' => $basePath, '--table' => 'video_game_titles', '--batch' => 50]);
    Artisan::call('import:transformed-csvs', ['--path' => $basePath, '--table' => 'video_games', '--batch' => 50]);
    Artisan::call('import:transformed-csvs', ['--path' => $basePath, '--table' => 'video_game_title_sources', '--batch' => 50]);

    $tgdb = VideoGameSource::query()->where('id', 900002)->first();
    expect($tgdb)->not->toBeNull();
    expect($tgdb->provider)->toBe('tgdb');
    expect($tgdb->provider_key)->toBe('tgdb');

    $game = VideoGame::query()->where('id', 900010)->first();
    expect($game)->not->toBeNull();

    $discovery = app(ProviderDiscoveryService::class);
    expect($discovery->getExternalId($game, 'tgdb'))->toBe(777);
});

test('pricing snapshot writes offers into products.metadata', function () {
    DB::table('video_game_prices')->where('product_id', 900020)->delete();
    DB::table('video_games')->where('id', 900021)->delete();
    DB::table('video_game_titles')->where('id', 900021)->delete();
    DB::table('products')->where('id', 900020)->delete();
    DB::table('products')->where('slug', 'pricing-test-game')->delete();

    DB::table('products')->insert([
        'id' => 900020,
        'type' => 'video_game',
        'name' => 'Pricing Test Game',
        'slug' => 'pricing-test-game',
        'metadata' => json_encode([]),
        'created_at' => now(),
        'updated_at' => now(),
    ]);

    $product = Product::query()->findOrFail(900020);

    DB::table('video_game_titles')->insert([
        'id' => 900021,
        'product_id' => 900020,
        'name' => 'Pricing Test Game',
        'slug' => 'pricing-test-game',
        'normalized_title' => 'pricing-test-game',
        'providers' => json_encode(['legacy']),
        'created_at' => now(),
        'updated_at' => now(),
    ]);

    DB::table('video_games')->insert([
        'id' => 900021,
        'video_game_title_id' => 900021,
        'slug' => 'pricing-test-game',
        'provider' => 'legacy',
        'external_id' => 900021,
        'name' => 'Pricing Test Game',
        'created_at' => now(),
        'updated_at' => now(),
    ]);

    SkuRegion::query()->create([
        'video_game_id' => 900021,
        'product_id' => 900020,
        'currency' => 'USD',
        'country_code' => 'US',
        'amount_minor' => 4999,
        'recorded_at' => now()->subHour(),
        'retailer' => 'steam',
        'url' => 'https://example.test/item',
        'tax_inclusive' => false,
        'region_code' => 'US',
        'sku' => 'SKU-1',
        'is_active' => true,
    ]);

    SkuRegion::query()->create([
        'video_game_id' => 900021,
        'product_id' => 900020,
        'currency' => 'USD',
        'country_code' => 'US',
        'amount_minor' => 3999,
        'recorded_at' => now(),
        'retailer' => 'epic',
        'url' => 'https://example.test/item2',
        'tax_inclusive' => false,
        'region_code' => 'US',
        'sku' => 'SKU-2',
        'is_active' => true,
    ]);

    $product->refreshPricingSnapshot();
    $product->refresh();

    $metadata = is_array($product->metadata) ? $product->metadata : [];
    expect($metadata)->toHaveKey('pricing');
    expect($metadata['pricing'])->toHaveKey('offers');
    expect($metadata['pricing']['offers'])->toBeArray();
    expect($metadata['pricing']['best_by_currency']['USD']['amount_minor'])->toBe(3999);
});
