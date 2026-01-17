<?php

declare(strict_types=1);

use App\Models\Country;
use App\Models\Currency;
use Database\Seeders\CountriesSeeder;
use Illuminate\Support\Facades\Schema;

uses()->group('database');

test('countries migration creates table with correct schema', function () {
    expect(Schema::hasTable('countries'))->toBeTrue();
    expect(Schema::hasColumns('countries', ['id', 'code', 'name', 'currency_id', 'region', 'metadata']))->toBeTrue();
});

test('countries seeder imports 30 countries from CSV', function () {
    // Run seeder
    CountriesSeeder::make()->run();

    // Should have exactly 30 countries
    expect(Country::count())->toBe(30);
});

test('all countries have valid currency foreign keys', function () {
    CountriesSeeder::make()->run();

    $countries = Country::all();

    // Every country should have a valid currency_id
    $countries->each(function (Country $country) {
        expect($country->currency)->toBeInstanceOf(Currency::class);
        expect($country->currency_id)->toBeGreaterThan(0);
    });
});

test('country codes are unique and properly formatted', function () {
    CountriesSeeder::make()->run();

    $countries = Country::all();

    // All codes should be 2 characters
    $countries->each(function (Country $country) {
        expect(strlen($country->code))->toBe(2);
        expect($country->code)->toBeUppercase();
    });

    // Should have 30 unique codes
    $uniqueCodes = $countries->pluck('code')->unique();
    expect($uniqueCodes)->toHaveCount(30);
});

test('expected countries are present', function () {
    CountriesSeeder::make()->run();

    $expectedCountries = ['US', 'CA', 'GB', 'JP', 'AU', 'FR', 'DE', 'BR', 'RU', 'ZA'];

    foreach ($expectedCountries as $code) {
        expect(Country::where('code', $code)->exists())->toBeTrue(
            "Country {$code} should exist in the database"
        );
    }
});

test('currency distribution matches expected values', function () {
    CountriesSeeder::make()->run();

    // Group countries by currency
    $distribution = Country::query()
        ->selectRaw('currency_id, COUNT(*) as count')
        ->groupBy('currency_id')
        ->pluck('count', 'currency_id');

    // EUR should have the most countries (8)
    $eurCurrency = Currency::where('code', 'EUR')->first();
    expect($distribution[$eurCurrency->id] ?? 0)->toBe(8);

    // USD should have 5 countries
    $usdCurrency = Currency::where('code', 'USD')->first();
    expect($distribution[$usdCurrency->id] ?? 0)->toBe(5);
});

test('BTC currency is not assigned to any country', function () {
    CountriesSeeder::make()->run();

    $btcCurrency = Currency::where('code', 'BTC')->first();

    // BTC should exist but have no countries
    expect($btcCurrency)->not->toBeNull();
    expect($btcCurrency->countries()->count())->toBe(0);
});

test('currency relationship works correctly', function () {
    CountriesSeeder::make()->run();

    $usa = Country::where('code', 'US')->first();

    expect($usa)->not->toBeNull();
    expect($usa->currency)->toBeInstanceOf(Currency::class);
    expect($usa->currency->code)->toBe('USD');
});

test('countries with regions are categorized correctly', function () {
    CountriesSeeder::make()->run();

    $northAmerican = Country::where('region', 'North America')->get();
    $european = Country::where('region', 'Europe')->get();

    // Should have some countries in each major region
    expect($northAmerican)->not->toBeEmpty();
    expect($european)->not->toBeEmpty();

    // US and CA should be North American
    expect($northAmerican->pluck('code'))->toContain('US');
    expect($northAmerican->pluck('code'))->toContain('CA');
});
