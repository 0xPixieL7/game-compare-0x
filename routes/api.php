<?php

use App\Http\Requests\StoreVideoGamePriceRequest;
use App\Models\VideoGame;
use App\Models\VideoGamePrice;
use Illuminate\Http\Request;
use Illuminate\Support\Facades\Route;

Route::get('/user', function (Request $request) {
    return $request->user();
})->middleware('auth:sanctum');

Route::post('/rust/status', function (Request $request) {
    \Illuminate\Support\Facades\Log::info('Rust API Server Status Alert:', $request->all());

    return response()->json(['message' => 'Status received', 'data' => $request->all()]);
});

// Compare page API endpoints
Route::prefix('compare')->name('api.compare.')->group(function () {
    Route::get('/stats', function () {
        return response()->json([
            'total_products' => \App\Models\Product::count(),
            'total_prices' => \App\Models\SkuRegion::count(),
            'regions' => \App\Models\SkuRegion::distinct('region_code')->count('region_code'),
        ]);
    })->name('stats');

    Route::get('/entries', function (Request $request) {
        $perPage = $request->get('per_page', 20);
        $products = \App\Models\Product::with('skuRegions')
            ->orderByDesc('release_date')
            ->paginate($perPage);

        return response()->json($products);
    })->name('entries');

    Route::get('/spotlight', function () {
        $spotlight = \App\Models\Product::query()
            ->orderByDesc('popularity_score')
            ->limit(10)
            ->get(['id', 'name', 'slug', 'platform', 'category', 'rating']);

        return response()->json($spotlight);
    })->name('spotlight');
});

Route::prefix('games/{game}/prices')->name('api.games.prices.')->group(function () {
    Route::get('/', function (VideoGame $game, Request $request) {
        $limit = min((int) $request->integer('limit', 100), 500);
        $activeOnly = $request->boolean('active', true);
        $currency = $request->string('currency')->upper()->value();
        $countryCode = $request->string('country_code')->upper()->value();
        $retailer = $request->string('retailer')->value();

        $query = VideoGamePrice::query()
            ->where('video_game_id', $game->id)
            ->orderBy('amount_minor', 'asc')
            ->orderByDesc('recorded_at');

        if ($activeOnly) {
            $query->where('is_active', true);
        }

        if ($currency !== '') {
            $query->where('currency', $currency);
        }

        if ($countryCode !== '') {
            $query->where('country_code', $countryCode);
        }

        if ($retailer !== '') {
            $query->where('retailer', $retailer);
        }

        $prices = $query->limit($limit)->get([
            'id',
            'video_game_id',
            'retailer',
            'country_code',
            'region_code',
            'currency',
            'amount_minor',
            'recorded_at',
            'url',
            'tax_inclusive',
            'condition',
            'sku',
            'is_active',
            'metadata',
        ]);

        return response()->json($prices->map(static function (VideoGamePrice $price) {
            return [
                'id' => $price->id,
                'video_game_id' => $price->video_game_id,
                'retailer' => $price->retailer,
                'country_code' => $price->country_code,
                'region_code' => $price->region_code,
                'currency' => $price->currency,
                'amount_minor' => $price->amount_minor,
                'amount' => $price->amount_minor / 100,
                'recorded_at' => $price->recorded_at?->toIso8601String(),
                'url' => $price->url,
                'tax_inclusive' => $price->tax_inclusive,
                'condition' => $price->condition,
                'sku' => $price->sku,
                'is_active' => $price->is_active,
                'metadata' => $price->metadata,
            ];
        }));
    })->name('index');

    Route::get('/latest', function (VideoGame $game, Request $request) {
        $activeOnly = $request->boolean('active', true);
        $currency = $request->string('currency')->upper()->value();
        $countryCode = $request->string('country_code')->upper()->value();
        $retailer = $request->string('retailer')->value();

        $query = VideoGamePrice::query()
            ->where('video_game_id', $game->id)
            ->orderByDesc('recorded_at');

        if ($activeOnly) {
            $query->where('is_active', true);
        }

        if ($currency !== '') {
            $query->where('currency', $currency);
        }

        if ($countryCode !== '') {
            $query->where('country_code', $countryCode);
        }

        if ($retailer !== '') {
            $query->where('retailer', $retailer);
        }

        /** @var VideoGamePrice|null $price */
        $price = $query->first([
            'id',
            'video_game_id',
            'retailer',
            'country_code',
            'region_code',
            'currency',
            'amount_minor',
            'recorded_at',
            'url',
            'tax_inclusive',
            'condition',
            'sku',
            'is_active',
            'metadata',
        ]);

        if (! $price) {
            return response()->json(['message' => 'No prices found'], 404);
        }

        return response()->json([
            'id' => $price->id,
            'video_game_id' => $price->video_game_id,
            'retailer' => $price->retailer,
            'country_code' => $price->country_code,
            'region_code' => $price->region_code,
            'currency' => $price->currency,
            'amount_minor' => $price->amount_minor,
            'amount' => $price->amount_minor / 100,
            'recorded_at' => $price->recorded_at?->toIso8601String(),
            'url' => $price->url,
            'tax_inclusive' => $price->tax_inclusive,
            'condition' => $price->condition,
            'sku' => $price->sku,
            'is_active' => $price->is_active,
            'metadata' => $price->metadata,
        ]);
    })->name('latest');

    Route::post('/', function (VideoGame $game, StoreVideoGamePriceRequest $request) {
        $validated = $request->validated();

        $price = VideoGamePrice::query()->updateOrCreate(
            [
                'video_game_id' => $game->id,
                'retailer' => $validated['retailer'],
                'country_code' => strtoupper($validated['country_code']),
            ],
            [
                'currency' => strtoupper($validated['currency']),
                'amount_minor' => (int) $validated['amount_minor'],
                'recorded_at' => $validated['recorded_at'] ?? now(),
                'url' => $validated['url'] ?? null,
                'tax_inclusive' => (bool) ($validated['tax_inclusive'] ?? false),
                'region_code' => isset($validated['region_code']) ? strtoupper((string) $validated['region_code']) : null,
                'condition' => $validated['condition'] ?? null,
                'sku' => $validated['sku'] ?? null,
                'is_active' => (bool) ($validated['is_active'] ?? true),
                'metadata' => $validated['metadata'] ?? null,
            ]
        );

        return response()->json([
            'id' => $price->id,
            'video_game_id' => $price->video_game_id,
            'retailer' => $price->retailer,
            'country_code' => $price->country_code,
            'region_code' => $price->region_code,
            'currency' => $price->currency,
            'amount_minor' => $price->amount_minor,
            'amount' => $price->amount_minor / 100,
            'recorded_at' => $price->recorded_at?->toIso8601String(),
            'url' => $price->url,
            'tax_inclusive' => $price->tax_inclusive,
            'condition' => $price->condition,
            'sku' => $price->sku,
            'is_active' => $price->is_active,
            'metadata' => $price->metadata,
        ], 201);
    })->middleware('auth:sanctum')->name('store');
});

Route::get('games/{game}/chart', [App\Http\Controllers\Api\GameChartController::class, 'priceHistory'])->name('api.games.chart');

