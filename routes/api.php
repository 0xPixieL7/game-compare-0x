<?php

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
