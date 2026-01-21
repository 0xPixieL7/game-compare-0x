<?php

use App\Http\Controllers\AIAssistantController;
use App\Http\Controllers\DashboardController;
use App\Http\Controllers\IgdbWebhookController;
use App\Http\Controllers\LandingController;
use App\Http\Controllers\VideoGameController;
use Illuminate\Support\Facades\Route;
use Inertia\Inertia;

// Health check endpoint for Railway/Docker
Route::get('/up', function () {
    return response()->json(['status' => 'ok'], 200);
});

// IGDB Webhooks (no CSRF protection needed - verified via X-Secret header)
Route::post('/webhooks/igdb/{eventType}', [IgdbWebhookController::class, 'handle'])
    ->where('eventType', 'create|update|delete');

Route::get('/', [LandingController::class, 'index'])->name('home');

Route::get('/dashboard', [DashboardController::class, 'index'])->name('dashboard');
Route::get('/dashboard/{gameId}', [DashboardController::class, 'show'])->name('dashboard.show')->whereNumber('gameId');

// Debug route
Route::get('/debug/{gameId}', function ($gameId) {
    $gameId = (int) $gameId;
    $start = microtime(true);

    $game = DB::table('video_games')
        ->select(['video_games.id', 'video_games.name'])
        ->where('video_games.id', $gameId)
        ->first();

    $queryTime = microtime(true) - $start;

    return response()->json([
        'game' => $game,
        'query_time' => $queryTime.'s',
        'status' => 'success',
    ]);
})->whereNumber('gameId');

Route::group(['prefix' => 'compare', 'as' => 'compare.'], function () {
    Route::get('/', [\App\Http\Controllers\CompareController::class, 'index'])->name('index'); // This becomes compare.index (alias for /compare)
    Route::get('/stats', [\App\Http\Controllers\CompareController::class, 'stats'])->name('stats');
    Route::get('/entries', [\App\Http\Controllers\CompareController::class, 'entries'])->name('entries');
    Route::get('/spotlight', [\App\Http\Controllers\CompareController::class, 'spotlight'])->name('spotlight');
});
// Basic Alias for legacy support if needed, pointing to same controller
if (! Route::has('compare')) {
    Route::get('/compare', [\App\Http\Controllers\CompareController::class, 'index'])->name('compare');
}

Route::get('/games', [VideoGameController::class, 'index'])->name('games.index');
Route::get('/games/{game}', [VideoGameController::class, 'show'])->name('games.show');

// AI Assistant API Routes
Route::prefix('api/ai')->group(function () {
    Route::post('/generate-model', [AIAssistantController::class, 'generateModel']);
    Route::post('/generate-migration', [AIAssistantController::class, 'generateMigration']);
    Route::post('/generate-tests', [AIAssistantController::class, 'generateTests']);
    Route::post('/validate-schema', [AIAssistantController::class, 'validateSchema']);
    Route::post('/optimize-query', [AIAssistantController::class, 'optimizeQuery']);
    Route::post('/auto-fix-types', [AIAssistantController::class, 'autoFixTypes']);
    Route::post('/generate-api-docs', [AIAssistantController::class, 'generateApiDocs']);
});
// Legal
Route::get('/privacy-policy', function () {
    return Inertia::render('Legal/PrivacyPolicy');
})->name('privacy-policy');

Route::get('/terms-of-service', function () {
    return Inertia::render('Legal/TermsOfService');
})->name('terms-of-service');
