<?php

use App\Http\Controllers\AIAssistantController;
use App\Http\Controllers\DashboardController;
use App\Http\Controllers\LandingController;
use App\Http\Controllers\VideoGameController;
use Illuminate\Support\Facades\Route;

// Health check endpoint for Railway/Docker
Route::get('/up', function () {
    return response()->json(['status' => 'ok'], 200);
});

Route::get('/', [LandingController::class, 'index'])->name('home');

Route::get('/dashboard', [DashboardController::class, 'index'])->name('dashboard');
Route::get('/dashboard/{gameId}', [DashboardController::class, 'show'])->name('dashboard.show');

// Debug route
Route::get('/debug/{gameId}', function ($gameId) {
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
});

Route::get('/compare', [\App\Http\Controllers\CompareController::class, 'index'])->name('compare');

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
