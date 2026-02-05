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

// IGDB Webhooks
Route::post('/webhooks/igdb/{eventType}', [IgdbWebhookController::class, 'handle'])
    ->where('eventType', 'create|update|delete');

// --- PRIMARY NAVIGATION ---
// Original Landing Page (Game Discovery)
Route::get('/', [LandingController::class, 'index'])->name('home');

// ARC Raiders Dedicated Page
Route::get('/arc-raiders', function () {
    return Inertia::render('ArcRaiders');
})->name('arc-raiders');

// Other specialized pages
Route::get('/calculator', function () {
    return Inertia::render('Calculator');
})->name('arc-raiders.calculator');

Route::get('/resume-service', function () {
    return Inertia::render('resume-service');
})->name('resume-service');

// --- AUTH PROTECTED ---
Route::middleware(['auth', 'verified'])->group(function () {
    Route::get('/dashboard', [DashboardController::class, 'index'])->name('dashboard');
    Route::get('/dashboard/{gameId}', [DashboardController::class, 'show'])->name('dashboard.show')->whereNumber('gameId');
    Route::post('/games/{game}/like', [\App\Http\Controllers\LikeController::class, 'toggle'])->name('games.like');
});

// --- CORE PRODUCT ROUTES ---
Route::group(['prefix' => 'compare', 'as' => 'compare.'], function () {
    Route::get('/', [\App\Http\Controllers\CompareController::class, 'index'])->name('index');
    Route::get('/stats', [\App\Http\Controllers\CompareController::class, 'stats'])->name('stats');
    Route::get('/entries', [\App\Http\Controllers\CompareController::class, 'entries'])->name('entries');
    Route::get('/spotlight', [\App\Http\Controllers\CompareController::class, 'spotlight'])->name('spotlight');
});

Route::get('/games', [VideoGameController::class, 'index'])->name('games.index');
Route::get('/games/{game}', [VideoGameController::class, 'show'])->name('games.show');
Route::get('/api/games/{game}/transition-media', [VideoGameController::class, 'transitionMedia'])->name('games.transition-media');

// AI Assistant API
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

Route::get('/api/debug/spotlight', [LandingController::class, 'debugSpotlight']);