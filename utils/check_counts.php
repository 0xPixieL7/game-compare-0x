<?php

use Illuminate\Support\Facades\DB;

$tables = [
    'video_game_sources',
    'video_game_title_sources',
    'video_game_titles',
    'video_games',
    'products',
    'images',
    'videos',
    'video_game_prices'
];

foreach ($tables as $table) {
    try {
        $count = DB::table($table)->count();
        echo "Table '{$table}': {$count} rows\n";
    } catch (\Exception $e) {
        echo "Table '{$table}': Error - " . $e->getMessage() . "\n";
    }
}

// Check join for Price Map
echo "\nChecking Product -> Game Map join count:\n";
$joinCount = DB::table('video_game_titles')
    ->join('video_games', 'video_games.video_game_title_id', '=', 'video_game_titles.id')
    ->whereNotNull('video_game_titles.product_id')
    ->count();
echo "Join count: {$joinCount}\n";

// Check a sample if count > 0
if ($joinCount > 0) {
    $sample = DB::table('video_game_titles')
        ->join('video_games', 'video_games.video_game_title_id', '=', 'video_game_titles.id')
        ->select('video_game_titles.product_id', 'video_games.id as video_game_id')
        ->first();
    echo "Sample: Product ID {$sample->product_id} -> Game ID {$sample->video_game_id}\n";
} else {
    echo "Debugging join failure:\n";
    $titlesCount = DB::table('video_game_titles')->count();
    $gamesCount = DB::table('video_games')->count();
    $titlesWithProductId = DB::table('video_game_titles')->whereNotNull('product_id')->count();
    
    echo "Titles total: $titlesCount\n";
    echo "Games total: $gamesCount\n";
    echo "Titles with Product ID: $titlesWithProductId\n";
    
    // Check if any video_game_title_id in video_games exists in video_game_titles
    $orphanGames = DB::table('video_games')
        ->leftJoin('video_game_titles', 'video_games.video_game_title_id', '=', 'video_game_titles.id')
        ->whereNull('video_game_titles.id')
        ->count();
    echo "Orphan Games (invalid title_id): $orphanGames\n";
}
