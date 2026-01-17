<?php

require __DIR__.'/vendor/autoload.php';

$app = require_once __DIR__.'/bootstrap/app.php';
$kernel = $app->make(Illuminate\Contracts\Console\Kernel::class);
$kernel->bootstrap();

use Illuminate\Support\Facades\DB;

// Clear cache first
Illuminate\Support\Facades\Cache::flush();
echo "Cache cleared.\n\n";

// Enable query logging
DB::enableQueryLog();

$start = microtime(true);

try {
    $controller = new App\Http\Controllers\LandingController(
        app(App\Services\ExchangeRates\TradingViewClient::class)
    );

    $request = Illuminate\Http\Request::create('/', 'GET');
    $response = $controller->index($request);

    $elapsed = (microtime(true) - $start) * 1000;

    echo "✓ Page loaded successfully\n";
    echo 'Total Time: '.round($elapsed)."ms\n\n";

    // Analyze queries
    $queries = DB::getQueryLog();
    echo 'Total Queries: '.count($queries)."\n\n";

    // Group by query time
    $sorted = collect($queries)->sortByDesc('time');

    echo "Top 10 Slowest Queries:\n";
    echo str_repeat('=', 80)."\n";

    foreach ($sorted->take(10) as $i => $query) {
        $num = $i + 1;
        $time = round($query['time'] ?? 0, 2);
        $sql = isset($query['query']) ? substr($query['query'], 0, 100) : (substr($query['sql'] ?? 'unknown', 0, 100));
        echo "{$num}. {$time}ms - {$sql}...\n";
    }

    echo "\n";

    $totalQueryTime = collect($queries)->sum('time');
    echo 'Total Query Time: '.round($totalQueryTime)."ms\n";
    echo 'PHP Processing Time: '.round($elapsed - $totalQueryTime)."ms\n";

} catch (Exception $e) {
    echo '❌ Error: '.$e->getMessage()."\n";
    echo 'File: '.$e->getFile().':'.$e->getLine()."\n";
    echo "\n".$e->getTraceAsString()."\n";
}
