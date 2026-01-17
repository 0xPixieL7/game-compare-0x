<?php

require __DIR__.'/vendor/autoload.php';

$app = require_once __DIR__.'/bootstrap/app.php';
$kernel = $app->make(Illuminate\Contracts\Http\Kernel::class);

echo "Testing Landing Page Performance\n";
echo "=================================\n\n";

// Warm up (ignore this request)
echo "Warming up...\n";
$request = Illuminate\Http\Request::create('/', 'GET');
$response = $kernel->handle($request);
$kernel->terminate($request, $response);
echo "Warmup complete.\n\n";

// Test 1: First load (cold cache)
echo "Test 1: Cold Cache (first load after cache clear)\n";
echo "-------------------------------------------------\n";

$start = microtime(true);
$request = Illuminate\Http\Request::create('/', 'GET');
$response = $kernel->handle($request);
$end = microtime(true);
$duration = round(($end - $start) * 1000, 2);

echo 'Status: '.$response->getStatusCode()."\n";
echo "Duration: {$duration}ms\n";
echo $duration < 1000 ? "✅ PASS (<1s)\n" : "❌ FAIL (>1s)\n";

$kernel->terminate($request, $response);

echo "\n";

// Test 2: Cached load
echo "Test 2: Warm Cache (subsequent load)\n";
echo "-------------------------------------\n";

$start = microtime(true);
$request = Illuminate\Http\Request::create('/', 'GET');
$response = $kernel->handle($request);
$end = microtime(true);
$duration = round(($end - $start) * 1000, 2);

echo 'Status: '.$response->getStatusCode()."\n";
echo "Duration: {$duration}ms\n";
echo $duration < 100 ? "✅ EXCELLENT (<100ms)\n" : ($duration < 1000 ? "✅ PASS (<1s)\n" : "❌ FAIL (>1s)\n");

$kernel->terminate($request, $response);

echo "\n";
echo "Performance Test Complete!\n";
