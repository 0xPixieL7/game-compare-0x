<?php

require __DIR__.'/vendor/autoload.php';

$app = require_once __DIR__.'/bootstrap/app.php';
$kernel = $app->make(Illuminate\Contracts\Http\Kernel::class);

echo "Testing Landing Page\n";
echo "====================\n\n";

try {
    $request = Illuminate\Http\Request::create('/', 'GET');
    $response = $kernel->handle($request);

    echo 'Status: '.$response->getStatusCode()."\n";

    if ($response->getStatusCode() >= 400) {
        echo "\n=== ERROR RESPONSE ===\n";
        echo $response->getContent();
        echo "\n";
    } else {
        echo "SUCCESS!\n";
    }

    $kernel->terminate($request, $response);
} catch (\Exception $e) {
    echo "EXCEPTION CAUGHT:\n";
    echo $e->getMessage()."\n\n";
    echo 'File: '.$e->getFile().':'.$e->getLine()."\n\n";
    echo "Stack trace:\n";
    echo $e->getTraceAsString()."\n";
}
