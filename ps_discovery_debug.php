<?php

require 'vendor/autoload.php';

use PlaystationStoreApi\Client;
use PlaystationStoreApi\Enum\CategoryEnum;
use PlaystationStoreApi\Enum\RegionEnum;
use PlaystationStoreApi\Request\RequestProductList;

$region = RegionEnum::JAPAN;
$category = CategoryEnum::PS5_GAMES; // Testing PS5 in Japan

$guzzle = new \GuzzleHttp\Client(['base_uri' => 'https://web.np.playstation.com/api/graphql/v1/']);
$client = new Client($region, $guzzle);

echo 'Testing Discovery for Region: '.$region->value."\n";
echo 'Category: '.$category->name.' ('.$category->value.")\n";

try {
    $request = RequestProductList::createFromCategory($category);
    $response = $client->get($request);

    $grid = $response['data']['categoryGridRetrieve'] ?? null;
    if (! $grid) {
        echo "NO GRID FOUND\n";
        echo json_encode($response, JSON_PRETTY_PRINT)."\n";
    } else {
        $concepts = $grid['concepts'] ?? [];
        echo 'Found '.count($concepts)." concepts.\n";
        if (count($concepts) > 0) {
            echo 'First concept ID: '.($concepts[0]['id'] ?? 'N/A')."\n";
            echo 'First concept Title: '.($concepts[0]['name'] ?? 'N/A')."\n";
        }
    }
} catch (\Throwable $e) {
    echo 'ERROR: '.$e->getMessage()."\n";
}

echo "\nTesting Discovery for NEW_GAMES\n";
$category = CategoryEnum::NEW_GAMES;
try {
    $request = RequestProductList::createFromCategory($category);
    $response = $client->get($request);
    $grid = $response['data']['categoryGridRetrieve'] ?? null;
    $concepts = $grid['concepts'] ?? [];
    echo 'Found '.count($concepts)." concepts for NEW.\n";
} catch (\Throwable $e) {
    echo 'ERROR: '.$e->getMessage()."\n";
}
