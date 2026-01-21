<?php

require 'vendor/autoload.php';

$conceptId = "10017780";
$region = "ja-jp";

$guzzle = new \GuzzleHttp\Client(['base_uri' => 'https://web.np.playstation.com/api/graphql/v1/']);
$locator = \PlaystationStoreApi\RequestLocatorService::default();
$info = $locator->get(\PlaystationStoreApi\Request\RequestPricingDataByConceptId::class);
$variables = json_encode(['conceptId' => $conceptId]);
$extensions = json_encode(['persistedQuery' => ['version' => 1, 'sha256Hash' => $info->value]]);

$query = http_build_query([
    'operationName' => $info->name,
    'variables' => $variables,
    'extensions' => $extensions,
]);

echo "Testing Region: $region, Concept: $conceptId\n";

try {
    $response = $guzzle->get('op?' . $query, [
        'headers' => [
            'x-psn-store-locale-override' => $region,
            'content-type' => 'application/json',
        ],
    ]);
    
    $data = json_decode((string)$response->getBody(), true);
    if (!isset($data['data']['conceptRetrieve'])) {
         echo "NOT FOUND IN JAPAN\n";
    } else {
         echo "FOUND IN JAPAN\n";
         echo "DEFAULT PRODUCT: " . (isset($data['data']['conceptRetrieve']['defaultProduct']) ? "YES" : "NO") . "\n";
    }
} catch (\Throwable $e) {
    echo "ERROR: " . $e->getMessage() . "\n";
}

// Try US for comparison
echo "\nTesting Region: en-us, Concept: $conceptId\n";
try {
    $response = $guzzle->get('op?' . $query, [
        'headers' => [
            'x-psn-store-locale-override' => 'en-us',
            'content-type' => 'application/json',
        ],
    ]);
    
    $data = json_decode((string)$response->getBody(), true);
    if (!isset($data['data']['conceptRetrieve'])) {
         echo "NOT FOUND IN US\n";
    } else {
         echo "FOUND IN US\n";
         echo "DEFAULT PRODUCT: " . (isset($data['data']['conceptRetrieve']['defaultProduct']) ? "YES" : "NO") . "\n";
    }
} catch (\Throwable $e) {
    echo "ERROR: " . $e->getMessage() . "\n";
}
