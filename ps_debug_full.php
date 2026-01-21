<?php

require 'vendor/autoload.php';

$conceptId = "234008";
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

$response = $guzzle->get('op?' . $query, [
    'headers' => [
        'x-psn-store-locale-override' => $region,
        'content-type' => 'application/json',
    ],
]);

$data = json_decode((string)$response->getBody(), true);
echo json_encode($data, JSON_PRETTY_PRINT) . "\n";
