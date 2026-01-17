<?php

declare(strict_types=1);

namespace App\Services\PriceCharting;

use Illuminate\Support\Facades\Http;
use Illuminate\Support\Facades\Log;
use RuntimeException;

class PriceChartingClient
{
    private const BASE_URL = 'https://www.pricecharting.com/api';

    public function __construct(
        private readonly string $apiToken
    ) {}

    /**
     * Fetch products from Price Charting API.
     * Can optionally filter by console name.
     *
     * @param  string|null  $consoleName  Optional console name to filter by (e.g., 'Nintendo Switch')
     * @return array The list of products
     *
     * @throws RuntimeException If the request fails
     */
    public function getProducts(?string $consoleName = null): array
    {
        $params = [
            't' => $this->apiToken,
        ];

        if ($consoleName) {
            $params['console'] = $consoleName;
        }

        // Using the 'products' endpoint to fetch multiple items
        $response = Http::timeout(60)
            ->get(self::BASE_URL.'/products', $params);

        if (! $response->successful()) {
            Log::error('Price Charting API Error', [
                'status' => $response->status(),
                'body' => $response->body(),
                'console_name' => $consoleName,
            ]);
            throw new RuntimeException('Price Charting API request failed: '.$response->body());
        }

        $data = $response->json();

        // The API might return a wrapper or just the array.
        // Usually it's key 'products' or just the array.
        // If it's a direct array of products, return it.
        // If it has 'products' key, return that.
        if (isset($data['products']) && is_array($data['products'])) {
            return $data['products'];
        }

        if (is_array($data)) {
            return $data;
        }

        throw new RuntimeException('Unexpected Price Charting API response format.');
    }

    /**
     * Get a single product by ID (if needed)
     */
    public function getProduct(string $id): array
    {
        $response = Http::get(self::BASE_URL.'/product', [
            't' => $this->apiToken,
            'id' => $id,
        ]);

        if (! $response->successful()) {
            throw new RuntimeException("Price Charting API request failed for product {$id}");
        }

        return $response->json();
    }
}
