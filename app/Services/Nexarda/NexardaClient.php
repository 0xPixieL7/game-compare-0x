<?php

declare(strict_types=1);

namespace App\Services\Nexarda;

use Illuminate\Support\Facades\Http;
use Illuminate\Support\Facades\Log;
use RuntimeException;

class NexardaClient
{
    private string $baseUrl;

    private ?string $apiKey;

    private int $timeout;

    public function __construct()
    {
        $this->baseUrl = config('services.nexarda.base_url', 'https://www.nexarda.com/api/v3');
        $this->apiKey = config('services.nexarda.api_key');
        $this->timeout = (int) config('services.nexarda.timeout', 30);
    }

    /**
     * Fetch prices for a specific game.
     */
    public function getPrices(string|int $id, string $currency = 'USD', string $type = 'game'): array
    {
        return $this->request('prices', [
            'type' => $type,
            'id' => $id,
            'currency' => $currency,
        ]);
    }

    /**
     * Search for games by name.
     */
    public function search(string $query): array
    {
        return $this->request('search', [
            'q' => $query,
        ]);
    }

    /**
     * Make a request to the Nexarda API.
     */
    private function request(string $endpoint, array $params = []): array
    {
        $url = "{$this->baseUrl}/{$endpoint}";

        if ($this->apiKey) {
            $params['key'] = $this->apiKey;
        }

        $response = Http::timeout($this->timeout)
            ->withHeaders([
                'Accept' => 'application/json',
            ])
            ->get($url, $params);

        if (! $response->successful()) {
            Log::error('Nexarda API Error', [
                'endpoint' => $endpoint,
                'status' => $response->status(),
                'body' => $response->body(),
            ]);

            if ($response->status() === 404) {
                return [];
            }

            throw new RuntimeException('Nexarda API request failed: '.$response->body());
        }

        $data = $response->json();

        if (! ($data['success'] ?? false)) {
            Log::warning('Nexarda API returned unsuccessful response', [
                'endpoint' => $endpoint,
                'data' => $data,
            ]);

            return [];
        }

        return $data;
    }
}
