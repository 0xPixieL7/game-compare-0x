<?php

declare(strict_types=1);

namespace App\Services\Tgdb;

use Illuminate\Support\Facades\Http;
use Illuminate\Support\Facades\Log;
use RuntimeException;

class TgdbClient
{
    private string $baseUrl;

    private ?string $publicKey;

    private ?string $privateKey;

    public function __construct()
    {
        $this->baseUrl = config('services.tgdb.base_url', 'https://api.thegamesdb.net/v1.1');
        $this->publicKey = config('services.tgdb.public_key');
        $this->privateKey = config('services.tgdb.private_key');
    }

    /**
     * Fetch all platforms.
     */
    public function getPlatforms(): array
    {
        return $this->request('Platforms', [], true); // Use private key for potentially larger limits if available
    }

    /**
     * Fetch games by platform ID (paginated).
     */
    public function getGamesByPlatform(int $platformId, int $page = 1): array
    {
        return $this->request('Games/ByPlatformID', [
            'id' => $platformId,
            'page' => $page,
            // 'include' => 'boxart,platform', // TGDB might support includes, but we'll fetch images separately for batching
        ], true);
    }

    /**
     * Fetch games by name (search).
     */
    public function getGamesByName(string $name): array
    {
        return $this->request('Games/ByGameName', [
            'name' => $name,
            // 'fields' => 'id,name,platform', // Optional optimization
        ], true);
    }

    /**
     * Fetch images for a batch of game IDs.
     *
     * @param  array<int|string>  $gameIds
     */
    public function getImages(array $gameIds): array
    {
        if (empty($gameIds)) {
            return [];
        }

        // TGDB allows comma-separated IDs
        $ids = implode(',', $gameIds);

        return $this->request('Games/Images', [
            'games_id' => $ids,
        ], true);
    }

    /**
     * Make a request to the TGDB API.
     *
     * @param  bool  $preferPrivate  Use private key if available (for server-side jobs)
     */
    private function request(string $endpoint, array $params = [], bool $preferPrivate = false): array
    {
        $url = "{$this->baseUrl}/{$endpoint}";

        // Determine which key to use
        $apiKey = ($preferPrivate && $this->privateKey) ? $this->privateKey : $this->publicKey;

        if (! $apiKey) {
            throw new RuntimeException('TGDB API Key not configured. Please set TGDB_PUBLIC_KEY or TGDB_PRIVATE_KEY.');
        }

        $params['apikey'] = $apiKey;

        $response = Http::timeout(30)->get($url, $params);

        if (! $response->successful()) {
            // Handle 404s gracefully (empty result) or throw
            if ($response->status() === 404) {
                return [];
            }

            Log::error('TGDB API Error', [
                'endpoint' => $endpoint,
                'status' => $response->status(),
                'body' => $response->body(),
            ]);
            throw new RuntimeException('TGDB API request failed: '.$response->body());
        }

        // DEBUG: Log response
        // Log::info("TGDB Response for $endpoint", $response->json());

        return $response->json();
    }
}
