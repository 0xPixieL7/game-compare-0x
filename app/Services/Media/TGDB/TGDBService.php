<?php

declare(strict_types=1);

namespace App\Services\Media\TGDB;

use Illuminate\Support\Facades\Http;
use Illuminate\Support\Facades\Log;

/**
 * TheGamesDB (TGDB) API Service
 * API Docs: https://thegamesdb.net/
 * 
 * Provides:
 * - Game metadata
 * - Box art (front/back)
 * - Screenshots
 * - Fanart
 * - Banners
 * - Clearlogos
 */
final class TGDBService
{
    private const BASE_URL = 'https://api.thegamesdb.net/v1';
    
    private string $apiKey;

    public function __construct()
    {
        $this->apiKey = config('services.tgdb.api_key', '');
        
        if (empty($this->apiKey)) {
            Log::warning('TGDBService: API key not configured');
        }
    }

    /**
     * Get full game details including images and metadata.
     */
    public function getFullDetails(int $gameId): ?array
    {
        if (empty($this->apiKey)) {
            return null;
        }

        try {
            // Get game details with images
            $gameData = $this->getGameById($gameId);
            
            if (!$gameData) {
                return null;
            }

            // Get additional images
            $images = $this->getGameImages($gameId);

            return [
                'media' => [
                    'boxart' => $images['boxart'] ?? [],
                    'screenshots' => $images['screenshots'] ?? [],
                    'fanart' => $images['fanart'] ?? [],
                    'banners' => $images['banners'] ?? [],
                    'clearlogo' => $images['clearlogo'] ?? [],
                ],
                'metadata' => $this->extractMetadata($gameData),
            ];
        } catch (\Throwable $e) {
            Log::error('TGDBService: getFullDetails failed', [
                'game_id' => $gameId,
                'error' => $e->getMessage(),
            ]);

            return null;
        }
    }

    /**
     * Get game by ID.
     */
    private function getGameById(int $gameId): ?array
    {
        $url = self::BASE_URL . '/Games/ByGameID';
        
        $response = Http::get($url, [
            'apikey' => $this->apiKey,
            'id' => $gameId,
            'include' => 'boxart,platform',
        ]);

        if ($response->failed()) {
            return null;
        }

        $data = $response->json();
        
        if (isset($data['data']['games'][0])) {
            return $data['data']['games'][0];
        }

        return null;
    }

    /**
     * Get all images for a game.
     */
    private function getGameImages(int $gameId): array
    {
        $url = self::BASE_URL . '/Games/Images';
        
        $response = Http::get($url, [
            'apikey' => $this->apiKey,
            'games_id' => $gameId,
        ]);

        if ($response->failed()) {
            return [];
        }

        $data = $response->json();
        $baseUrl = $data['data']['base_url']['original'] ?? '';
        $images = $data['data']['images'][$gameId] ?? [];

        return [
            'boxart' => $this->formatImages($images['boxart'] ?? [], $baseUrl, 'boxart'),
            'screenshots' => $this->formatImages($images['screenshot'] ?? [], $baseUrl, 'screenshots'),
            'fanart' => $this->formatImages($images['fanart'] ?? [], $baseUrl, 'fanart'),
            'banners' => $this->formatImages($images['banner'] ?? [], $baseUrl, 'graphical'),
            'clearlogo' => $this->formatImages($images['clearlogo'] ?? [], $baseUrl, 'clearlogo'),
        ];
    }

    /**
     * Format image URLs with base URL.
     */
    private function formatImages(array $images, string $baseUrl, string $type): array
    {
        $formatted = [];

        foreach ($images as $image) {
            $formatted[] = [
                'id' => $image['id'] ?? null,
                'type' => $image['type'] ?? $type,
                'side' => $image['side'] ?? null, // For boxart: "front", "back"
                'filename' => $image['filename'] ?? null,
                'resolution' => $image['resolution'] ?? null,
                'url' => $baseUrl . $type . '/' . ($image['filename'] ?? ''),
            ];
        }

        return $formatted;
    }

    /**
     * Extract metadata from game data.
     */
    private function extractMetadata(array $gameData): array
    {
        return [
            'id' => $gameData['id'] ?? null,
            'game_title' => $gameData['game_title'] ?? null,
            'release_date' => $gameData['release_date'] ?? null,
            'platform' => $gameData['platform'] ?? null,
            'players' => $gameData['players'] ?? null,
            'overview' => $gameData['overview'] ?? null,
            'last_updated' => $gameData['last_updated'] ?? null,
            'rating' => $gameData['rating'] ?? null,
            'coop' => $gameData['coop'] ?? null,
            'youtube' => $gameData['youtube'] ?? null,
            'os' => $gameData['os'] ?? null,
            'processor' => $gameData['processor'] ?? null,
            'ram' => $gameData['ram'] ?? null,
            'hdd' => $gameData['hdd'] ?? null,
            'video' => $gameData['video'] ?? null,
            'sound' => $gameData['sound'] ?? null,
            'developers' => $gameData['developers'] ?? [],
            'publishers' => $gameData['publishers'] ?? [],
            'genres' => $gameData['genres'] ?? [],
        ];
    }

    /**
     * Search for games by name.
     */
    public function search(string $name, ?int $platformId = null): array
    {
        if (empty($this->apiKey)) {
            return [];
        }

        $url = self::BASE_URL . '/Games/ByGameName';
        
        $params = [
            'apikey' => $this->apiKey,
            'name' => $name,
        ];

        if ($platformId) {
            $params['filter[platform]'] = $platformId;
        }

        $response = Http::get($url, $params);

        if ($response->failed()) {
            return [];
        }

        $data = $response->json();
        $results = [];

        foreach ($data['data']['games'] ?? [] as $game) {
            $results[] = [
                'id' => $game['id'] ?? null,
                'game_title' => $game['game_title'] ?? null,
                'release_date' => $game['release_date'] ?? null,
                'platform' => $game['platform'] ?? null,
                'overview' => $game['overview'] ?? null,
            ];
        }

        return $results;
    }

    /**
     * Get list of platforms.
     */
    public function getPlatforms(): array
    {
        if (empty($this->apiKey)) {
            return [];
        }

        $url = self::BASE_URL . '/Platforms';
        
        $response = Http::get($url, [
            'apikey' => $this->apiKey,
        ]);

        if ($response->failed()) {
            return [];
        }

        $data = $response->json();
        $platforms = [];

        foreach ($data['data']['platforms'] ?? [] as $platform) {
            $platforms[] = [
                'id' => $platform['id'] ?? null,
                'name' => $platform['name'] ?? null,
                'alias' => $platform['alias'] ?? null,
                'icon' => $platform['icon'] ?? null,
                'console' => $platform['console'] ?? null,
                'controller' => $platform['controller'] ?? null,
                'developer' => $platform['developer'] ?? null,
                'manufacturer' => $platform['manufacturer'] ?? null,
            ];
        }

        return $platforms;
    }

    /**
     * Get YouTube URL if available.
     */
    public function getYouTubeUrl(array $metadata): ?string
    {
        $youtubeId = $metadata['youtube'] ?? null;
        
        if ($youtubeId) {
            return "https://www.youtube.com/watch?v={$youtubeId}";
        }

        return null;
    }
}
