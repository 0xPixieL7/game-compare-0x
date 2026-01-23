<?php

declare(strict_types=1);

namespace App\Services\Price\GiantBomb;

use Illuminate\Support\Facades\Http;
use Illuminate\Support\Facades\Log;

/**
 * Giant Bomb API Service
 * API Docs: https://www.giantbomb.com/api/documentation
 * 
 * Provides rich video content including:
 * - Gameplay videos
 * - Reviews
 * - Quick Looks
 * - Trailers
 */
final class GiantBombService
{
    private const BASE_URL = 'https://www.giantbomb.com/api';
    
    private string $apiKey;

    public function __construct()
    {
        $this->apiKey = config('services.giantbomb.api_key', '');
        
        if (empty($this->apiKey)) {
            Log::warning('GiantBombService: API key not configured');
        }
    }

    /**
     * Get full game details including videos and metadata.
     * 
     * @param string $gameGuid Giant Bomb game GUID (e.g., "3030-41484")
     * @return array|null
     */
    public function getFullDetails(string $gameGuid): ?array
    {
        if (empty($this->apiKey)) {
            return null;
        }

        try {
            // Get game details
            $gameData = $this->getGame($gameGuid);
            
            if (!$gameData) {
                return null;
            }

            // Get videos related to this game
            $videos = $this->getGameVideos($gameGuid);

            return [
                'media' => [
                    'videos' => $videos,
                    'images' => $this->extractImages($gameData),
                ],
                'metadata' => $this->extractMetadata($gameData),
            ];
        } catch (\Throwable $e) {
            Log::error('GiantBombService: getFullDetails failed', [
                'game_guid' => $gameGuid,
                'error' => $e->getMessage(),
            ]);

            return null;
        }
    }

    /**
     * Get game data by GUID.
     */
    private function getGame(string $gameGuid): ?array
    {
        $url = self::BASE_URL . "/game/{$gameGuid}/";
        
        $response = Http::get($url, [
            'api_key' => $this->apiKey,
            'format' => 'json',
        ]);

        if ($response->failed()) {
            return null;
        }

        $data = $response->json();
        
        if (($data['status_code'] ?? 0) !== 1) {
            return null;
        }

        return $data['results'] ?? null;
    }

    /**
     * Get videos associated with a game.
     */
    private function getGameVideos(string $gameGuid): array
    {
        $url = self::BASE_URL . '/videos/';
        
        $response = Http::get($url, [
            'api_key' => $this->apiKey,
            'format' => 'json',
            'filter' => "game:{$gameGuid}",
            'sort' => 'publish_date:desc',
            'limit' => 50, // Get up to 50 videos
        ]);

        if ($response->failed()) {
            return [];
        }

        $data = $response->json();
        
        if (($data['status_code'] ?? 0) !== 1) {
            return [];
        }

        $videos = [];
        foreach ($data['results'] ?? [] as $video) {
            $videos[] = [
                'id' => $video['id'] ?? null,
                'guid' => $video['guid'] ?? null,
                'name' => $video['name'] ?? null,
                'url' => $video['site_detail_url'] ?? null,
                'api_url' => $video['api_detail_url'] ?? null,
                'duration' => $video['length_seconds'] ?? null,
                'publish_date' => $video['publish_date'] ?? null,
                'thumbnail_url' => $video['image']['medium_url'] ?? $video['image']['small_url'] ?? null,
                'image_urls' => [
                    'icon' => $video['image']['icon_url'] ?? null,
                    'medium' => $video['image']['medium_url'] ?? null,
                    'screen' => $video['image']['screen_url'] ?? null,
                    'small' => $video['image']['small_url'] ?? null,
                    'super' => $video['image']['super_url'] ?? null,
                    'thumb' => $video['image']['thumb_url'] ?? null,
                    'tiny' => $video['image']['tiny_url'] ?? null,
                ],
                'embed_player' => $video['embed_player'] ?? null, // Embeddable player URL
                'video_type' => $video['video_type'] ?? null, // "Quick Look", "Review", "Trailer", etc.
                'deck' => $video['deck'] ?? null, // Short description
                'user' => $video['user'] ?? null,
            ];
        }

        return $videos;
    }

    /**
     * Extract images from game data.
     */
    private function extractImages(array $gameData): array
    {
        $image = $gameData['image'] ?? [];
        
        return [
            'icon' => $image['icon_url'] ?? null,
            'medium' => $image['medium_url'] ?? null,
            'screen' => $image['screen_url'] ?? null,
            'small' => $image['small_url'] ?? null,
            'super' => $image['super_url'] ?? null,
            'thumb' => $image['thumb_url'] ?? null,
            'tiny' => $image['tiny_url'] ?? null,
            'original' => $image['original_url'] ?? null,
        ];
    }

    /**
     * Extract metadata from game data.
     */
    private function extractMetadata(array $gameData): array
    {
        return [
            'name' => $gameData['name'] ?? null,
            'deck' => $gameData['deck'] ?? null, // Short description
            'description' => $gameData['description'] ?? null, // HTML description
            'original_release_date' => $gameData['original_release_date'] ?? null,
            'expected_release_year' => $gameData['expected_release_year'] ?? null,
            'genres' => array_column($gameData['genres'] ?? [], 'name'),
            'developers' => array_column($gameData['developers'] ?? [], 'name'),
            'publishers' => array_column($gameData['publishers'] ?? [], 'name'),
            'platforms' => array_column($gameData['platforms'] ?? [], 'name'),
            'themes' => array_column($gameData['themes'] ?? [], 'name'),
            'franchises' => array_column($gameData['franchises'] ?? [], 'name'),
            'site_detail_url' => $gameData['site_detail_url'] ?? null,
            'api_detail_url' => $gameData['api_detail_url'] ?? null,
        ];
    }

    /**
     * Search for games by name.
     */
    public function search(string $query, int $limit = 10): array
    {
        if (empty($this->apiKey)) {
            return [];
        }

        $url = self::BASE_URL . '/search/';
        
        $response = Http::get($url, [
            'api_key' => $this->apiKey,
            'format' => 'json',
            'query' => $query,
            'resources' => 'game',
            'limit' => $limit,
        ]);

        if ($response->failed()) {
            return [];
        }

        $data = $response->json();
        
        if (($data['status_code'] ?? 0) !== 1) {
            return [];
        }

        $results = [];
        foreach ($data['results'] ?? [] as $game) {
            $results[] = [
                'guid' => $game['guid'] ?? null,
                'id' => $game['id'] ?? null,
                'name' => $game['name'] ?? null,
                'deck' => $game['deck'] ?? null,
                'original_release_date' => $game['original_release_date'] ?? null,
                'image_url' => $game['image']['medium_url'] ?? null,
                'platforms' => array_column($game['platforms'] ?? [], 'name'),
            ];
        }

        return $results;
    }

    /**
     * Get video stream URLs for a specific video.
     * 
     * Note: Giant Bomb videos require API key for streaming.
     */
    public function getVideoStreamUrls(string $videoGuid): ?array
    {
        if (empty($this->apiKey)) {
            return null;
        }

        $url = self::BASE_URL . "/video/{$videoGuid}/";
        
        $response = Http::get($url, [
            'api_key' => $this->apiKey,
            'format' => 'json',
        ]);

        if ($response->failed()) {
            return null;
        }

        $data = $response->json();
        
        if (($data['status_code'] ?? 0) !== 1) {
            return null;
        }

        $video = $data['results'] ?? null;
        
        if (!$video) {
            return null;
        }

        return [
            'low_url' => $video['low_url'] ?? null,
            'high_url' => $video['high_url'] ?? null,
            'hd_url' => $video['hd_url'] ?? null,
            'youtube_id' => $video['youtube_id'] ?? null,
        ];
    }
}
