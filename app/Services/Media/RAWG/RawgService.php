<?php

declare(strict_types=1);

namespace App\Services\Media\RAWG;

use Illuminate\Support\Facades\Http;
use Illuminate\Support\Facades\Log;

/**
 * RAWG Video Game Database API Service
 * API Docs: https://api.rawg.io/docs/
 * 
 * Provides:
 * - Game metadata
 * - Screenshots
 * - Trailers (YouTube)
 * - Gameplay clips
 * - Store links
 * - Reviews and ratings
 */
final class RawgService
{
    private const BASE_URL = 'https://api.rawg.io/api';
    
    private string $apiKey;

    public function __construct()
    {
        $this->apiKey = config('services.rawg.api_key', '');
        
        if (empty($this->apiKey)) {
            Log::warning('RawgService: API key not configured');
        }
    }

    /**
     * Get full game details including media and metadata.
     * 
     * OPTIMIZED: Single API call - RAWG returns screenshots, trailers, stores, etc. in base response
     */
    public function getFullDetails(string $gameSlug): ?array
    {
        if (empty($this->apiKey)) {
            return null;
        }

        try {
            // Single API call gets everything
            $gameData = $this->getGame($gameSlug);
            
            if (!$gameData) {
                return null;
            }

            // Extract screenshots from response
            $screenshots = [];
            foreach ($gameData['short_screenshots'] ?? [] as $screenshot) {
                $screenshots[] = [
                    'id' => $screenshot['id'] ?? null,
                    'image' => $screenshot['image'] ?? null,
                ];
            }

            return [
                'media' => [
                    'hero_image' => $gameData['background_image'] ?? null, // Primary hero/cover
                    'background_additional' => $gameData['background_image_additional'] ?? null, // Secondary background/character art
                    'screenshots' => $screenshots, // Minimal - just for reference
                    'screenshot_count' => count($screenshots),
                    'clip' => $gameData['clip'] ?? null, // Video clip if available
                ],
                'metadata' => $this->extractMetadata($gameData),
            ];
        } catch (\Throwable $e) {
            Log::error('RawgService: getFullDetails failed', [
                'game_slug' => $gameSlug,
                'error' => $e->getMessage(),
            ]);

            return null;
        }
    }

    /**
     * Get game by slug.
     */
    private function getGame(string $slug): ?array
    {
        $url = self::BASE_URL . "/games/{$slug}";
        
        $response = Http::get($url, [
            'key' => $this->apiKey,
        ]);

        if ($response->failed()) {
            return null;
        }

        return $response->json();
    }

    /**
     * Get all games with filtering and pagination.
     * 
     * @param array $filters Optional filters
     * @param int $page Page number (default: 1)
     * @param int $pageSize Results per page (default: 20, max: 40)
     */
    public function getAllGames(array $filters = [], int $page = 1, int $pageSize = 20): array
    {
        if (empty($this->apiKey)) {
            return [];
        }

        $url = self::BASE_URL . '/games';
        
        $params = [
            'key' => $this->apiKey,
            'page' => $page,
            'page_size' => min($pageSize, 40), // RAWG max is 40
        ];

        // Apply filters
        if (isset($filters['dates'])) {
            $params['dates'] = $filters['dates']; // e.g., "2024-01-01,2024-12-31"
        }
        if (isset($filters['platforms'])) {
            $params['platforms'] = $filters['platforms']; // e.g., "4,5,18" (PC, macOS, PlayStation)
        }
        if (isset($filters['stores'])) {
            $params['stores'] = $filters['stores']; // e.g., "1,3" (Steam, PlayStation Store)
        }
        if (isset($filters['developers'])) {
            $params['developers'] = $filters['developers'];
        }
        if (isset($filters['publishers'])) {
            $params['publishers'] = $filters['publishers'];
        }
        if (isset($filters['genres'])) {
            $params['genres'] = $filters['genres']; // e.g., "4" (Action)
        }
        if (isset($filters['tags'])) {
            $params['tags'] = $filters['tags'];
        }
        if (isset($filters['creators'])) {
            $params['creators'] = $filters['creators'];
        }
        if (isset($filters['ordering'])) {
            $params['ordering'] = $filters['ordering']; // e.g., "-released", "-rating", "-added"
        }

        $response = Http::get($url, $params);

        if ($response->failed()) {
            return [];
        }

        $data = $response->json();

        return [
            'count' => $data['count'] ?? 0,
            'next' => $data['next'] ?? null,
            'previous' => $data['previous'] ?? null,
            'results' => $data['results'] ?? [],
        ];
    }

    /**
     * Get YouTube trailers for a game (separate endpoint).
     */
    public function getYouTubeTrailers(string $slug): array
    {
        $url = self::BASE_URL . "/games/{$slug}/youtube";
        
        $response = Http::get($url, [
            'key' => $this->apiKey,
        ]);

        if ($response->failed()) {
            return [];
        }

        $data = $response->json();
        $videos = [];

        foreach ($data['results'] ?? [] as $video) {
            $videos[] = [
                'id' => $video['id'] ?? null,
                'external_id' => $video['external_id'] ?? null, // YouTube video ID
                'channel_id' => $video['channel_id'] ?? null,
                'channel_title' => $video['channel_title'] ?? null,
                'name' => $video['name'] ?? null,
                'description' => $video['description'] ?? null,
                'created' => $video['created'] ?? null,
                'view_count' => $video['view_count'] ?? null,
                'comments_count' => $video['comments_count'] ?? null,
                'thumbnails' => $video['thumbnails'] ?? [],
                'url' => $video['external_id'] ? "https://www.youtube.com/watch?v={$video['external_id']}" : null,
            ];
        }

        return $videos;
    }

    /**
     * Extract metadata from game data.
     */
    private function extractMetadata(array $gameData): array
    {
        return [
            'id' => $gameData['id'] ?? null,
            'slug' => $gameData['slug'] ?? null,
            'name' => $gameData['name'] ?? null,
            'description' => $gameData['description'] ?? null,
            'description_raw' => $gameData['description_raw'] ?? null,
            'released' => $gameData['released'] ?? null,
            'tba' => $gameData['tba'] ?? false,
            'updated' => $gameData['updated'] ?? null,
            'rating' => $gameData['rating'] ?? null,
            'rating_top' => $gameData['rating_top'] ?? null,
            'ratings_count' => $gameData['ratings_count'] ?? null,
            'reviews_text_count' => $gameData['reviews_text_count'] ?? null,
            'metacritic' => $gameData['metacritic'] ?? null,
            'metacritic_url' => $gameData['metacritic_url'] ?? null,
            'playtime' => $gameData['playtime'] ?? null,
            'platforms' => array_column($gameData['platforms'] ?? [], 'platform'),
            'developers' => array_map(fn($d) => $d['name'] ?? null, $gameData['developers'] ?? []),
            'publishers' => array_map(fn($p) => $p['name'] ?? null, $gameData['publishers'] ?? []),
            'genres' => array_map(fn($g) => $g['name'] ?? null, $gameData['genres'] ?? []),
            'tags' => array_map(fn($t) => $t['name'] ?? null, $gameData['tags'] ?? []),
            'esrb_rating' => $gameData['esrb_rating']['name'] ?? null,
            'website' => $gameData['website'] ?? null,
            'reddit_url' => $gameData['reddit_url'] ?? null,
        ];
    }

    /**
     * Search for games.
     */
    public function search(string $query, int $pageSize = 10): array
    {
        if (empty($this->apiKey)) {
            return [];
        }

        $url = self::BASE_URL . '/games';
        
        $response = Http::get($url, [
            'key' => $this->apiKey,
            'search' => $query,
            'page_size' => $pageSize,
        ]);

        if ($response->failed()) {
            return [];
        }

        $data = $response->json();
        $results = [];

        foreach ($data['results'] ?? [] as $game) {
            $results[] = [
                'id' => $game['id'] ?? null,
                'slug' => $game['slug'] ?? null,
                'name' => $game['name'] ?? null,
                'released' => $game['released'] ?? null,
                'background_image' => $game['background_image'] ?? null,
                'rating' => $game['rating'] ?? null,
                'metacritic' => $game['metacritic'] ?? null,
                'platforms' => array_column($game['platforms'] ?? [], 'platform'),
            ];
        }

        return $results;
    }

    /**
     * Get stores where game is available.
     */
    public function getStores(string $slug): array
    {
        $url = self::BASE_URL . "/games/{$slug}/stores";
        
        $response = Http::get($url, [
            'key' => $this->apiKey,
        ]);

        if ($response->failed()) {
            return [];
        }

        $data = $response->json();
        $stores = [];

        foreach ($data['results'] ?? [] as $storeData) {
            $store = $storeData['store'] ?? [];
            $stores[] = [
                'id' => $storeData['id'] ?? null,
                'store_id' => $store['id'] ?? null,
                'store_name' => $store['name'] ?? null,
                'store_slug' => $store['slug'] ?? null,
                'url' => $storeData['url'] ?? null,
            ];
        }

        return $stores;
    }
}
