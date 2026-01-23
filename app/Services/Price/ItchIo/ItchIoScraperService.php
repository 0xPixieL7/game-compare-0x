<?php

declare(strict_types=1);

namespace App\Services\Price\ItchIo;

use Illuminate\Support\Facades\Http;
use Illuminate\Support\Facades\Log;

/**
 * itch.io Price Service
 * 
 * itch.io is a popular indie game marketplace.
 * Note: itch.io doesn't have an official public API, so this uses web scraping.
 * For production, consider contacting itch.io for API access.
 */
final class ItchIoScraperService
{
    /**
     * Get full details including price and media from itch.io game page.
     * 
     * @param string $gameSlug The itch.io game slug (e.g., "celeste" from https://mattmakesgames.itch.io/celeste)
     * @param string|null $username Optional username if known
     * @return array|null
     */
    public function getFullDetails(string $gameSlug, ?string $username = null): ?array
    {
        try {
            // Try to construct URL
            if ($username) {
                $url = "https://{$username}.itch.io/{$gameSlug}";
            } else {
                // Search for the game first
                $searchResults = $this->search($gameSlug);
                if (empty($searchResults)) {
                    return null;
                }
                $url = $searchResults[0]['url'] ?? null;
                if (!$url) {
                    return null;
                }
            }

            $response = Http::timeout(15)
                ->withHeaders([
                    'User-Agent' => 'Mozilla/5.0 (compatible; game-compare/1.0)',
                    'Accept' => 'text/html,application/xhtml+xml',
                ])
                ->get($url);

            if ($response->failed()) {
                return null;
            }

            $html = $response->body();

            return [
                'price' => $this->extractPrice($html),
                'media' => $this->extractMedia($html, $url),
                'metadata' => $this->extractMetadata($html),
            ];
        } catch (\Throwable $e) {
            Log::debug('ItchIoScraperService: Failed to fetch game details', [
                'game_slug' => $gameSlug,
                'error' => $e->getMessage(),
            ]);

            return null;
        }
    }

    /**
     * Search for games on itch.io.
     */
    public function search(string $query, int $limit = 5): array
    {
        try {
            $response = Http::timeout(10)
                ->withHeaders(['User-Agent' => 'Mozilla/5.0 (compatible; game-compare/1.0)'])
                ->get('https://itch.io/search', [
                    'q' => $query,
                ]);

            if ($response->failed()) {
                return [];
            }

            $html = $response->body();
            $results = [];

            // Parse search results (simplified - would need proper HTML parsing in production)
            preg_match_all('/<a[^>]*class="[^"]*game_link[^"]*"[^>]*href="([^"]+)"[^>]*>([^<]+)<\/a>/i', $html, $matches, PREG_SET_ORDER);

            foreach (array_slice($matches, 0, $limit) as $match) {
                $results[] = [
                    'url' => $match[1] ?? null,
                    'title' => trim($match[2] ?? ''),
                ];
            }

            return $results;
        } catch (\Throwable $e) {
            Log::debug('ItchIoScraperService: Search failed', [
                'query' => $query,
                'error' => $e->getMessage(),
            ]);

            return [];
        }
    }

    /**
     * Extract price from HTML.
     */
    private function extractPrice(string $html): ?array
    {
        // Look for price in various formats
        // Free games
        if (preg_match('/No payments? required|Free/i', $html)) {
            return [
                'currency' => 'USD',
                'amount_minor' => 0,
                'display_price' => 'Free',
                'is_free' => true,
            ];
        }

        // Pay what you want
        if (preg_match('/Name your own price|Pay what you want/i', $html)) {
            return [
                'currency' => 'USD',
                'amount_minor' => 0,
                'display_price' => 'Pay What You Want',
                'is_pwyw' => true,
            ];
        }

        // Fixed price - look for price pattern
        if (preg_match('/\$(\d+(?:\.\d{2})?)/i', $html, $matches)) {
            $price = (float) $matches[1];
            return [
                'currency' => 'USD',
                'amount_minor' => (int) ($price * 100),
                'display_price' => "\${$matches[1]}",
                'is_free' => false,
            ];
        }

        return null;
    }

    /**
     * Extract media from HTML.
     */
    private function extractMedia(string $html, string $pageUrl): array
    {
        $images = [];
        $videos = [];

        // Extract cover image
        if (preg_match('/<div[^>]*class="[^"]*game_thumb[^"]*"[^>]*>.*?<img[^>]*src="([^"]+)"[^>]*>/is', $html, $match)) {
            $images[] = [
                'url' => $match[1],
                'type' => 'cover',
            ];
        }

        // Extract screenshot images
        preg_match_all('/<div[^>]*class="[^"]*screenshot[^"]*"[^>]*>.*?<a[^>]*href="([^"]+)"[^>]*>/is', $html, $matches);
        foreach ($matches[1] ?? [] as $url) {
            $images[] = [
                'url' => $url,
                'type' => 'screenshot',
            ];
        }

        // Extract YouTube embeds
        if (preg_match_all('/youtube\.com\/embed\/([a-zA-Z0-9_-]+)/i', $html, $matches)) {
            foreach ($matches[1] as $videoId) {
                $videos[] = [
                    'url' => "https://www.youtube.com/watch?v={$videoId}",
                    'thumbnail' => "https://img.youtube.com/vi/{$videoId}/maxresdefault.jpg",
                    'type' => 'trailer',
                ];
            }
        }

        return [
            'images' => array_filter($images, fn($img) => !empty($img['url'])),
            'videos' => $videos,
            'cover_image' => $images[0]['url'] ?? null,
        ];
    }

    /**
     * Extract metadata from HTML.
     */
    private function extractMetadata(string $html): array
    {
        $metadata = [];

        // Title
        if (preg_match('/<h1[^>]*class="[^"]*game_title[^"]*"[^>]*>([^<]+)<\/h1>/i', $html, $match)) {
            $metadata['title'] = trim($match[1]);
        }

        // Author/Developer
        if (preg_match('/<div[^>]*class="[^"]*game_author[^"]*"[^>]*>.*?by\s*<a[^>]*>([^<]+)<\/a>/is', $html, $match)) {
            $metadata['developer'] = trim($match[1]);
        }

        // Description
        if (preg_match('/<div[^>]*class="[^"]*formatted_description[^"]*"[^>]*>(.*?)<\/div>/is', $html, $match)) {
            $metadata['description'] = trim(strip_tags($match[1]));
        }

        // Tags
        preg_match_all('/<a[^>]*class="[^"]*game_tag[^"]*"[^>]*>([^<]+)<\/a>/i', $html, $matches);
        $metadata['tags'] = array_map('trim', $matches[1] ?? []);

        // Platform
        if (preg_match('/Windows|Linux|macOS|Android|Web/i', $html, $match)) {
            $metadata['platforms'] = [$match[0]];
        }

        return $metadata;
    }

    /**
     * Get price only (legacy compatibility).
     */
    public function getPrice(string $gameSlug, ?string $username = null): ?array
    {
        $fullDetails = $this->getFullDetails($gameSlug, $username);
        return $fullDetails['price'] ?? null;
    }
}
