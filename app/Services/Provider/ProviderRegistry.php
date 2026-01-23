<?php

declare(strict_types=1);

namespace App\Services\Provider;

use Illuminate\Support\Str;

final class ProviderRegistry
{
    /**
     * @return array{
     *   provider: string,
     *   provider_key: string,
     *   display_name: string,
     *   category: string,
     *   slug: string,
     *   base_url: string|null,
     *   metadata: array<string, mixed>
     * }
     */
    public static function meta(string $provider): array
    {
        $provider = strtolower(trim($provider));

        $known = [
            'igdb' => [
                'display_name' => 'IGDB',
                'category' => 'catalogue',
                'base_url' => 'https://api.igdb.com',
            ],
            'rawg' => [
                'display_name' => 'RAWG',
                'category' => 'catalogue',
                'base_url' => config('services.rawg.base_url', 'https://api.rawg.io/api'),
            ],
            'steam' => [
                'display_name' => 'Steam',
                'category' => 'store',
                'base_url' => 'https://store.steampowered.com',
            ],
            'steam_store' => [
                'display_name' => 'Steam Store',
                'category' => 'store',
                'base_url' => 'https://store.steampowered.com',
            ],
            'playstation_store' => [
                'display_name' => 'PlayStation Store',
                'category' => 'store',
                'base_url' => 'https://store.playstation.com',
            ],
            'psstore' => [
                'display_name' => 'PlayStation Store',
                'category' => 'store',
                'base_url' => 'https://store.playstation.com',
            ],
            'tgdb' => [
                'display_name' => 'TheGamesDB',
                'category' => 'catalogue',
                'base_url' => config('services.tgdb.base_url', 'https://api.thegamesdb.net/v1'),
            ],
            'thegamesdb_mirror' => [
                'display_name' => 'TheGamesDB Mirror',
                'category' => 'catalogue',
                'base_url' => null,
            ],
            'xbox' => [
                'display_name' => 'Xbox Store',
                'category' => 'store',
                'base_url' => 'https://www.xbox.com',
            ],
            'gog' => [
                'display_name' => 'GOG',
                'category' => 'store',
                'base_url' => 'https://www.gog.com',
            ],
            'epic_games' => [
                'display_name' => 'Epic Games Store',
                'category' => 'store',
                'base_url' => 'https://store.epicgames.com',
            ],
            'itch_io' => [
                'display_name' => 'itch.io',
                'category' => 'store',
                'base_url' => 'https://itch.io',
            ],
            'nintendo_eshop' => [
                'display_name' => 'Nintendo eShop',
                'category' => 'store',
                'base_url' => 'https://www.nintendo.com',
            ],
            'giantbomb' => [
                'display_name' => 'GiantBomb',
                'category' => 'catalogue',
                'base_url' => 'https://www.giantbomb.com',
            ],
            'nexarda' => [
                'display_name' => 'Nexarda',
                'category' => 'catalogue',
                'base_url' => config('services.nexarda.base_url'),
            ],
        ];

        $entry = $known[$provider] ?? [
            'display_name' => strtoupper($provider),
            'category' => 'unknown',
            'base_url' => null,
        ];

        return [
            'provider' => $provider,
            'provider_key' => $provider,
            'display_name' => (string) $entry['display_name'],
            'category' => (string) $entry['category'],
            'slug' => Str::slug((string) $entry['display_name']),
            'base_url' => $entry['base_url'],
            'metadata' => [
                'registry' => 'ProviderRegistry',
            ],
        ];
    }
}
