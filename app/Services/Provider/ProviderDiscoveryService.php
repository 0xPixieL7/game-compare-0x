<?php

declare(strict_types=1);

namespace App\Services\Provider;

use App\Models\VideoGame;
use App\Models\VideoGameTitleSource;
use Illuminate\Support\Collection;

/**
 * Provider Discovery Service.
 *
 * Queries VideoGameTitleSource to discover all provider mappings for a game.
 * Used by enrichment jobs to know which external APIs to call.
 */
class ProviderDiscoveryService
{
    /**
     * Provider names that support price lookups.
     */
    public const PRICE_PROVIDERS = [
        'steam',
        'steam_store',
        'playstation_store',
        'xbox',
        'gog',
        'epic',
    ];

    /**
     * Provider names that support media lookups.
     */
    public const MEDIA_PROVIDERS = [
        'igdb',
        'tgdb',
        'steam',
        'rawg',
    ];

    /**
     * Get all provider mappings for a video game.
     *
     * @return Collection<int, VideoGameTitleSource>
     */
    public function getProviderMappingsForGame(VideoGame $game): Collection
    {
        // Ensure relationships are loaded
        $game->loadMissing('title.sources');

        return $game->title?->sources ?? collect();
    }

    /**
     * Get provider mappings filtered to price-capable providers.
     *
     * @return Collection<int, VideoGameTitleSource>
     */
    public function getPriceProviderMappings(VideoGame $game): Collection
    {
        return $this->getProviderMappingsForGame($game)
            ->filter(fn (VideoGameTitleSource $source) => in_array($source->provider, self::PRICE_PROVIDERS, true));
    }

    /**
     * Get provider mappings filtered to media-capable providers.
     *
     * @return Collection<int, VideoGameTitleSource>
     */
    public function getMediaProviderMappings(VideoGame $game): Collection
    {
        return $this->getProviderMappingsForGame($game)
            ->filter(fn (VideoGameTitleSource $source) => in_array($source->provider, self::MEDIA_PROVIDERS, true));
    }

    /**
     * Get the external ID for a specific provider.
     * Returns as integer since external_id is unsignedBigInteger in schema.
     */
    public function getExternalId(VideoGame $game, string $provider): ?int
    {
        $source = $this->getProviderMappingsForGame($game)
            ->firstWhere('provider', $provider);

        return $source?->external_id !== null ? (int) $source->external_id : null;
    }

    /**
     * Get the provider item ID (e.g., PSN product ID) for a specific provider.
     * Returns as integer since provider_item_id is unsignedBigInteger in schema.
     */
    public function getProviderItemId(VideoGame $game, string $provider): ?int
    {
        $source = $this->getProviderMappingsForGame($game)
            ->firstWhere('provider', $provider);

        return $source?->provider_item_id !== null ? (int) $source->provider_item_id : null;
    }

    /**
     * Check if a game has a mapping for a specific provider.
     */
    public function hasProvider(VideoGame $game, string $provider): bool
    {
        return $this->getProviderMappingsForGame($game)
            ->contains('provider', $provider);
    }

    /**
     * Get the preferred media provider source for a game.
     * Prioritizes IGDB > TGDB > Steam.
     */
    public function getPreferredMediaSource(VideoGame $game): ?VideoGameTitleSource
    {
        $mappings = $this->getMediaProviderMappings($game);

        // Priority order
        foreach (['igdb', 'tgdb', 'steam'] as $provider) {
            $source = $mappings->firstWhere('provider', $provider);
            if ($source) {
                return $source;
            }
        }

        return null;
    }

    /**
     * Get all provider names for a game.
     *
     * @return array<int, string>
     */
    public function getProviderNames(VideoGame $game): array
    {
        return $this->getProviderMappingsForGame($game)
            ->pluck('provider')
            ->unique()
            ->values()
            ->all();
    }
}
