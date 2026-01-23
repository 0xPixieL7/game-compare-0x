<?php

declare(strict_types=1);

namespace App\Services\Providers\Rawg;

use App\Jobs\Enrichment\Traits\ExtractsStoreUrls;

final class RawgCommerceLinkResolver
{
    use ExtractsStoreUrls;

    /**
     * @param  array<int, array<string, mixed>>  $stores
     * @return array<int, array{provider:string, provider_item_id:int, raw_id:string, url:string}>
     */
    public function resolve(array $stores): array
    {
        $out = [];

        foreach ($stores as $entry) {
            if (! is_array($entry)) {
                continue;
            }

            $url = $entry['url'] ?? null;
            if (! is_string($url) || $url === '') {
                continue;
            }

            $store = $entry['store'] ?? [];
            $storeSlug = is_array($store) ? ($store['slug'] ?? null) : null;
            $storeName = is_array($store) ? ($store['name'] ?? null) : null;

            $provider = $this->normalizeStoreProvider((string) ($storeSlug ?: $storeName ?: ''));
            if (! $provider) {
                continue;
            }

            // Always prefer normalized provider key for extraction.
            $rawId = $this->extractStoreAppId($url, $provider);
            if (! is_string($rawId) || $rawId === '') {
                continue;
            }

            $providerItemId = $this->toNumericProviderItemId($rawId);

            $out[] = [
                'provider' => $provider,
                'provider_item_id' => $providerItemId,
                'raw_id' => $rawId,
                'url' => $url,
            ];
        }

        // de-dupe by provider+provider_item_id
        $seen = [];
        $deduped = [];
        foreach ($out as $row) {
            $k = $row['provider'].':'.$row['provider_item_id'];
            if (isset($seen[$k])) {
                continue;
            }
            $seen[$k] = true;
            $deduped[] = $row;
        }

        return $deduped;
    }

    private function toNumericProviderItemId(string $raw): int
    {
        if (ctype_digit($raw)) {
            return (int) $raw;
        }

        // bigint-safe deterministic mapping (32-bit) for non-numeric store IDs.
        return abs(crc32($raw));
    }
}
