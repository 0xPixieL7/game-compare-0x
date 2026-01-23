<?php

declare(strict_types=1);

namespace App\Services\Providers;

use App\Jobs\Enrichment\FetchRawgDataJob;
use App\Models\Product;
use App\Models\VideoGame;
use App\Models\VideoGameSource;
use App\Models\VideoGameTitle;
use App\Models\VideoGameTitleSource;
use App\Services\Provider\ProviderRegistry;
use App\Services\Providers\Rawg\RawgCommerceLinkResolver;
use Illuminate\Support\Arr;
use Illuminate\Support\Facades\Http;
use Illuminate\Support\Facades\Log;
use Illuminate\Support\Str;

final class RawgProvider
{
    public const PROVIDER_KEY = 'rawg';

    private VideoGameSource $providerSource;

    public function __construct()
    {
        $meta = ProviderRegistry::meta(self::PROVIDER_KEY);

        $this->providerSource = VideoGameSource::query()->firstOrCreate(
            ['provider' => self::PROVIDER_KEY],
            [
                'provider_key' => $meta['provider_key'],
                'display_name' => $meta['display_name'],
                'category' => $meta['category'],
                'slug' => $meta['slug'],
                'metadata' => array_merge($meta['metadata'], [
                    'base_url' => $meta['base_url'],
                ]),
                'items_count' => 0,
            ]
        );
    }

    public function ensureSourceExists(): VideoGameSource
    {
        return $this->providerSource;
    }

    /**
     * Synchronous ingest for a list of RAWG game IDs.
     *
     * @param  array<int, int>  $rawgIds
     * @return array{created:int, updated:int, skipped:int, errors:int}
     */
    public function ingestRawgIds(array $rawgIds, bool $dispatchMediaJob = false): array
    {
        $stats = ['created' => 0, 'updated' => 0, 'skipped' => 0, 'errors' => 0];

        foreach ($rawgIds as $rawgId) {
            try {
                $result = $this->ingestRawgId((int) $rawgId, $dispatchMediaJob);

                if ($result['status'] === 'created') {
                    $stats['created']++;
                } elseif ($result['status'] === 'updated') {
                    $stats['updated']++;
                } else {
                    $stats['skipped']++;
                }
            } catch (\Throwable) {
                $stats['errors']++;
            }
        }

        return $stats;
    }

    /**
     * Ingest a single RAWG game ID.
     *
     * NOTE: This performs network I/O when executed. Do not run unless intended.
     */
    /**
     * @return array{status:'created'|'updated'|'skipped', video_game_id:int|null}
     */
    public function ingestRawgId(int $rawgId, bool $dispatchMediaJob = false): array
    {
        $apiKey = (string) config('services.rawg.api_key');
        if ($apiKey === '') {
            return ['status' => 'skipped', 'video_game_id' => null];
        }

        $baseUrl = (string) config('services.rawg.base_url', 'https://api.rawg.io/api');
        $timeout = (int) config('services.rawg.timeout', 10);

        $response = Http::timeout($timeout)->get(rtrim($baseUrl, '/')."/games/{$rawgId}", [
            'key' => $apiKey,
        ]);

        if (! $response->successful()) {
            return ['status' => 'skipped', 'video_game_id' => null];
        }

        /** @var array<string, mixed> $payload */
        $payload = $response->json();

        $name = trim((string) ($payload['name'] ?? ''));
        if ($name === '') {
            return ['status' => 'skipped', 'video_game_id' => null];
        }

        $normalizedTitle = Str::lower(preg_replace('/[^a-z0-9]+/', '', Str::ascii($name)) ?? '');
        $slug = Str::slug($name);

        // Product: store external_ids.rawg and metadata.providers (array)
        $product = Product::query()->firstOrCreate(
            ['slug' => $slug],
            [
                'type' => 'video_game',
                'name' => $name,
                'title' => $name,
                'normalized_title' => $normalizedTitle,
            ]
        );

        $externalIds = is_array($product->external_ids) ? $product->external_ids : [];
        $externalIds['rawg'] = $rawgId;
        $product->external_ids = $externalIds;

        $metadata = is_array($product->metadata) ? $product->metadata : [];
        $providers = Arr::wrap($metadata['providers'] ?? []);
        if (! in_array(self::PROVIDER_KEY, $providers, true)) {
            $providers[] = self::PROVIDER_KEY;
        }
        $metadata['providers'] = array_values(array_unique(array_filter($providers)));
        $product->metadata = $metadata;
        $product->save();

        $title = VideoGameTitle::query()->firstOrCreate(
            [
                'product_id' => $product->id,
                'normalized_title' => $normalizedTitle,
            ],
            [
                'name' => $name,
                'slug' => $slug.'-'.$product->id,
                'providers' => [self::PROVIDER_KEY],
            ]
        );

        $existingProviders = is_array($title->providers) ? $title->providers : [];
        if (! in_array(self::PROVIDER_KEY, $existingProviders, true)) {
            $title->providers = array_values(array_unique(array_merge($existingProviders, [self::PROVIDER_KEY])));
            $title->save();
        }

        VideoGameTitleSource::query()->updateOrCreate(
            [
                'video_game_title_id' => $title->id,
                'video_game_source_id' => $this->providerSource->id,
                'provider' => self::PROVIDER_KEY,
                'provider_item_id' => $rawgId,
            ],
            [
                'external_id' => $rawgId,
                'slug' => $slug,
                'name' => $name,
                'description' => $payload['description_raw'] ?? null,
                'release_date' => $payload['released'] ?? null,
                'platform' => collect($payload['platforms'] ?? [])
                    ->map(fn ($p) => $p['platform']['name'] ?? null)
                    ->filter()
                    ->values()
                    ->all(),
                'genre' => collect($payload['genres'] ?? [])
                    ->map(fn ($g) => $g['name'] ?? null)
                    ->filter()
                    ->values()
                    ->all(),
                'rating' => is_numeric($payload['rating'] ?? null)
                    ? ((float) $payload['rating']) * 20
                    : null,
                'rating_count' => is_numeric($payload['ratings_count'] ?? null)
                    ? (int) $payload['ratings_count']
                    : null,
                'raw_payload' => $payload,
            ]
        );

        // Resolve store links into provider mappings (Steam/PS/Xbox/etc.).
        // RAWG provides store links via /games/{id}/stores.
        $stores = $this->fetchStoreLinks($apiKey, $rawgId, $payload);
        $this->resolveAndPersistCommerceLinks($title, $stores);

        $videoGame = VideoGame::query()->updateOrCreate(
            [
                'provider' => self::PROVIDER_KEY,
                'external_id' => (string) $rawgId,
            ],
            [
                'video_game_title_id' => $title->id,
                'slug' => $title->slug,
                'name' => $name,
                'summary' => $payload['description_raw'] ?? null,
                'release_date' => $payload['released'] ?? null,
                'rating' => is_numeric($payload['rating'] ?? null)
                    ? ((float) $payload['rating']) * 20
                    : null,
                'rating_count' => is_numeric($payload['ratings_count'] ?? null)
                    ? (int) $payload['ratings_count']
                    : null,
                'platform' => collect($payload['platforms'] ?? [])
                    ->map(fn ($p) => $p['platform']['name'] ?? null)
                    ->filter()
                    ->values()
                    ->all(),
                'genre' => collect($payload['genres'] ?? [])
                    ->map(fn ($g) => $g['name'] ?? null)
                    ->filter()
                    ->values()
                    ->all(),
                'url' => $payload['website'] ?? null,
                'source_payload' => $payload,
                'attributes' => [
                    'metacritic' => $payload['metacritic'] ?? null,
                    'esrb_rating' => $payload['esrb_rating']['name'] ?? null,
                ],
            ]
        );

        $this->providerSource->recordVideoGameId((int) $videoGame->id);

        if ($dispatchMediaJob) {
            FetchRawgDataJob::dispatch((int) $videoGame->id, $rawgId)->onQueue('media-rawg');
        }

        return [
            'status' => $videoGame->wasRecentlyCreated ? 'created' : 'updated',
            'video_game_id' => (int) $videoGame->id,
        ];
    }

    /**
     * @param  array<string, mixed>  $payload
     */
    /**
     * @param  array<int, array<string, mixed>>  $stores
     */
    private function resolveAndPersistCommerceLinks(VideoGameTitle $title, array $stores): void
    {
        if ($stores === []) {
            return;
        }

        $resolver = new RawgCommerceLinkResolver;
        $resolved = $resolver->resolve($stores);

        if ($resolved !== []) {
            Log::info('RAWG ingest: resolved store mappings', [
                'title_id' => $title->id,
                'providers' => array_values(array_unique(array_map(static fn ($r) => $r['provider'], $resolved))),
                'count' => count($resolved),
            ]);
        }

        foreach ($resolved as $row) {
            $provider = $row['provider'];
            $providerItemId = $row['provider_item_id'];
            $rawId = $row['raw_id'];
            $url = $row['url'];

            $source = $this->ensureProviderSource($provider);

            VideoGameTitleSource::query()->updateOrCreate(
                [
                    'video_game_title_id' => $title->id,
                    'video_game_source_id' => $source->id,
                    'provider' => $provider,
                    'provider_item_id' => $providerItemId,
                ],
                [
                    // NOTE: schema requires bigint; for non-numeric store IDs we hash to provider_item_id.
                    'external_id' => $providerItemId,
                    'name' => $title->name,
                    'slug' => $title->slug,
                    'raw_payload' => [
                        'discovered_via' => 'rawg_stores',
                        'rawg_store_id' => $rawId,
                        'rawg_store_url' => $url,
                    ],
                ]
            );

            // Also stamp the product external_ids/providers for cheap lookup.
            if ($title->product) {
                $product = $title->product;

                $externalIds = is_array($product->external_ids) ? $product->external_ids : [];
                if (! array_key_exists($provider, $externalIds)) {
                    $externalIds[$provider] = $providerItemId;
                    $product->external_ids = $externalIds;
                }

                $metadata = is_array($product->metadata) ? $product->metadata : [];
                $providers = Arr::wrap($metadata['providers'] ?? []);
                if (! in_array($provider, $providers, true)) {
                    $providers[] = $provider;
                    $metadata['providers'] = array_values(array_unique(array_filter($providers)));
                    $product->metadata = $metadata;
                }

                if ($product->isDirty()) {
                    $product->save();
                }
            }
        }
    }

    /**
     * NOTE: Network I/O.
     *
     * @return array<int, array<string, mixed>>
     */
    /**
     * @param  array<string, mixed>  $gamePayload
     */
    private function fetchStoreLinks(string $apiKey, int $rawgId, array $gamePayload): array
    {
        $baseUrl = (string) config('services.rawg.base_url', 'https://api.rawg.io/api');
        $timeout = (int) config('services.rawg.timeout', 10);

        // RAWG /games/{id} includes store metadata (name/slug) but often has empty url.
        // /games/{id}/stores includes the urls but only store_id. Merge them.
        $storeMeta = [];
        $payloadStores = $gamePayload['stores'] ?? null;
        if (is_array($payloadStores)) {
            foreach ($payloadStores as $ps) {
                if (! is_array($ps)) {
                    continue;
                }

                $store = $ps['store'] ?? null;
                if (! is_array($store)) {
                    continue;
                }

                $storeId = $store['id'] ?? null;
                if (is_int($storeId) || (is_string($storeId) && ctype_digit($storeId))) {
                    $storeMeta[(int) $storeId] = $store;
                }
            }
        }

        $response = Http::timeout($timeout)->get(rtrim($baseUrl, '/')."/games/{$rawgId}/stores", [
            'key' => $apiKey,
            'page_size' => 40,
        ]);

        if (! $response->successful()) {
            return [];
        }

        $results = $response->json('results');
        if (! is_array($results)) {
            return [];
        }

        $merged = [];
        foreach ($results as $row) {
            if (! is_array($row)) {
                continue;
            }

            $storeId = $row['store_id'] ?? null;
            $url = $row['url'] ?? null;
            if (! (is_int($storeId) || (is_string($storeId) && ctype_digit($storeId)))) {
                continue;
            }
            if (! is_string($url) || $url === '') {
                continue;
            }

            $merged[] = [
                'id' => $row['id'] ?? null,
                'url' => $url,
                'store_id' => (int) $storeId,
                'store' => $storeMeta[(int) $storeId] ?? null,
            ];
        }

        if ($merged !== []) {
            Log::debug('RAWG ingest: store links fetched', [
                'rawg_id' => $rawgId,
                'count' => count($merged),
            ]);
        }

        return $merged;
    }

    private function ensureProviderSource(string $provider): VideoGameSource
    {
        $provider = strtolower(trim($provider));

        $meta = ProviderRegistry::meta($provider);

        return VideoGameSource::query()->firstOrCreate(
            ['provider' => $provider],
            [
                'provider_key' => $meta['provider_key'],
                'display_name' => $meta['display_name'],
                'category' => $meta['category'],
                'slug' => $meta['slug'],
                'metadata' => array_merge($meta['metadata'], [
                    'base_url' => $meta['base_url'],
                ]),
                'items_count' => 0,
            ]
        );
    }

    /**
     * Discover top games for a RAWG genre slug.
     *
     * NOTE: Network I/O.
     *
     * @return array<int, array<string, mixed>>
     */
    public function discoverTopGamesByGenre(string $genreSlug, int $perGenre = 50, string $ordering = '-rating'): array
    {
        $apiKey = (string) config('services.rawg.api_key');
        if ($apiKey === '') {
            return [];
        }

        $baseUrl = (string) config('services.rawg.base_url', 'https://api.rawg.io/api');
        $timeout = (int) config('services.rawg.timeout', 10);

        $response = Http::timeout($timeout)->get(rtrim($baseUrl, '/').'/games', [
            'key' => $apiKey,
            'genres' => $genreSlug,
            'ordering' => $ordering,
            'page_size' => min(50, max(1, $perGenre)),
        ]);

        if (! $response->successful()) {
            return [];
        }

        $results = $response->json('results');

        return is_array($results) ? $results : [];
    }

    /**
     * Discover top games without genre constraint (e.g. "trending" style lists).
     *
     * NOTE: Network I/O.
     *
     * @return array<int, array<string, mixed>>
     */
    public function discoverTopGames(int $count = 50, string $ordering = '-added'): array
    {
        $apiKey = (string) config('services.rawg.api_key');
        if ($apiKey === '') {
            return [];
        }

        $baseUrl = (string) config('services.rawg.base_url', 'https://api.rawg.io/api');
        $timeout = (int) config('services.rawg.timeout', 10);

        $response = Http::timeout($timeout)->get(rtrim($baseUrl, '/').'/games', [
            'key' => $apiKey,
            'ordering' => $ordering,
            'page_size' => min(50, max(1, $count)),
        ]);

        if (! $response->successful()) {
            return [];
        }

        $results = $response->json('results');

        return is_array($results) ? $results : [];
    }

    /**
     * Discover upcoming games (date-filtered).
     *
     * NOTE: Network I/O.
     *
     * @return array<int, array<string, mixed>>
     */
    public function discoverUpcomingGames(int $count = 50, int $daysAhead = 365): array
    {
        $apiKey = (string) config('services.rawg.api_key');
        if ($apiKey === '') {
            return [];
        }

        $baseUrl = (string) config('services.rawg.base_url', 'https://api.rawg.io/api');
        $timeout = (int) config('services.rawg.timeout', 10);

        $from = now()->toDateString();
        $to = now()->addDays(max(1, $daysAhead))->toDateString();

        $response = Http::timeout($timeout)->get(rtrim($baseUrl, '/').'/games', [
            'key' => $apiKey,
            'dates' => $from.','.$to,
            'ordering' => 'released',
            'page_size' => min(50, max(1, $count)),
        ]);

        if (! $response->successful()) {
            return [];
        }

        $results = $response->json('results');

        return is_array($results) ? $results : [];
    }

    /**
     * NOTE: Network I/O.
     *
     * @return array<int, string>
     */
    public function discoverGenreSlugs(int $pageSize = 40): array
    {
        $apiKey = (string) config('services.rawg.api_key');
        if ($apiKey === '') {
            return [];
        }

        $baseUrl = (string) config('services.rawg.base_url', 'https://api.rawg.io/api');
        $timeout = (int) config('services.rawg.timeout', 10);

        $response = Http::timeout($timeout)->get(rtrim($baseUrl, '/').'/genres', [
            'key' => $apiKey,
            'page_size' => min(100, max(1, $pageSize)),
        ]);

        if (! $response->successful()) {
            return [];
        }

        $results = $response->json('results');
        if (! is_array($results)) {
            return [];
        }

        return collect($results)
            ->map(fn ($g) => is_array($g) ? ($g['slug'] ?? null) : null)
            ->filter(fn ($s) => is_string($s) && $s !== '')
            ->unique()
            ->values()
            ->all();
    }
}
