<?php

declare(strict_types=1);

namespace App\Services\Catalogue;

use App\Models\Product;
use App\Models\Retailer;
use App\Models\TheGamesDbGame;
use App\Models\VideoGame;
use App\Models\VideoGamePrice;
use App\Models\VideoGameTitle;
use App\Models\VideoGameTitleSource;
use App\Services\Nexarda\NexardaClient;
use App\Support\Platforms\PlatformFamilyDetector;
use App\Support\Strings\GameNameNormalizer;
use Generator;
use Illuminate\Support\Arr;
use Illuminate\Support\Collection;
use Illuminate\Support\Facades\Cache;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\File;
use Illuminate\Support\Facades\Schema;
use Illuminate\Support\Str;
use SplFileObject;
use Throwable;

class PriceCrossReferencer
{
    private const CACHE_VERSION = 'v3';

    protected $casts = [
        'has_digital' => 'boolean',
        'has_physical' => 'boolean',
    ];

    public function __construct(
        private readonly ?string $giantBombFile = null,
        private readonly ?string $nexardaFile = null,
        private readonly ?string $priceGuideFile = null,
        private readonly ?int $cacheMinutes = null,
    ) {}

    public function build(): Collection
    {
        $ttl = now()->addMinutes(max(1, $this->cacheMinutes ?? (int) config('catalogue.cross_reference.cache_minutes', 45)));

        if ($this->shouldBypassPersistentCache()) {
            return collect(iterator_to_array($this->stream(), preserve_keys: false));
        }

        $cacheKey = $this->cacheKey();

        return Cache::remember($cacheKey, $ttl, function (): Collection {
            return collect(iterator_to_array($this->stream(), preserve_keys: false));
        });
    }

    public function stream(): Generator
    {
        [$keys, $giantBomb, $nexarda, $priceGuide, $mirror] = $this->prepareIndexes();

        foreach ($keys as $key) {
            $entry = $this->prepareEntry((string) $key, $giantBomb, $nexarda, $priceGuide, $mirror);

            if ($entry !== null) {
                yield $entry;
            }
        }
    }

    private function shouldBypassPersistentCache(): bool
    {
        if (config('catalogue.cross_reference.runtime_only', false)) {
            return true;
        }

        return Cache::getDefaultDriver() === 'file';
    }

    private function prepareIndexes(): array
    {
        // $giantBomb = $this->parseGiantBomb();
        $giantBomb = collect(); // Disabled for performance per user request
        $nexarda = $this->indexNexarda();
        $priceGuide = $this->indexPriceGuide();
        $mirror = $this->indexTheGamesDb();

        // Merge DB-backed Nexarda data if available
        $nexardaDb = $this->loadNexardaFromDatabase();
        if ($nexardaDb->isNotEmpty()) {
            $nexarda = $nexarda->merge($nexardaDb);
        }

        if ($giantBomb->isEmpty() && $nexarda->isEmpty() && $priceGuide->isEmpty() && $mirror->isEmpty()) {
            return [collect(), collect(), collect(), collect(), collect()];
        }

        $keys = collect()
            ->merge($giantBomb->keys())
            ->merge($nexarda->keys())
            ->merge($priceGuide->keys())
            ->merge($mirror->keys())
            ->unique()
            ->values();

        return [$keys, $giantBomb, $nexarda, $priceGuide, $mirror];
    }

    private function prepareEntry(string $key, Collection $giantBomb, Collection $nexarda, Collection $priceGuide, Collection $mirror): ?array
    {
        $game = $giantBomb->get($key);
        $digital = $nexarda->get($key);
        $mirrorEntry = $mirror->get($key);

        if (is_array($digital)) {
            $currencies = collect($digital['currencies'] ?? [])
                ->filter(fn (array $row) => array_key_exists('amount', $row))
                ->values();

            if ($currencies->isEmpty()) {
                $digital = null;
            } else {
                $digital['currencies'] = $currencies->all();
                $digital['best'] = $currencies
                    ->filter(fn (array $row) => $row['amount'] !== null)
                    ->sortBy('amount')
                    ->first();
            }
        } else {
            $digital = null;
        }

        $rawPhysical = $priceGuide->get($key, []);
        $physicalAll = collect(is_array($rawPhysical) ? $rawPhysical : [])
            ->filter(fn ($row) => is_array($row))
            ->map(function (array $row) {
                // Some downstream consumers (and a few historical mirrors) provide only a formatted value.
                // Ensure we always have a numeric `price` available for comparisons and sorting.
                if (! array_key_exists('price', $row) && isset($row['formatted_price'])) {
                    $row['price'] = $this->normalizeNumber($row['formatted_price']);
                }

                if (! isset($row['formatted_price']) && ($row['price'] ?? null) !== null) {
                    $row['formatted_price'] = $this->formatUsd((float) $row['price'], null);
                }

                return $row;
            })
            ->values();

        $bestPhysical = $physicalAll
            ->filter(fn (array $row) => $row['price'] !== null)
            ->sortBy('price')
            ->first();

        $physical = $physicalAll
            ->sortBy(function (array $row) {
                return $row['price'] ?? INF;
            })
            ->take(6)
            ->values();

        if (! $game && ! $digital && $physical->isEmpty() && ! $mirrorEntry) {
            return null;
        }

        $name = $game['name']
            ?? ($mirrorEntry['name'] ?? null)
            ?? ($digital['name'] ?? null)
            ?? ($physical->first()['product_name'] ?? null)
            ?? (string) Str::of($key)->headline();

        $platforms = collect()
            ->merge($game['platforms'] ?? [])
            ->merge($digital['platforms'] ?? [])
            ->merge($mirrorEntry ? array_filter([$mirrorEntry['platform'] ?? null]) : [])
            ->merge($physical->pluck('console')->filter()->all())
            ->filter()
            ->map(fn ($value) => is_string($value) ? $value : null)
            ->filter()
            ->unique()
            ->values()
            ->all();

        $image = $game['image'] ?? ($mirrorEntry['image'] ?? null);
        $image = is_string($image) ? trim($image) : null;

        if ($image === null || $image === '') {
            $image = $this->resolveFallbackImageFromDatabase(
                normalized: $key,
                name: $name,
                platforms: $platforms
            );

            // Only fall back to a placeholder when *all* DB-backed sources fail.
            if ($image === null || $image === '') {
                $image = $this->placeholderImageUrl();
            }
        }

        return [
            'guid' => $game['guid']
                ?? ($mirrorEntry && isset($mirrorEntry['id']) ? 'mirror-'.$mirrorEntry['id'] : null),
            'name' => $name,
            'image' => $image,
            'normalized' => $key,
            'digital' => $digital,
            'physical' => $physical->all(),
            'best_physical' => $bestPhysical,
            'platforms' => $platforms,
            'has_digital' => $digital !== null,
            'has_physical' => $physical->isNotEmpty(),
        ];
    }

    /**
     * When cross-reference entries lack a primary image (common for price-guide-only matches),
     * attempt to resolve an image from already-ingested, DB-backed provider title rows.
     *
     * Order matters: prefer richer metadata sources first.
     */
    private function resolveFallbackImageFromDatabase(string $normalized, ?string $name, array $platforms): ?string
    {
        return null; // Disabled for performance - causing timeouts on build()
        // Avoid expensive lookups when the schema isn't present (e.g., partial test schema).
        if (! Schema::hasTable('video_game_titles') || ! Schema::hasTable('video_game_sources')) {
            return null;
        }

        $candidates = [$normalized];

        $normalizedFromName = $this->normalizeName($name);
        if (is_string($normalizedFromName) && $normalizedFromName !== '') {
            $candidates[] = $normalizedFromName;
        }

        $candidates = array_values(array_unique(array_filter($candidates, fn ($v) => is_string($v) && $v !== '')));

        if ($candidates === []) {
            return null;
        }

        $providerPriority = $this->providerKeysForImageLookup($platforms);

        $select = [
            'vgs.provider_key as provider_key',
            'vgt.links as links',
            'vgt.media as media',
            'vgt.payload as payload',
        ];

        // Optional columns vary across historical migrations.
        foreach (['provider_item_id', 'title', 'raw_title'] as $column) {
            if (Schema::hasColumn('video_game_titles', $column)) {
                $select[] = "vgt.{$column} as {$column}";
            }
        }

        try {
            $rows = DB::table('video_game_titles as vgt')
                ->join('video_game_sources as vgs', 'vgs.id', '=', 'vgt.video_game_source_id')
                ->whereIn('vgt.normalized_title', $candidates)
                ->select($select)
                ->get();
        } catch (Throwable) {
            return null;
        }

        if ($rows->isEmpty()) {
            return null;
        }

        $byProvider = [];

        foreach ($rows as $row) {
            $providerKey = is_string($row->provider_key ?? null) ? $row->provider_key : null;
            if (! $providerKey) {
                continue;
            }

            $byProvider[$providerKey][] = $row;
        }

        foreach ($providerPriority as $providerKey) {
            foreach (($byProvider[$providerKey] ?? []) as $row) {
                $links = $this->decodeJson($row->links ?? null);
                $media = $this->decodeJson($row->media ?? null);
                $payload = $this->decodeJson($row->payload ?? null);

                $image = $this->resolveCanonicalImage($links, $media, $payload);

                if (is_string($image) && trim($image) !== '') {
                    return trim($image);
                }
            }
        }

        return null;
    }

    /**
     * Preferred provider order for image lookups.
     *
     * Platform-aware: only query store sources if the title appears to belong to that family.
     * This keeps the logic deterministic and avoids unnecessary/incorrect store biases.
     *
     * @return array<int, string>
     */
    private function providerKeysForImageLookup(array $platforms): array
    {
        $providerKeys = [
            // Resource-rich metadata sources
            'rawg',
            'igdb',
            'thegamesdb',
            'giantbomb',
        ];

        if ($this->looksLikePlayStationFamily($platforms)) {
            $providerKeys[] = 'playstation_store';
        }

        if ($this->looksLikeXboxFamily($platforms)) {
            // In this repo we model “Xbox store” under Microsoft Store.
            $providerKeys[] = 'microsoft_store';
        }

        return $providerKeys;
    }

    private function looksLikePlayStationFamily(array $platforms): bool
    {
        return PlatformFamilyDetector::looksLikePlayStationFamily($platforms);
    }

    private function looksLikeXboxFamily(array $platforms): bool
    {
        return PlatformFamilyDetector::looksLikeXboxFamily($platforms);
    }

    private function placeholderImageUrl(): string
    {
        // Placeholder should be used only as a last resort.
        // Keep consistent with the compare UI placeholder.
        return asset('images/placeholders/game-cover.svg');
    }

    private function cacheKey(): string
    {
        return 'catalogue:crossref:'.self::CACHE_VERSION.':giantbomb-nexarda-priceguide';
    }

    private function parseGiantBomb(): Collection
    {
        $fromDatabase = $this->loadGiantBombFromDatabase();
        if ($fromDatabase->isNotEmpty()) {
            return $fromDatabase;
        }

        return $this->loadGiantBombFromFile();
    }

    private function loadGiantBombFromDatabase(): Collection
    {
        $legacyMirror = $this->loadGiantBombFromLegacyMirrorTable();
        if ($legacyMirror->isNotEmpty()) {
            return $legacyMirror;
        }

        $index = [];

        try {
            $rows = DB::table('video_game_titles as vgt')
                ->join('video_game_sources as vgs', 'vgs.id', '=', 'vgt.video_game_source_id')
                ->where('vgs.provider_key', 'giantbomb')
                ->select([
                    'vgt.id',
                    'vgt.normalized_title',
                    'vgt.name',
                    'vgt.raw_title',
                    'vgt.payload',
                    'vgt.links',
                    'vgt.media',
                    'vgt.provider_item_id',
                ])
                ->orderBy('vgt.id')
                ->get();

            foreach ($rows as $row) {
                $normalized = is_string($row->normalized_title)
                    ? $row->normalized_title
                    : $this->normalizeName($row->name ?? $row->raw_title ?? null);

                if ($normalized === null) {
                    continue;
                }

                $payload = $this->decodeJson($row->payload ?? null);
                $links = $this->decodeJson($row->links ?? null);
                $media = $this->decodeJson($row->media ?? null);

                $image = $this->resolveCanonicalImage($links, $media, $payload);
                $platforms = $this->resolveCanonicalPlatforms($links, $payload);

                $name = is_string($row->name) && $row->name !== ''
                    ? $row->name
                    : (is_string($row->raw_title) ? $row->raw_title : null);

                $index[$normalized] = [
                    'guid' => is_string($row->provider_item_id) ? $row->provider_item_id : null,
                    'name' => $name,
                    'image' => $image,
                    'normalized' => $normalized,
                    'platforms' => $platforms,
                ];
            }
        } catch (Throwable) {
            return collect();
        }

        return collect($index);
    }

    /**
     * Legacy support: the test suite (and some older ingestion paths) still persist mirrored
     * Giant Bomb games into the `giant_bomb_games` table. When that table exists *and*
     * has the expected columns (i.e., it is not the reduced Postgres view), prefer it
     * over file-based JSON.
     */
    private function loadGiantBombFromLegacyMirrorTable(): Collection
    {
        if (! Schema::hasTable('giant_bomb_games')) {
            return collect();
        }

        // If this is the Postgres compatibility view (created in 2025_12_27_050000...),
        // it will not include the legacy columns we need for cross-referencing.
        if (! Schema::hasColumn('giant_bomb_games', 'name')) {
            return collect();
        }

        $select = ['id', 'name'];

        foreach ([
            'guid',
            'normalized_name',
            'primary_image_url',
            'image_original_url',
            'image_super_url',
            'image_small_url',
            'image',
            'platforms',
        ] as $column) {
            if (Schema::hasColumn('giant_bomb_games', $column)) {
                $select[] = $column;
            }
        }

        $index = [];

        try {
            $rows = DB::table('giant_bomb_games')
                ->select($select)
                ->orderBy('id')
                ->get();

            foreach ($rows as $row) {
                $name = is_string($row->name) ? trim($row->name) : '';
                if ($name === '') {
                    continue;
                }

                $normalized = null;
                if (property_exists($row, 'normalized_name') && is_string($row->normalized_name) && $row->normalized_name !== '') {
                    $normalized = $row->normalized_name;
                } else {
                    $normalized = $this->normalizeName($name);
                }

                if ($normalized === null) {
                    continue;
                }

                $image = null;
                foreach (['primary_image_url', 'image_original_url', 'image_super_url', 'image_small_url'] as $candidate) {
                    if (property_exists($row, $candidate) && is_string($row->{$candidate}) && trim($row->{$candidate}) !== '') {
                        $image = trim($row->{$candidate});
                        break;
                    }
                }

                if ($image === null && property_exists($row, 'image')) {
                    $decoded = $this->decodeJson($row->image ?? null);
                    if (is_array($decoded)) {
                        $image = Arr::first([
                            Arr::get($decoded, 'original_url'),
                            Arr::get($decoded, 'super_url'),
                            Arr::get($decoded, 'small_url'),
                        ], fn ($v) => is_string($v) && trim($v) !== '');

                        $image = is_string($image) ? trim($image) : null;
                    }
                }

                $platforms = [];
                if (property_exists($row, 'platforms')) {
                    $decodedPlatforms = $this->decodeJson($row->platforms ?? null);
                    if (is_array($decodedPlatforms)) {
                        $platforms = collect($decodedPlatforms)
                            ->filter(fn ($p) => is_string($p) && trim($p) !== '')
                            ->values()
                            ->all();
                    }
                }

                $index[$normalized] = [
                    'guid' => property_exists($row, 'guid') && is_string($row->guid) ? $row->guid : null,
                    'name' => $name,
                    'image' => $image,
                    'normalized' => $normalized,
                    'platforms' => $platforms,
                ];
            }
        } catch (Throwable) {
            return collect();
        }

        return collect($index);
    }

    private function loadGiantBombFromFile(): Collection
    {
        $path = $this->resolvePath($this->giantBombFile ?? config('catalogue.cross_reference.giant_bomb_catalogue_file'))
            ?? base_path('giant_bomb_games_detailed.json');

        if (! is_string($path) || $path === '' || ! is_file($path)) {
            return collect();
        }

        $normalizedIndex = [];

        foreach ($this->iterateGiantBombFile($path) as $guid => $row) {
            if (! is_array($row)) {
                continue;
            }

            $name = $row['name'] ?? null;
            $normalized = $this->normalizeName($name);

            if (! $name || ! $normalized) {
                continue;
            }

            $normalizedIndex[$normalized] = [
                'guid' => is_string($guid) ? $guid : ($row['guid'] ?? null),
                'name' => $name,
                'image' => $this->resolveImage($row),
                'normalized' => $normalized,
                'platforms' => $this->extractPlatforms($row),
            ];
        }

        return collect($normalizedIndex);
    }

    /**
     * @return iterable<int|string, mixed>
     */
    private function iterateGiantBombFile(string $path): iterable
    {
        try {
            if (class_exists('JsonMachine\\JsonMachine')) {
                $options = [];

                $decoderClass = 'JsonMachine\\JsonDecoder\\ExtJsonDecoder';
                if (class_exists($decoderClass)) {
                    $options['decoder'] = new $decoderClass(true);
                }

                $jsonMachineClass = 'JsonMachine\\JsonMachine';

                /** @var iterable<int|string, mixed> $stream */
                $stream = $jsonMachineClass::fromFile($path, '', $options);

                return $stream;
            }

            $payload = json_decode(File::get($path), true, 512, JSON_THROW_ON_ERROR);

            return is_array($payload) ? $payload : [];
        } catch (Throwable) {
            return [];
        }
    }

    private function decodeJson(mixed $value): array
    {
        if (is_array($value)) {
            return $value;
        }

        if (is_string($value)) {
            $decoded = json_decode($value, true);

            return is_array($decoded) ? $decoded : [];
        }

        return [];
    }

    private function resolveCanonicalImage(array $links, array $media, array $payload): ?string
    {
        $candidates = [];

        if (isset($media['cover_url']) && is_string($media['cover_url'])) {
            $candidates[] = $media['cover_url'];
        }

        if (isset($links['primary_image_url']) && is_string($links['primary_image_url'])) {
            $candidates[] = $links['primary_image_url'];
        }

        if (isset($links['images']) && is_array($links['images'])) {
            foreach ($links['images'] as $img) {
                if (is_string($img)) {
                    $candidates[] = $img;
                } elseif (is_array($img)) {
                    foreach (['super_url', 'original_url', 'small_url'] as $key) {
                        if (isset($img[$key]) && is_string($img[$key])) {
                            $candidates[] = $img[$key];
                        }
                    }
                }
            }
        }

        if (isset($payload['image']) && is_array($payload['image'])) {
            foreach (['super_url', 'original_url', 'small_url'] as $key) {
                if (isset($payload['image'][$key]) && is_string($payload['image'][$key])) {
                    $candidates[] = $payload['image'][$key];
                }
            }
        }

        foreach ($candidates as $candidate) {
            if (is_string($candidate) && trim($candidate) !== '') {
                return trim($candidate);
            }
        }

        return null;
    }

    private function resolveCanonicalPlatforms(array $links, array $payload): array
    {
        $platforms = [];

        $fromLinks = Arr::get($links, 'platforms', []);
        if (is_array($fromLinks)) {
            $platforms = array_merge($platforms, array_filter(array_map(fn ($v) => is_string($v) ? $v : null, $fromLinks)));
        }

        $fromPayload = Arr::get($payload, 'platforms', []);
        if (is_array($fromPayload)) {
            $platforms = array_merge($platforms, array_filter(array_map(fn ($v) => is_string($v) ? $v : null, $fromPayload)));
        }

        return collect($platforms)
            ->filter(fn ($value) => is_string($value) && trim($value) !== '')
            ->unique()
            ->values()
            ->all();
    }

    private function indexNexarda(): Collection
    {
        $path = $this->resolvePath($this->nexardaFile ?? config('catalogue.nexarda.local_catalogue_file'))
            ?? base_path('nexarda_product_catalogue.json');

        if (! is_string($path) || $path === '' || ! is_file($path)) {
            return collect();
        }

        try {
            $payload = json_decode(File::get($path), true, 512, JSON_THROW_ON_ERROR);
        } catch (Throwable) {
            return collect();
        }

        $games = $payload['games'] ?? $payload['items'] ?? $payload;
        if (! is_array($games)) {
            return collect();
        }

        return collect($games)
            ->filter(fn ($row) => is_array($row))
            ->mapWithKeys(function (array $row) {
                $name = $row['name'] ?? null;
                $normalized = $this->normalizeName($name);
                if (! $normalized) {
                    return [];
                }

                $platformCandidates = array_filter([
                    is_string($row['platform'] ?? null) ? $row['platform'] : null,
                    is_string($row['platform_name'] ?? null) ? $row['platform_name'] : null,
                    is_string($row['console'] ?? null) ? $row['console'] : null,
                ], fn ($v) => is_string($v) && trim($v) !== '');

                $platforms = collect($platformCandidates)
                    ->merge($this->extractPlatforms($row))
                    ->map(function ($value) {
                        if (! is_string($value)) {
                            return null;
                        }

                        $value = trim($value);
                        if ($value === '') {
                            return null;
                        }

                        // Nexarda sometimes prefixes platform names with region hints.
                        $value = preg_replace('/^(PAL|JP)[\s\-_:]+/i', '', $value) ?? $value;

                        $value = trim($value);

                        return $value !== '' ? $value : null;
                    })
                    ->filter()
                    ->unique()
                    ->values()
                    ->all();

                $prices = $row['prices'] ?? [];
                $discounts = $row['discounts'] ?? [];

                $currencies = collect($prices)
                    ->map(function ($value, $currency) use ($discounts) {
                        $amount = $this->normalizeNumber($value);
                        $discount = $this->normalizeNumber($discounts[$currency] ?? null);

                        if ($amount === null) {
                            return null;
                        }

                        $code = strtoupper((string) $currency);

                        return [
                            'code' => $code,
                            'amount' => $amount,
                            'formatted' => $amount === 0.0 ? 'Free' : $this->formatCurrency($amount, $code),
                            'discount' => $discount !== null ? (int) round($discount) : null,
                            'is_free' => $amount === 0.0,
                        ];
                    })
                    ->filter()
                    ->values()
                    ->all();

                return [$normalized => [
                    'source' => 'nexarda',
                    'name' => $name,
                    'slug' => $this->sanitizeNexardaSlug($row['slug'] ?? null),
                    'url' => $this->buildNexardaUrl($row['slug'] ?? null),
                    'platforms' => $platforms,
                    'currencies' => $currencies,
                ]];
            });
    }

    private function loadNexardaFromDatabase(): Collection
    {
        if (! Schema::hasTable('video_game_prices') || ! Schema::hasTable('video_game_titles')) {
            return collect();
        }

        $index = [];

        try {
            $rows = DB::table('video_game_prices as vgp')
                ->join('video_games as vg', 'vg.id', '=', 'vgp.video_game_id')
                ->join('video_game_titles as vgt', 'vgt.id', '=', 'vg.video_game_title_id')
                ->where('vgp.is_active', true)
                ->where(function ($q) {
                    // Legacy Nexarda rows may not have condition.
                    $q->where('vg.provider', 'nexarda')
                        ->orWhere('vgp.condition', 'digital');
                })
                ->select([
                    'vgt.normalized_title',
                    'vgt.name as title_name',
                    'vgt.slug as title_slug',
                    'vgp.currency',
                    'vgp.amount_minor',
                    'vgp.retailer',
                    'vgp.url',
                    'vg.provider as game_provider',
                    'vg.attributes',
                    'vg.platform',
                ])
                ->get();

            foreach ($rows as $row) {
                $normalized = $row->normalized_title;
                if (! $normalized) {
                    continue;
                }

                $currency = $row->currency ?? null;
                if (! is_string($currency) || $currency === '') {
                    continue;
                }

                $code = strtoupper($currency);
                $divisor = $this->currencyMinorDivisor($code);
                $amountMinor = is_numeric($row->amount_minor ?? null) ? (int) $row->amount_minor : null;
                if ($amountMinor === null) {
                    continue;
                }

                $amount = $amountMinor / $divisor;

                if (! isset($index[$normalized])) {
                    $attributes = json_decode($row->attributes ?? '{}', true);
                    $platforms = json_decode($row->platform ?? '[]', true);

                    $platformList = [];
                    if (is_array($platforms) && $platforms !== []) {
                        $platformList = $platforms;
                    } elseif (is_array($attributes) && isset($attributes['platform']) && is_array($attributes['platform'])) {
                        $platformList = $attributes['platform'];
                    }

                    $source = ($row->game_provider ?? null) === 'nexarda'
                        ? 'nexarda_db'
                        : 'store_prices_db';

                    $index[$normalized] = [
                        'source' => $source,
                        'name' => $row->title_name,
                        'slug' => $row->title_slug,
                        'url' => $row->url,
                        'platforms' => $platformList,
                        'currencies' => [],
                    ];
                }

                // Prefer first non-empty URL.
                if ((! is_string($index[$normalized]['url'] ?? null) || trim((string) $index[$normalized]['url']) === '')
                    && is_string($row->url)
                    && trim($row->url) !== '') {
                    $index[$normalized]['url'] = $row->url;
                }

                $index[$normalized]['currencies'][] = [
                    'code' => $code,
                    'amount' => $amount,
                    'formatted' => $amount === 0.0 ? 'Free' : $this->formatCurrency($amount, $code),
                    'retailer' => $row->retailer,
                ];
            }
        } catch (Throwable) {
            return collect();
        }

        return collect($index);
    }

    private function currencyMinorDivisor(string $currency): int
    {
        // Minimal set: we only ingest JPY in the default store markets.
        // Add more zero-decimal currencies only when needed.
        return in_array(strtoupper($currency), ['JPY', 'KRW', 'VND'], true) ? 1 : 100;
    }

    private function indexPriceGuide(): Collection
    {
        // Source: PriceCharting CSV export (NOT ITAD). Column names match the PriceCharting Prices API.
        // Upstream docs: https://www.pricecharting.com/api-documentation
        // Local reference: docs/price_charting_api.md
        $databaseTable = config('catalogue.cross_reference.price_guide_table');
        $databaseConnection = config('catalogue.cross_reference.price_guide_connection');

        if (is_string($databaseTable) && $databaseTable !== '') {
            $databaseIndex = $this->indexPriceGuideFromDatabase($databaseTable, is_string($databaseConnection) && $databaseConnection !== '' ? $databaseConnection : null);

            if ($databaseIndex !== null) {
                return $databaseIndex;
            }
        }

        $path = $this->resolvePath($this->priceGuideFile ?? config('catalogue.cross_reference.price_guide_file'))
            ?? base_path('price-guide.csv');

        if (! is_string($path) || $path === '' || ! is_file($path)) {
            return collect();
        }

        try {
            $file = new SplFileObject($path);
        } catch (Throwable) {
            return collect();
        }

        $file->setFlags(SplFileObject::READ_CSV | SplFileObject::SKIP_EMPTY);
        $file->setCsvControl(',', '"', '\\');

        $headers = null;
        $rows = collect();

        $rowCount = 0;
        foreach ($file as $row) {
            if ($rowCount++ > 5000) {
                break;
            }
            if ($headers === null) {
                $headers = $row;

                continue;
            }

            if (! is_array($row)) {
                continue;
            }

            $data = $this->associateRow($headers, $row);
            if ($data === null) {
                continue;
            }

            $name = $data['product-name'] ?? null;
            $normalized = $this->normalizeName($name);
            if (! $normalized) {
                continue;
            }

            // The PriceCharting export commonly uses `loose-price`, but some mirrors use a formatted-only
            // column name (e.g. `formatted-price`). Be liberal in what we accept.
            $priceRaw = $data['loose-price']
                ?? $data['loose_price']
                ?? $data['formatted-price']
                ?? $data['formatted_price']
                ?? $data['price']
                ?? null;
            $price = $this->normalizeNumber($priceRaw);

            $console = isset($data['console-name']) ? trim((string) $data['console-name']) : null;
            if (is_string($console) && $console !== '') {
                // Some exports include region hints as prefixes (e.g., "JP Nintendo Switch", "PAL PS5").
                // Treat these as the same platform to avoid index fragmentation.
                $console = preg_replace('/^(PAL|JP)[\s\-_:]+/i', '', $console) ?? $console;
                $console = trim($console);
                $console = $console !== '' ? $console : null;
            }

            $formattedRaw = $data['formatted-price'] ?? $data['formatted_price'] ?? null;
            $formattedRaw = is_string($formattedRaw) ? trim($formattedRaw) : null;

            $rows->push([
                'normalized' => $normalized,
                'product_name' => $name,
                'id' => $data['id'] ?? null,
                'console' => $console,
                'price' => $price,
                'formatted_price' => $formattedRaw ?: $this->formatUsd($price, $priceRaw),
            ]);
        }

        return $rows
            ->groupBy('normalized')
            ->map(function (Collection $group): array {
                return $group
                    ->map(fn (array $row) => array_diff_key($row, ['normalized' => true]))
                    ->sortBy(function (array $row) {
                        return $row['price'] ?? INF;
                    })
                    ->values()
                    ->all();
            });
    }

    private function indexPriceGuideFromDatabase(string $table, ?string $connection): ?Collection
    {
        try {
            $schemaConnection = $connection ?? config('database.default');

            if (! Schema::connection($schemaConnection)->hasTable($table)) {
                return null;
            }
        } catch (Throwable) {
            return null;
        }

        try {
            $builder = ($connection ? DB::connection($connection) : DB::connection())->table($table);
        } catch (Throwable) {
            return null;
        }

        $grouped = [];

        $processRow = function ($row) use (&$grouped): void {
            if (is_array($row)) {
                $data = $row;
            } elseif (is_object($row)) {
                $data = (array) $row;
            } else {
                return;
            }

            $name = $data['product_name'] ?? ($data['name'] ?? null);
            $normalized = $data['normalized'] ?? $this->normalizeName(is_string($name) ? $name : null);

            if (! is_string($normalized) || $normalized === '') {
                return;
            }

            // Mirrors vary widely; sometimes they persist only formatted prices.
            $priceRaw = $data['price']
                ?? ($data['loose_price'] ?? null)
                ?? ($data['loose-price'] ?? null)
                ?? ($data['formatted_price'] ?? null)
                ?? ($data['formatted-price'] ?? null);
            $price = $this->normalizeNumber($priceRaw);

            $console = $data['console'] ?? ($data['console_name'] ?? null);
            if (is_string($console) && trim($console) !== '') {
                $console = preg_replace('/^(PAL|JP)[\s\-_:]+/i', '', $console) ?? $console;
                $console = trim($console);
                $console = $console !== '' ? $console : null;
            }

            $formattedRaw = $data['formatted_price'] ?? ($data['formatted-price'] ?? null);
            $formattedRaw = is_string($formattedRaw) ? trim($formattedRaw) : null;

            $entry = [
                'product_name' => $name,
                'id' => $data['id'] ?? null,
                'console' => $console,
                'price' => $price,
                'formatted_price' => $formattedRaw ?: $this->formatUsd($price, $priceRaw),
            ];

            $grouped[$normalized][] = $entry;
        };

        $schemaConnection = $connection ?? config('database.default');
        $hasIdColumn = false;

        try {
            $hasIdColumn = Schema::connection($schemaConnection)->hasColumn($table, 'id');
        } catch (Throwable) {
            $hasIdColumn = false;
        }

        $chunkSize = 1000;
        $orderColumn = 'product_name';

        try {
            $schemaInspector = Schema::connection($schemaConnection);

            if (! $schemaInspector->hasColumn($table, $orderColumn) && $schemaInspector->hasColumn($table, 'name')) {
                $orderColumn = 'name';
            }
        } catch (Throwable) {
            $orderColumn = 'product_name';
        }

        try {
            if ($hasIdColumn) {
                $builder
                    ->orderBy('id')
                    ->chunkById($chunkSize, function ($rows) use ($processRow): void {
                        foreach ($rows as $row) {
                            $processRow($row);
                        }
                    });
            } else {
                $builder
                    ->orderBy($orderColumn)
                    ->chunk($chunkSize, function ($rows) use ($processRow): void {
                        foreach ($rows as $row) {
                            $processRow($row);
                        }
                    });
            }
        } catch (Throwable) {
            return null;
        }

        if ($grouped === []) {
            return collect();
        }

        return collect($grouped)
            ->map(function (array $entries): array {
                return collect($entries)
                    ->map(fn (array $row) => array_diff_key($row, ['normalized' => true]))
                    ->sortBy(function (array $row) {
                        return $row['price'] ?? INF;
                    })
                    ->values()
                    ->all();
            });
    }

    private function indexTheGamesDb(): Collection
    {
        try {
            $games = TheGamesDbGame::query()
                ->select(['external_id', 'title', 'slug', 'platform', 'image_url', 'thumb_url'])
                ->where(function ($query): void {
                    $query->whereNotNull('image_url')
                        ->orWhereNotNull('thumb_url');
                })
                ->limit(5000)
                ->get();
        } catch (Throwable) {
            return collect();
        }

        return $games
            ->mapWithKeys(function (TheGamesDbGame $game): array {
                $normalized = $this->normalizeName($game->title);
                if (! $normalized) {
                    return [];
                }

                $image = $game->image_url ?: $game->thumb_url;
                if (! is_string($image) || trim($image) === '') {
                    return [];
                }

                return [$normalized => [
                    'id' => $game->external_id,
                    'name' => $game->title,
                    'image' => $image,
                    'platform' => $game->platform,
                    'slug' => $game->slug,
                    'thumb' => $game->thumb_url,
                ]];
            });
    }

    private function resolvePath(?string $path): ?string
    {
        if (! $path) {
            return null;
        }

        if (str_starts_with($path, DIRECTORY_SEPARATOR) || preg_match('/^[A-Za-z]:\\\\/', $path) === 1) {
            return $path;
        }

        return base_path($path);
    }

    private function normalizeName(?string $name): ?string
    {
        return GameNameNormalizer::normalize($name);
    }

    private function resolveImage(array $row): ?string
    {
        $image = $row['image'] ?? null;
        if (is_array($image)) {
            foreach (['super_url', 'original_url', 'small_url'] as $key) {
                if (! empty($image[$key]) && is_string($image[$key])) {
                    return $image[$key];
                }
            }
        }

        $gallery = $row['images'] ?? null;
        if (is_array($gallery)) {
            foreach ($gallery as $candidate) {
                if (! is_array($candidate)) {
                    continue;
                }

                foreach (['super_url', 'original_url', 'small_url'] as $key) {
                    if (! empty($candidate[$key]) && is_string($candidate[$key])) {
                        return $candidate[$key];
                    }
                }
            }
        }

        return null;
    }

    private function normalizeNumber(mixed $value): ?float
    {
        if ($value === null || $value === '' || $value === 'unavailable') {
            return null;
        }

        if (is_string($value)) {
            $value = trim($value);
            if ($value === '') {
                return null;
            }

            $value = preg_replace('/[^0-9.\-]/', '', $value);
        }

        if ($value === '' || $value === null) {
            return null;
        }

        if (! is_numeric($value)) {
            return null;
        }

        return (float) $value;
    }

    private function associateRow(?array $headers, array $row): ?array
    {
        if (! is_array($headers)) {
            return null;
        }

        $headerCount = count($headers);
        if (count($row) < $headerCount) {
            $row = array_pad($row, $headerCount, null);
        }

        $assoc = [];
        foreach ($headers as $index => $column) {
            if (! is_string($column) || $column === '') {
                continue;
            }

            $assoc[$column] = $row[$index] ?? null;
        }

        return $assoc;
    }

    private function formatCurrency(float $amount, string $currency): string
    {
        $formatted = number_format($amount, 2, '.', ',');

        return match ($currency) {
            'USD' => '$'.$formatted,
            'EUR' => '€'.$formatted,
            'GBP' => '£'.$formatted,
            'AUD' => 'A$'.$formatted,
            'CAD' => 'C$'.$formatted,
            default => $currency.' '.$formatted,
        };
    }

    private function formatUsd(?float $amount, mixed $raw): ?string
    {
        if ($amount === null) {
            if (is_string($raw) && trim($raw) !== '') {
                return trim($raw);
            }

            return null;
        }

        $formatted = number_format($amount, 2, '.', ',');

        return '$'.$formatted;
    }

    private function extractPlatforms(array $row): array
    {
        $platforms = $row['platforms'] ?? null;

        if (! is_array($platforms)) {
            return [];
        }

        return collect($platforms)
            ->map(function ($platform) {
                if (is_array($platform)) {
                    return $platform['name'] ?? ($platform['abbreviation'] ?? null);
                }

                return is_string($platform) ? $platform : null;
            })
            ->filter(fn ($value) => is_string($value) && $value !== '')
            ->unique()
            ->values()
            ->all();
    }

    private function sanitizeNexardaSlug(?string $slug): ?string
    {
        if (! is_string($slug) || $slug === '') {
            return null;
        }

        $trimmed = ltrim($slug, '/');

        return $trimmed !== '' ? $trimmed : null;
    }

    public function syncNexarda(int $limit = 100, bool $all = false, string $currency = 'USD'): int
    {
        $client = app(NexardaClient::class);
        $query = VideoGameTitleSource::where('provider', 'nexarda')
            ->whereNotNull('provider_item_id');

        if (! $all) {
            $query->limit($limit);
        }

        $sources = $query->get();
        $processed = 0;

        foreach ($sources as $source) {
            try {
                $this->processNexardaSource($client, $source, $currency);
                $processed++;
                usleep(500000); // Rate limiting
            } catch (Throwable) {
                continue;
            }
        }

        return $processed;
    }

    public function backfillNexarda(int $limit = 100, string $currency = 'USD'): int
    {
        $client = app(NexardaClient::class);
        $titles = VideoGameTitle::whereDoesntHave('sources', function ($q) {
            $q->where('provider', 'nexarda');
        })
            ->limit($limit)
            ->get();

        $processed = 0;
        foreach ($titles as $title) {
            try {
                $results = $client->search($title->name);
                if (empty($results['results'])) {
                    continue;
                }

                $match = $results['results'][0];
                $source = VideoGameTitleSource::updateOrCreate(
                    [
                        'video_game_title_id' => $title->id,
                        'provider' => 'nexarda',
                    ],
                    [
                        'provider_item_id' => (string) $match['id'],
                        'name' => $match['name'],
                        'slug' => $match['slug'] ?? null,
                        'external_id' => (int) $match['id'],
                        'updated_at' => now(),
                    ]
                );

                $this->processNexardaSource($client, $source, $currency);
                $processed++;
                usleep(500000);
            } catch (Throwable) {
                continue;
            }
        }

        return $processed;
    }

    public function ingestNexardaCatalogue(?string $path = null): int
    {
        $path = $path ?? base_path('nexarda_product_catalogue.json');
        if (! file_exists($path)) {
            return 0;
        }

        $data = json_decode(file_get_contents($path), true);
        $games = $data['games'] ?? [];
        $processed = 0;
        $now = now();
        $retailerCache = [];

        foreach (array_chunk($games, 200) as $chunk) {
            DB::transaction(function () use ($chunk, $now, &$processed, &$retailerCache) {
                foreach ($chunk as $row) {
                    $name = $row['name'];
                    $slug = Str::slug($name);
                    if ($slug === '') {
                        $slug = 'nexarda-'.$row['id'];
                    }

                    $product = Product::firstOrCreate(
                        ['name' => $name],
                        ['slug' => $slug, 'type' => 'video_game']
                    );

                    $title = VideoGameTitle::firstOrCreate(
                        ['product_id' => $product->id, 'slug' => $slug],
                        ['name' => $name]
                    );

                    $source = VideoGameTitleSource::updateOrCreate(
                        [
                            'video_game_title_id' => $title->id,
                            'provider' => 'nexarda',
                        ],
                        [
                            'provider_item_id' => (string) $row['id'],
                            'name' => $name,
                            'slug' => $row['slug'] ?? null,
                            'external_id' => (int) $row['id'],
                            'raw_payload' => json_encode($row),
                            'updated_at' => $now,
                        ]
                    );

                    $videoGame = VideoGame::updateOrCreate(
                        [
                            'video_game_title_id' => $title->id,
                            'provider' => 'nexarda',
                            'external_id' => (int) $row['id'],
                        ],
                        [
                            'name' => $name,
                            'slug' => $slug,
                            'attributes' => json_encode([
                                'platform' => [],
                                'nexarda_slug' => $row['slug'] ?? null,
                            ]),
                            'updated_at' => $now,
                        ]
                    );

                    $priceRows = [];
                    foreach ($row['prices'] as $cur => $val) {
                        if ($val === 'unavailable') {
                            continue;
                        }

                        $code = strtoupper($cur);
                        $retailerSlug = 'nexarda_'.strtolower($code);

                        if (! isset($retailerCache[$retailerSlug])) {
                            $retailerCache[$retailerSlug] = Retailer::firstOrCreate(
                                ['slug' => $retailerSlug],
                                ['name' => "Nexarda {$code} (Catalogue)"]
                            );
                        }

                        $priceRows[] = [
                            'video_game_id' => $videoGame->id,
                            'currency' => $code,
                            'country_code' => $this->getCountryForCurrency($code),
                            'amount_minor' => (int) round(((float) $val) * 100),
                            'retailer' => $retailerCache[$retailerSlug]->name,
                            'recorded_at' => $now,
                            'is_active' => true,
                            'metadata' => json_encode([
                                'src' => 'nexarda_catalogue',
                                'discount_percent' => $row['discounts'][$cur] ?? 0,
                            ]),
                            'updated_at' => $now,
                        ];
                    }

                    if (! empty($priceRows)) {
                        VideoGamePrice::upsert(
                            $priceRows,
                            ['video_game_id', 'retailer', 'country_code'],
                            ['currency', 'amount_minor', 'recorded_at', 'is_active', 'metadata', 'updated_at']
                        );
                    }
                    $processed++;
                }
            });
        }

        return $processed;
    }

    private function processNexardaSource(NexardaClient $client, VideoGameTitleSource $source, string $currency): void
    {
        $data = $client->getPrices($source->provider_item_id, $currency);
        if (empty($data['prices']['list'])) {
            return;
        }

        $source->update([
            'raw_payload' => json_encode($data),
            'updated_at' => now(),
        ]);

        $videoGame = VideoGame::where('video_game_title_id', $source->video_game_title_id)
            ->where('provider', 'nexarda')
            ->first() ?? VideoGame::where('video_game_title_id', $source->video_game_title_id)->first();

        if (! $videoGame) {
            return;
        }

        $now = now();
        $priceRows = [];

        foreach ($data['prices']['list'] as $offer) {
            $storeName = $offer['store']['name'] ?? 'Unknown Store';
            $retailerSlug = 'nexarda_'.Str::slug($storeName);

            $retailer = Retailer::firstOrCreate(
                ['slug' => $retailerSlug],
                ['name' => $storeName.' (via Nexarda)']
            );

            $amount = (int) round(($offer['price'] ?? 0) * 100);
            if ($amount <= 0) {
                continue;
            }

            $priceRows[] = [
                'video_game_id' => $videoGame->id,
                'currency' => strtoupper($currency),
                'country_code' => 'US',
                'amount_minor' => $amount,
                'retailer' => $retailer->name,
                'url' => $offer['url'] ?? $source->provider_url,
                'recorded_at' => $now,
                'is_active' => true,
                'metadata' => json_encode([
                    'src' => 'nexarda_live',
                    'store' => $storeName,
                    'is_sale' => ($offer['price'] < ($data['prices']['highest'] ?? 0)),
                ]),
                'updated_at' => $now,
            ];
        }

        if (! empty($priceRows)) {
            VideoGamePrice::upsert(
                $priceRows,
                ['video_game_id', 'retailer', 'country_code'],
                ['currency', 'amount_minor', 'recorded_at', 'is_active', 'metadata', 'updated_at']
            );
        }
    }

    private function getCountryForCurrency(string $currency): string
    {
        return match ($currency) {
            'USD' => 'US',
            'EUR' => 'EU',
            'GBP' => 'GB',
            default => 'US',
        };
    }

    private function buildNexardaUrl(?string $slug): ?string
    {
        if (! is_string($slug) || $slug === '') {
            return null;
        }

        if (str_starts_with($slug, 'http://') || str_starts_with($slug, 'https://')) {
            return $slug;
        }

        return 'https://www.nexarda.com/'.ltrim($slug, '/');
    }
}
