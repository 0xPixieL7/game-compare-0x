<?php

declare(strict_types=1);

namespace App\Services\Providers;

use App\Models\Product;
use App\Models\VideoGame;
use App\Models\VideoGamePrice;
use App\Models\VideoGameSource;
use App\Models\VideoGameTitle;
use App\Models\VideoGameTitleSource;
use App\Services\Normalization\PlatformNormalizer;
use GuzzleHttp\Client as HTTPClient;
use GuzzleHttp\Exception\ConnectException;
use GuzzleHttp\Exception\RequestException;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\Middleware;
use GuzzleHttp\Promise\Utils;
use Illuminate\Support\Arr;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Log;
use Illuminate\Support\Str;
use PlaystationStoreApi\Client;
use PlaystationStoreApi\Enum\CategoryEnum;
use PlaystationStoreApi\Enum\RegionEnum;
use PlaystationStoreApi\Request\RequestPricingDataByConceptId;
use PlaystationStoreApi\Request\RequestProductList;
use Psr\Http\Message\RequestInterface;
use Psr\Http\Message\ResponseInterface;

class PlayStationStoreProvider
{
    private const PROVIDER_KEY = 'playstation_store';

    private const PROVIDER_NAME = 'PlayStation Store';

    private const API_URL = 'https://web.np.playstation.com/api/graphql/v1/';

    private VideoGameSource $providerSource;

    private PlatformNormalizer $platformNormalizer;

    private HTTPClient $guzzle;

    private \PlaystationStoreApi\RequestLocatorService $requestServiceLocator;

    /**
     * @param  array<string>  $regions  List of regions to fetch pricing for (e.g., ['en-us', 'en-gb', 'ja-jp'])
     */
    public function __construct(
        private array $regions = ['en-us'],
        private readonly int $timeout = 15,
        ?PlatformNormalizer $platformNormalizer = null,

    ) {
        // Deduplicate regions by their enum value to avoid redundant requests
        $seenEnums = [];
        $this->regions = array_filter($this->regions, function ($r) use (&$seenEnums) {
            $enum = $this->resolveRegionEnum($r)->value;
            if (in_array($enum, $seenEnums)) {
                return false;
            }
            $seenEnums[] = $enum;

            return true;
        });

        // Setup Guzzle Retry Middleware
        $stack = HandlerStack::create();
        $stack->push(Middleware::retry(function (
            $retries,
            RequestInterface $request,
            ?ResponseInterface $response = null,
            $exception = null
        ) {
            // Limit retries
            if ($retries >= 3) {
                return false;
            }

            // Retry on connection exceptions (network errors)
            if ($exception instanceof ConnectException || $exception instanceof RequestException) {
                return true;
            }

            // Retry on server errors (5xx) or Rate Limits (429)
            if ($response) {
                if ($response->getStatusCode() >= 500 || $response->getStatusCode() === 429) {
                    return true;
                }
            }

            return false;
        }, function ($retries) {
            // Exponential backoff: 1s, 2s, 4s...
            return 1000 * pow(2, $retries);
        }));

        $this->guzzle = new HTTPClient([
            'base_uri' => self::API_URL,
            'timeout' => $this->timeout,
            'handler' => $stack,
        ]);
        $this->requestServiceLocator = \PlaystationStoreApi\RequestLocatorService::default();

        // Ensure provider source exists in database (with retry for timeouts/race conditions)
        $this->providerSource = retry(3, function () {
            try {
                return VideoGameSource::query()->firstOrCreate(
                    ['provider_key' => self::PROVIDER_KEY],
                    [
                        'provider' => self::PROVIDER_KEY,
                        'display_name' => self::PROVIDER_NAME,
                        'category' => 'store',
                        'slug' => Str::slug(self::PROVIDER_NAME),
                        'active' => true,
                    ]
                );
            } catch (\Illuminate\Database\UniqueConstraintViolationException $e) {
                return VideoGameSource::query()->where('provider_key', self::PROVIDER_KEY)->firstOrFail();
            }
        }, 100);

        $this->platformNormalizer = $platformNormalizer ?? new PlatformNormalizer;
    }

    /**
     * Ingest products with multi-region pricing (avoids duplicates).
     */
    public function ingestProducts(array $options = []): array
    {
        $category = Arr::get($options, 'category', CategoryEnum::PS5_GAMES);
        $maxPages = (int) Arr::get($options, 'max_pages', 1);

        // Step 1: Fetch catalog from PRIMARY region to discover games
        $primaryRegion = $this->regions[0] ?? 'en-us';
        $conceptIds = $this->fetchCatalogConceptIds($primaryRegion, $category, $maxPages);

        $created = 0;
        $updated = 0;
        $skipped = 0;
        $errors = [];
        $priceRecordsCreated = 0;

        // Step 2: For each game, fetch pricing from ALL regions
        foreach ($conceptIds as $conceptId) {
            try {
                $result = $this->ingestConceptWithMultiRegionPricing($conceptId);

                if ($result['created']) {
                    $created++;
                } elseif ($result['updated']) {
                    $updated++;
                } else {
                    $skipped++;
                }

                $priceRecordsCreated += $result['price_records_created'];

            } catch (\Throwable $e) {
                $errors[] = [
                    'concept_id' => $conceptId,
                    'error' => $e->getMessage(),
                ];
                $skipped++;
            }
        }

        return [
            'success' => true,
            'stats' => [
                'created' => $created,
                'updated' => $updated,
                'skipped' => $skipped,
                'errors' => \count($errors),
                'price_records_created' => $priceRecordsCreated,
                'regions_queried' => \count($this->regions),
            ],
            'errors' => $errors,
            'meta' => [
                'provider' => self::PROVIDER_KEY,
                'provider_name' => self::PROVIDER_NAME,
                'regions' => $this->regions,
                'category' => $category->value ?? 'unknown',
            ],
        ];
    }

    /**
     * Fetch concept IDs from catalog (catalog discovery from single region).
     *
     * @return array<string>
     */
    public function fetchCatalogConceptIds(string $region, CategoryEnum $category, int $maxPages, int $stopYear = 2015): array
    {
        $conceptIds = [];

        try {
            $client = $this->createClient($region);
            $request = RequestProductList::createFromCategory($category);

            for ($page = 0; $page < $maxPages; $page++) {
                $response = $client->get($request);
                $grid = $response['data']['categoryGridRetrieve'] ?? [];
                $concepts = $grid['concepts'] ?? [];

                if (empty($concepts)) {
                    break;
                }

                // Log progress
                if ($page % 50 === 0) {
                    Log::info("PlayStation Discovery: Scanned {$page} pages...");
                }

                foreach ($concepts as $concept) {
                    // Check date filter (assuming descending sort)
                    if (isset($concept['releaseDate'])) {
                        // releaseDate format example: "2023-11-10T00:00:00Z"
                        $year = (int) substr($concept['releaseDate'], 0, 4);
                        if ($year < $stopYear) {
                            Log::info("PlayStation Discovery: Reached stop year {$stopYear} at page {$page}. Stopping.");

                            // Reached games older than retention period
                            return $conceptIds;
                        }
                    }

                    if ($id = $concept['id'] ?? null) {
                        $conceptIds[] = $id;
                    }
                }

                // Log progress with meaningful time context
                if ($page % 50 === 0 && isset($concepts[0]['releaseDate'])) {
                    $latestDate = $concepts[0]['releaseDate'];
                    $oldestDateOnPage = end($concepts)['releaseDate'] ?? $latestDate;

                    Log::info("PlayStation Discovery: Scanned {$page} pages. Current Date Range: {$latestDate} -> {$oldestDateOnPage}");
                }

                // Check for next page
                if ($page >= $maxPages - 1) {
                    break;
                }

                $request = $request->createNextPageRequest();
            }

            Log::info('PlayStation Discovery: Finished with '.count($conceptIds).' concepts.');

        } catch (\Throwable $e) {
            Log::error('PlayStation Store catalog fetch failed', [
                'region' => $region,
                'error' => $e->getMessage(),
            ]);
        }

        return $conceptIds;
    }

    /**
     * Ingest a single concept with pricing from ALL configured regions.
     */
    public function ingestConceptWithMultiRegionPricing(string $conceptId): array
    {
        // Try to fetch game metadata from regions in order until one succeeds
        $gameData = null;
        $metadataRegion = null;

        foreach ($this->regions as $region) {
            $gameData = $this->fetchConceptMetadata($conceptId, $region);
            if ($gameData) {
                $metadataRegion = $region;
                break;
            }
        }

        if (! $gameData) {
            return ['created' => false, 'updated' => false, 'price_records_created' => 0];
        }

        $title = $gameData['title'];
        $invariantTitle = $gameData['invariant_title'] ?? null;
        $normalizedTitle = $this->normalizeTitle($title);

        // 1. Find or create Product (robust resolution)
        $product = $this->resolveProduct($title, $invariantTitle);

        if (! $product) {
            // Fallback: Create new product if still not found
            $product = retry(3, function () use ($title, $normalizedTitle) {
                try {
                    return Product::query()->firstOrCreate(
                        ['name' => $title],
                        [
                            'type' => 'video_game',
                            'slug' => Str::slug($title),
                            'title' => $title,
                            'normalized_title' => $normalizedTitle,
                        ]
                    );
                } catch (\Illuminate\Database\UniqueConstraintViolationException $e) {
                    return Product::query()->where('name', $title)
                        ->orWhere('slug', Str::slug($title))
                        ->first();
                }
            }, 100);
        }

        $wasProductCreated = $product->wasRecentlyCreated;

        // 2. Find or create VideoGameTitle
        $videoGameTitle = VideoGameTitle::query()->firstOrCreate(
            [
                'product_id' => $product->id,
                'normalized_title' => $normalizedTitle,
            ],
            [
                'name' => $title,
                'slug' => $product->slug.'-'.$product->id,
            ]
        );

        $wasTitleCreated = $videoGameTitle->wasRecentlyCreated;

        // 3. Create VideoGameTitleSource mapping
        $conceptNumericId = $this->conceptNumericId($conceptId);

        VideoGameTitleSource::query()->updateOrCreate(
            [
                'video_game_title_id' => $videoGameTitle->id,
                'video_game_source_id' => $this->providerSource->id,
                'provider' => self::PROVIDER_KEY,
                // Schema requires bigint; store numeric id as provider_item_id
                'provider_item_id' => $conceptNumericId,
            ],
            [
                'external_id' => $conceptNumericId,
                'slug' => Str::slug($title),
                'name' => $title,
                'raw_payload' => array_merge($gameData['metadata']['raw_payload'] ?? [], [
                    'concept_id' => $conceptId,
                    'metadata_region' => $metadataRegion,
                ]),
            ]
        );

        // 4. Create or update VideoGame (with media data)
        $platforms = $this->platformNormalizer->normalizeMany($gameData['platform'] ?? []);
        $genres = $this->normalizeGenres($gameData['metadata']['genres'] ?? []);
        $media = $gameData['media'] ?? [];
        $ratingPercent = $gameData['metadata']['rating_percent'] ?? null;
        $ratingCount = $gameData['metadata']['rating_count'] ?? null;

        $videoGame = VideoGame::query()->updateOrCreate(
            [
                'provider' => self::PROVIDER_KEY,
                'external_id' => $conceptNumericId,
            ],
            [
                'slug' => $videoGameTitle->slug,
                'name' => $title,
                'provider' => self::PROVIDER_KEY,
                'external_id' => $conceptNumericId,
                'description' => $gameData['metadata']['description'] ?? null,
                'summary' => $gameData['metadata']['summary'] ?? null,
                'url' => $gameData['metadata']['url'] ?? null,
                'platform' => $platforms === [] ? null : $platforms,
                'genre' => $genres === [] ? null : $genres,
                'developer' => $gameData['metadata']['developer'] ?? null,
                'publisher' => $gameData['metadata']['publisher'] ?? null,
                'release_date' => $gameData['metadata']['release_date'] ?? null,
                'rating' => $ratingPercent,
                'rating_count' => $ratingCount,
                'media' => empty($media) ? null : $media,
                'source_payload' => $gameData['metadata']['raw_payload'] ?? null,
                'attributes' => $gameData['metadata']['attributes'] ?? null,
                'video_game_title_id' => $videoGameTitle->id,
            ]
        );

        $wasGameCreated = $videoGame->wasRecentlyCreated;

        $this->providerSource->recordVideoGameId($videoGame->id);

        // 5. Fetch pricing from ALL regions
        // First, check metadata region price if it was extracted
        $primaryPrice = $gameData['price'] ?? null;

        // Identify regions we still need to fetch (using async)
        $remainingRegions = array_filter(
            $this->regions,
            fn ($r) => $r !== $metadataRegion
        );

        // Fetch remaining regions in parallel (BATCHED/ASYNC)
        $asyncPrices = $this->fetchPricingForRegionsAsync($conceptId, $remainingRegions);

        // Combine
        $allPrices = [];
        if ($primaryPrice) {
            $allPrices[$metadataRegion] = $primaryPrice;
        }
        foreach ($asyncPrices as $region => $price) {
            if ($price) {
                $allPrices[$region] = $price;
            }
        }

        $priceRecordsCreated = 0;

        // Debug logging
        if (empty($allPrices)) {
            Log::info("PS Debug: No prices returned for {$conceptId}");
        } else {
            Log::info('PS Debug: Processing '.count($allPrices)." prices for {$conceptId}");
        }

        foreach ($allPrices as $region => $priceData) {
            $currency = $priceData['currency'] ?? null;
            $amountMinor = $priceData['amount_minor'] ?? null;

            if ($currency === null || $amountMinor === null) {
                continue;
            }

            $countryCode = strtoupper(Str::afterLast($region, '-'));
            $regionCode = Str::before($region, '-');
            $priceRegionUrl = "https://store.playstation.com/{$region}/concept/{$conceptId}";

            VideoGamePrice::query()->updateOrCreate(
                [
                    'video_game_id' => $videoGame->id,
                    'retailer' => self::PROVIDER_NAME.' '.strtoupper($region),
                    'country_code' => $countryCode,
                ],
                [
                    'product_id' => $videoGame->title?->product_id,
                    'currency' => $currency,
                    'amount_minor' => $amountMinor,
                    'recorded_at' => now(),
                    'tax_inclusive' => $priceData['is_discounted'] ?? false,
                    'region_code' => $regionCode,
                    'url' => $priceRegionUrl,
                    'is_active' => true,
                    'bucket' => 'snapshot',
                    'condition' => 'digital',
                ]
            );

            $priceRecordsCreated++;
        }

        return [
            'created' => $wasProductCreated || $wasTitleCreated || $wasGameCreated,
            'updated' => ! ($wasProductCreated && $wasTitleCreated && $wasGameCreated),
            'price_records_created' => $priceRecordsCreated,
        ];
    }

    /**
     * Fetch pricing for multiple regions concurrently.
     */
    public function fetchPricingForRegionsAsync(string $conceptId, array $regions): array
    {
        if (empty($regions)) {
            return [];
        }

        $promises = [];
        foreach ($regions as $region) {
            try {
                // We manually build the request properties to use Guzzle's requestAsync
                // while still using the library's metadata hashes.
                $info = $this->requestServiceLocator->get(RequestPricingDataByConceptId::class);
                $variables = json_encode(new RequestPricingDataByConceptId($conceptId), JSON_THROW_ON_ERROR);
                $extensions = json_encode([
                    'persistedQuery' => [
                        'version' => 1,
                        'sha256Hash' => $info->value,
                    ],
                ], JSON_THROW_ON_ERROR);

                $query = http_build_query([
                    'operationName' => $info->name,
                    'variables' => $variables,
                    'extensions' => $extensions,
                ]);

                $promises[$region] = $this->guzzle->requestAsync('GET', 'op?'.$query, [
                    'headers' => [
                        'x-psn-store-locale-override' => $this->resolveRegionEnum($region)->value,
                        'content-type' => 'application/json',
                    ],
                ]);
            } catch (\Throwable $e) {
                Log::warning("Failed to create promise for region {$region}: ".$e->getMessage());
            }
        }

        // Wait for all requests to finish or fail
        $results = Utils::settle($promises)->wait();

        $prices = [];
        foreach ($results as $region => $result) {
            if ($result['state'] === 'fulfilled') {
                try {
                    $response = $result['value'];
                    $body = (string) $response->getBody();
                    $data = json_decode($body, true, 512, JSON_THROW_ON_ERROR);

                    $conceptData = $data['data']['conceptRetrieve'] ?? null;
                    $defaultProduct = $conceptData['defaultProduct'] ?? null;
                    $priceData = $defaultProduct ? $this->extractPriceFromProduct($defaultProduct) : null;

                    if ($priceData) {
                        $prices[$region] = $priceData;
                    }
                } catch (\Throwable $e) {
                    Log::warning("Failed to parse PS pricing for region {$region}: ".$e->getMessage());
                }
            } else {
                // Log::info("PS Async Request failed for region {$region}: " . ($result['reason']->getMessage() ?? 'Unknown Error'));
            }
        }

        return $prices;
    }

    /**
     * Fetch game metadata (title, platform, genres, etc.) from a concept.
     */
    public function fetchConceptMetadata(string $conceptId, string $region): ?array
    {
        try {
            $client = $this->createClient($region);
            $response = $client->get(new RequestPricingDataByConceptId($conceptId));
            $conceptData = $response['data']['conceptRetrieve'] ?? null;

            if (! $conceptData) {
                return null;
            }

            $defaultProduct = $conceptData['defaultProduct'] ?? $conceptData['selectableProducts']['purchasableProducts'][0] ?? null;
            if (! $defaultProduct) {
                return null;
            }

            $productId = $defaultProduct['id'] ?? null;
            // Try resolving name from multiple places
            $name = $conceptData['name'] ?? $defaultProduct['name'] ?? null;
            $invariantName = $defaultProduct['invariantName'] ?? $conceptData['name'] ?? null;

            // Ensure we have at least one name
            if (! $name && ! $invariantName) {
                return null;
            }

            if (! $name) {
                $name = $invariantName;
            }

            if (! $productId) {
                return null;
            }

            $platforms = $this->resolvePlatforms($conceptData, $defaultProduct);

            // Extract and normalize media data (combine concept and product media)
            $mediaRaw = array_merge($conceptData['media'] ?? [], $defaultProduct['media'] ?? []);
            $media = $this->extractMediaData($mediaRaw, $region);

            $rawPayload = $conceptData;
            $releaseDate = $defaultProduct['releaseDate'] ?? null;
            $publisher = $defaultProduct['publisherName'] ?? null;
            $developer = $defaultProduct['developerName'] ?? null;
            $starRating = Arr::get($defaultProduct, 'starRating', []);
            $score = Arr::get($starRating, 'score');
            $ratingPercent = is_numeric($score) ? ($score / 5) * 100 : null;
            $ratingCount = Arr::get($starRating, 'count');

            // Extract Price (Optimization)
            $price = $this->extractPriceFromProduct($defaultProduct);

            // Extract Attributes
            $attributes = [
                'content_rating' => $conceptData['contentRating'] ?? null,
                'voice_language' => $conceptData['voiceLanguage'] ?? [],
                'subtitles' => $conceptData['subtitles'] ?? [],
                'legal_text' => Arr::get($defaultProduct, 'legalText'),
            ];

            // Build Store URL
            $storeUrl = "https://store.playstation.com/{$region}/concept/{$conceptId}";

            return [
                'title' => $name,
                'invariant_title' => $invariantName,
                'platform' => $platforms,
                'media' => $media,
                'price' => $price, // Included for optimization
                'metadata' => [
                    'concept_id' => $conceptId,
                    'product_id' => $productId,
                    'raw_media' => $conceptData['media'] ?? [],
                    'genres' => $conceptData['genres'] ?? [],
                    'release_date' => $releaseDate ? date('Y-m-d', strtotime($releaseDate)) : null,
                    'publisher' => $publisher,
                    'developer' => $developer,
                    'rating_percent' => $ratingPercent,
                    'rating_count' => is_numeric($ratingCount) ? (int) $ratingCount : null,
                    'description' => Arr::get($defaultProduct, 'longDescription'),
                    'summary' => Arr::get($defaultProduct, 'shortDescription'),
                    'raw_payload' => $rawPayload,
                    'url' => $storeUrl,
                    'attributes' => $attributes,
                ],
            ];

        } catch (\Throwable $e) {
            Log::warning('PlayStation Store concept metadata fetch failed', [
                'concept_id' => $conceptId,
                'region' => $region,
                'error' => $e->getMessage(),
            ]);

            return null;
        }
    }

    /**
     * Resolve product by title, invariant title, or alternative names.
     */
    private function resolveProduct(string $title, ?string $invariantTitle = null): ?Product
    {
        $searchTitles = collect([$title, $invariantTitle])
            ->filter()
            ->unique()
            ->all();

        foreach ($searchTitles as $searchTitle) {
            // 1. Direct name match on Product
            if ($product = Product::where('name', $searchTitle)->first()) {
                return $product;
            }

            // 2. Direct name match on VideoGameTitle
            if ($vgTitle = VideoGameTitle::where('name', $searchTitle)->first()) {
                if ($product = $vgTitle->product) {
                    return $product;
                }
            }

            // 3. Alternative name match (IGDB source)
            $altNameRecord = DB::table('video_game_alternative_names')
                ->where('name', $searchTitle)
                ->first();

            if ($altNameRecord) {
                $vg = VideoGame::find($altNameRecord->video_game_id);
                if ($vg && $vg->videoGameTitle && $vg->videoGameTitle->product) {
                    return $vg->videoGameTitle->product;
                }
            }
        }

        return null;
    }

    /**
     * Helper to extract price from product data.
     */
    private function extractPriceFromProduct(array $product): ?array
    {
        $price = $product['price'] ?? null;
        if (! $price) {
            return null;
        }

        $currencyCode = $price['currencyCode'] ?? null;
        $discountedValue = $price['discountedValue'] ?? null;
        $basePriceValue = $price['basePriceValue'] ?? null;

        if (! $currencyCode || ($discountedValue === null && $basePriceValue === null)) {
            return null;
        }

        return [
            'currency' => $currencyCode,
            'amount_minor' => (int) ($discountedValue ?? $basePriceValue),
            'base_price' => (int) ($basePriceValue ?? 0),
            'is_discounted' => $discountedValue !== null && $discountedValue < $basePriceValue,
        ];
    }

    /**
     * Extract and normalize media data from PlayStation Store API response.
     *
     * @param  array<mixed>  $mediaData
     * @return array{images: array<array{url: string, type: string, role: string}>, videos: array<array{url: string, type: string, role: string}>}
     */
    public function extractMediaData(array $mediaData, string $region = 'en-us'): array
    {
        $images = [];
        $videos = [];

        foreach ($mediaData as $item) {
            $typename = $item['__typename'] ?? null;
            $type = $item['type'] ?? null;
            $role = $item['role'] ?? 'unknown';
            $url = $item['url'] ?? null;

            if (! $url) {
                continue;
            }

            // Generate deterministic ID from URL
            $mediaId = abs(crc32($url));

            $normalized = [
                'id' => $mediaId,
                'url' => $url,
                'type' => $type ?? 'unknown',
                'role' => $role,
            ];

            // Categorize by __typename (Media can be ImageMedia or VideoMedia or just Media)
            $isImage = ($typename === 'ImageMedia' || $typename === 'Media') && strtolower($type) === 'image';
            $isVideo = ($typename === 'VideoMedia' || $typename === 'Media') && strtolower($type) === 'video';

            if ($isImage) {
                $images[] = $normalized;
            } elseif ($isVideo) {
                $videos[] = $normalized;
            }
        }

        return [
            'images' => $images,
            'videos' => $videos,
        ];
    }

    /**
     * Create a new PlayStation Store API client for a specific region.
     */
    private function createClient(string $region): Client
    {
        // Use the shared Guzzle client for connection pooling if possible,
        // but Client wrapper expects ClientInterface, which our shared one is.
        // Creating new Client wrapper is cheap.
        return new Client(
            $this->resolveRegionEnum($region),
            $this->guzzle // Reuse the shared Guzzle client
        );
    }

    /**
     * Extract platform from PlayStation product ID.
     */
    private function resolvePlatforms(array $conceptData, array $productData): array
    {
        $platforms = collect($conceptData['platforms'] ?? $productData['platforms'] ?? [])
            ->map(fn ($platform) => is_array($platform) ? Arr::get($platform, 'name') : $platform)
            ->filter()
            ->values()
            ->all();

        if ($platforms === []) {
            $platforms = [$this->guessPlatformFromProductId($productData['id'] ?? '')];
        }

        return $platforms;
    }

    private function guessPlatformFromProductId(string $productId): string
    {
        if (str_contains($productId, 'PPSA')) {
            return 'PlayStation 5';
        }

        if (str_contains($productId, 'CUSA')) {
            return 'PlayStation 4';
        }

        return 'PlayStation';
    }

    private function normalizeGenres(array $genres): array
    {
        return collect($genres)
            ->map(fn ($genre) => is_array($genre) ? Arr::get($genre, 'name') : $genre)
            ->filter()
            ->unique()
            ->values()
            ->all();
    }

    private function conceptNumericId(string $conceptId): int
    {
        if (is_numeric($conceptId)) {
            return (int) $conceptId;
        }

        return abs(crc32($conceptId));
    }

    /**
     * Normalize title for matching (simple implementation).
     */
    private function normalizeTitle(string $title): string
    {
        return Str::lower(
            preg_replace('/[^a-z0-9]+/', '', Str::ascii($title)) ?? ''
        );
    }

    /**
     * Resolve region string to RegionEnum.
     */
    private function resolveRegionEnum(string $region): RegionEnum
    {
        $regionMap = [
            'en-us' => RegionEnum::UNITED_STATES,
            'us' => RegionEnum::UNITED_STATES,
            'en-gb' => RegionEnum::UNITED_KINGDOM,
            'uk' => RegionEnum::UNITED_KINGDOM,
            'gb' => RegionEnum::UNITED_KINGDOM,
            'ja-jp' => RegionEnum::JAPAN,
            'jp-jp' => RegionEnum::JAPAN,
            'jp' => RegionEnum::JAPAN,
        ];

        return $regionMap[strtolower($region)]
            ?? RegionEnum::tryFrom($region)
            ?? RegionEnum::UNITED_STATES;
    }
}
