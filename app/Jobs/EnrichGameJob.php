<?php

namespace App\Jobs;

use App\Console\Commands\Enrich\Concerns\InteractsWithEnrichmentProviders;
use App\Services\Media\TGDB\TGDBService;
use App\Services\Price\Amazon\AmazonScraperService;
use App\Services\Price\GameDataAggregatorService;
use App\Services\Price\GiantBomb\GiantBombService;
use App\Services\Price\ItchIo\ItchIoScraperService;
use App\Services\Price\PlayStation\PlayStationStoreService;
use App\Services\Price\Steam\SteamStoreService;
use App\Services\Price\Xbox\XboxStoreService;
use Illuminate\Bus\Queueable;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Bus\Dispatchable;
use Illuminate\Queue\InteractsWithQueue;
use Illuminate\Queue\SerializesModels;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Log;

class EnrichGameJob implements ShouldQueue
{
    use Dispatchable, InteractsWithQueue, Queueable, SerializesModels;
    use InteractsWithEnrichmentProviders;

    public int $gameId;

    /** @var array<int, string> */
    public array $providers = [];

    public bool $worldwide = false;

    public bool $pricesOnly = false;

    public function __construct(
        int $gameId,
        array $providers,
        bool $worldwide = false,
        bool $pricesOnly = false
    ) {
        $this->gameId = $gameId;
        $this->providers = $providers;
        $this->worldwide = $worldwide;
        $this->pricesOnly = $pricesOnly;
    }

    public function handle(
        SteamStoreService $steam,
        XboxStoreService $xbox,
        PlayStationStoreService $playstation,
        AmazonScraperService $amazon,
        GameDataAggregatorService $aggregator,
        GiantBombService $giantBomb,
        TGDBService $tgdb,
        ItchIoScraperService $itchIo
    ): void {
        // Backwards-compatible defaults for older serialized jobs.
        $providers = isset($this->providers) ? $this->providers : [];
        $worldwide = isset($this->worldwide) ? $this->worldwide : false;
        $pricesOnly = isset($this->pricesOnly) ? $this->pricesOnly : false;

        $game = DB::table('video_games')->where('id', $this->gameId)->first();

        if (! $game) {
            return;
        }

        try {
            // Resolve provider IDs (attributes + external_links) and skip providers without IDs.
            $attributes = match (true) {
                is_array($game->attributes) => $game->attributes,
                is_string($game->attributes) => json_decode($game->attributes, true) ?? [],
                default => [],
            };

            $originalAttributes = $attributes;
            $attributes = $this->resolveIdsFromExternalLinks($game, $attributes);
            $attributes = $this->resolveIdsFromExternalLinksTable((int) $game->id, $attributes);

            // Persist updated attributes only if changed.
            if ($attributes !== $originalAttributes) {
                DB::table('video_games')->where('id', $game->id)->update([
                    'attributes' => json_encode($attributes),
                ]);
            }

            foreach ($providers as $provider) {
                if (! self::providerIsAvailable($provider, $attributes)) {
                    continue;
                }

                if ($provider === 'epic' && ! self::isEpicBearerTokenUsable()) {
                    continue;
                }

                match ($provider) {
                    'steam' => $this->enrichWithSteam($steam, $aggregator, $game, $worldwide, true, $pricesOnly),
                    'xbox' => $this->enrichWithXbox($xbox, $aggregator, $game, $worldwide, true, $pricesOnly),
                    'amazon' => $this->enrichWithAmazon($aggregator, $game, $worldwide),
                    'gog' => $this->enrichWithGog($aggregator, $game, $worldwide),
                    'epic' => $this->enrichWithEpic($aggregator, $game, $worldwide),
                    'giantbomb' => $this->enrichWithGiantBomb($giantBomb, $aggregator, $game),
                    'tgdb' => $this->enrichWithTGDB($tgdb, $aggregator, $game),
                    'itchio' => $this->enrichWithItchIo($itchIo, $aggregator, $game, $worldwide, $pricesOnly),
                    default => null,
                };
            }
        } catch (\Throwable $e) {
            Log::error("EnrichGameJob failed for game {$this->gameId}", [
                'error' => $e->getMessage(),
                'trace' => $e->getTraceAsString(),
            ]);
            // Re-throw to trigger job failure handling
            throw $e;
        }
    }

    private static function isEpicBearerTokenUsable(): bool
    {
        static $cached = null;

        if ($cached !== null) {
            return $cached;
        }

        $token = (string) config('services.epic.bearer_token');
        if ($token === '') {
            return $cached = false;
        }

        $token = str_replace('eg1~', '', $token);
        $parts = explode('.', $token);
        if (count($parts) < 2) {
            return $cached = false;
        }

        $payloadJson = base64_decode(strtr($parts[1], '-_', '+/'));
        if (! is_string($payloadJson) || $payloadJson === '') {
            return $cached = false;
        }

        $payload = json_decode($payloadJson, true);
        if (! is_array($payload) || ! isset($payload['exp'])) {
            return $cached = false;
        }

        $exp = (int) $payload['exp'];

        // Require at least 60 seconds of validity.
        return $cached = ($exp - time()) > 60;
    }

    /** @param array<string, mixed> $attributes */
    private function resolveIdsFromExternalLinksTable(int $gameId, array $attributes): array
    {
        $links = DB::table('video_game_external_links')
            ->select(['category', 'external_id', 'url'])
            ->where('video_game_id', $gameId)
            ->whereIn('category', [0, 1, 3, 31, 36, 26])
            ->get();

        foreach ($links as $link) {
            $url = is_string($link->url ?? null) ? (string) $link->url : '';
            $externalId = is_string($link->external_id ?? null) ? (string) $link->external_id : '';

            // Steam
            if (empty($attributes['steam_id'])) {
                if ($url !== '' && preg_match('/store\.steampowered\.com\/app\/(\d+)/', $url, $m) === 1) {
                    $attributes['steam_id'] = (int) $m[1];
                } elseif ($externalId !== '' && preg_match('/^\d+$/', $externalId) === 1) {
                    $attributes['steam_id'] = (int) $externalId;
                }
            }

            // GOG
            if (empty($attributes['gog_slug'])) {
                if ($url !== '' && preg_match('/gog\.com\/game\/([^\/\?]+)/', $url, $m) === 1) {
                    $attributes['gog_slug'] = $m[1];
                } elseif ($externalId !== '' && preg_match('/^[a-z0-9_\-]+$/i', $externalId) === 1 && preg_match('/^\d+$/', $externalId) !== 1) {
                    $attributes['gog_slug'] = $externalId;
                }
            }

            // Epic (slug only; ignore numeric IDs)
            if (empty($attributes['epic_slug']) && $url !== '' && preg_match('/\/p\/([^\/\?]+)/', $url, $m) === 1) {
                $attributes['epic_slug'] = $m[1];
            }

            // Xbox BigId
            if (empty($attributes['xbox_bigid'])) {
                if ($url !== '' && preg_match('/microsoft\.com\/.*\/([A-Z0-9]{12,})/i', $url, $m) === 1) {
                    $attributes['xbox_bigid'] = strtoupper($m[1]);
                } elseif ($externalId !== '' && preg_match('/^[A-Z0-9]{12,}$/i', $externalId) === 1) {
                    $attributes['xbox_bigid'] = strtoupper($externalId);
                }
            }

            // PlayStation
            if ($url !== '') {
                if (empty($attributes['ps_product_id']) && preg_match('/\/product\/([A-Za-z0-9_\-]+)/', $url, $m) === 1) {
                    $attributes['ps_product_id'] = $m[1];
                }
                if (empty($attributes['ps_concept_id']) && preg_match('/\/concept\/(\d+)/', $url, $m) === 1) {
                    $attributes['ps_concept_id'] = $m[1];
                }
            }

            // itch.io
            if (empty($attributes['itchio_url']) && $url !== '' && str_contains($url, 'itch.io/')) {
                $attributes['itchio_url'] = $url;
            }
        }

        return $attributes;
    }

    /** @param array<string, mixed> $attributes */
    private static function providerIsAvailable(string $provider, array $attributes): bool
    {
        return match ($provider) {
            'steam' => ! empty($attributes['steam_id']),
            'gog' => ! empty($attributes['gog_slug']),
            'epic' => ! empty($attributes['epic_slug']) && preg_match('/^\d+$/', (string) $attributes['epic_slug']) !== 1,
            'xbox' => ! empty($attributes['xbox_bigid']),
            'playstation' => ! empty($attributes['ps_product_id']) || ! empty($attributes['ps_concept_id']),
            'amazon' => ! empty($attributes['amazon_url']),
            'itchio' => ! empty($attributes['itchio_url']),
            default => true,
        };
    }
}
