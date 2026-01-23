<?php

declare(strict_types=1);

namespace App\Console\Commands;

use App\Models\Image;
use App\Models\Video;
use App\Models\VideoGame;
use App\Models\VideoGameSource;
use App\Models\VideoGameTitleSource;
use App\Services\Normalization\IgdbRatingHelper;
use Illuminate\Console\Command;
use Illuminate\Support\Facades\Http;

class IgdbRefreshIdsCommand extends Command
{
    protected $signature = 'igdb:refresh-ids {ids* : IGDB IDs to refresh} {--provider=igdb}';

    protected $description = 'Refresh specific IGDB games by their IDs';

    private string $baseUrl = 'https://api.igdb.com/v4';

    public function handle(): int
    {
        $ids = $this->argument('ids');
        $provider = $this->option('provider');

        $clientId = config('services.igdb.client_id');
        $clientSecret = config('services.igdb.client_secret');

        $this->info('🔑 Obtaining OAuth token...');
        $response = Http::asForm()->post('https://id.twitch.tv/oauth2/token', [
            'client_id' => $clientId,
            'client_secret' => $clientSecret,
            'grant_type' => 'client_credentials',
        ]);

        if (! $response->successful()) {
            $this->error('Failed to obtain token');

            return self::FAILURE;
        }

        $token = $response->json('access_token');
        $providerSource = VideoGameSource::where('provider', $provider)->first();

        $this->info('🚀 Fetching '.count($ids).' games from IGDB...');

        $fields = implode(',', [
            'id', 'name', 'slug', 'summary', 'storyline',
            'first_release_date', 'rating', 'rating_count',
            'aggregated_rating', 'aggregated_rating_count',
            'hypes', 'follows', 'total_rating', 'total_rating_count',
            'platforms', 'genres', 'themes', 'keywords',
            'category', 'status', 'url', 'checksum',
            'cover.*', 'screenshots.*', 'videos.*', 'artworks.*', 'websites.*',
            'external_games.*',
        ]);

        $idString = implode(',', $ids);
        $query = "fields {$fields}; where id = ({$idString}); limit 500;";

        $response = Http::withHeaders([
            'Client-ID' => $clientId,
            'Authorization' => 'Bearer '.$token,
        ])->withBody($query, 'text/plain')->post("{$this->baseUrl}/games");

        if (! $response->successful()) {
            $this->error('API request failed: '.$response->body());

            return self::FAILURE;
        }

        $games = $response->json();
        $ratingHelper = new IgdbRatingHelper;
        $now = now();

        foreach ($games as $gameData) {
            $externalId = (string) $gameData['id'];
            $this->info("✨ Processing: {$gameData['name']} (External ID: {$externalId})");

            $rating = $ratingHelper->extractPercentage($gameData);
            $ratingCount = $ratingHelper->extractRatingCount($gameData);

            // 1. Find or update VideoGame via source
            $titleSource = VideoGameTitleSource::where('video_game_source_id', $providerSource->id)
                ->where('provider_item_id', $externalId)
                ->first();

            if ($titleSource) {
                $videoGame = VideoGame::find($titleSource->video_game_title_id); // This might be wrong if title_id != game_id
                // Actually, let's find the video_game by external_id and provider directly
                $videoGame = VideoGame::where('provider', $provider)->where('external_id', $externalId)->first();
            } else {
                $this->warn("Game not found in database: {$gameData['name']}. Skipping for now (auto-creation needs more logic).");

                continue;
            }

            if (! $videoGame) {
                $this->error("Could not find VideoGame row for {$externalId}");

                continue;
            }

            // Update core data
            $videoGame->update([
                'rating' => $rating,
                'rating_count' => $ratingCount,
                'summary' => $gameData['summary'] ?? $videoGame->summary,
                'release_date' => isset($gameData['first_release_date'])
                    ? date('Y-m-d H:i:s', $gameData['first_release_date'])
                    : $videoGame->release_date,
            ]);

            // Update source payload
            $titleSource->update([
                'raw_payload' => json_encode($gameData),
                'updated_at' => $now,
            ]);

            // 2. Sync Media
            // Images
            $images = [];
            if (isset($gameData['cover']['image_id'])) {
                $images[] = "https://images.igdb.com/igdb/image/upload/t_cover_big/{$gameData['cover']['image_id']}.jpg";
            }
            if (isset($gameData['screenshots'])) {
                foreach ($gameData['screenshots'] as $s) {
                    if (isset($s['image_id'])) {
                        $images[] = "https://images.igdb.com/igdb/image/upload/t_screenshot_huge/{$s['image_id']}.jpg";
                    }
                }
            }
            if (isset($gameData['artworks'])) {
                foreach ($gameData['artworks'] as $a) {
                    if (isset($a['image_id'])) {
                        $images[] = "https://images.igdb.com/igdb/image/upload/t_1080p/{$a['image_id']}.jpg";
                    }
                }
            }

            foreach ($images as $url) {
                Image::upsert([
                    'imageable_type' => VideoGame::class,
                    'imageable_id' => $videoGame->id,
                    'url' => $url,
                    'updated_at' => $now,
                ], ['imageable_type', 'imageable_id', 'url'], ['updated_at']);
            }

            // Videos
            if (isset($gameData['videos'])) {
                foreach ($gameData['videos'] as $v) {
                    if (isset($v['video_id'])) {
                        $videoId = $v['video_id'];
                        $videoName = $v['name'] ?? 'Trailer';
                        $url = "https://www.youtube.com/watch?v={$videoId}";

                        Video::upsert([
                            'videoable_type' => VideoGame::class,
                            'videoable_id' => $videoGame->id,
                            'video_game_id' => $videoGame->id,
                            'url' => $url,
                            'video_id' => $videoId,
                            'title' => $videoName,
                            'provider' => 'youtube',
                            'metadata' => json_encode([$v]),
                            'updated_at' => $now,
                        ], ['videoable_type', 'videoable_id', 'url'], ['title', 'metadata', 'updated_at']);
                    }
                }
            }
        }

        // 3. Batch Fetch External Game IDs (TGDB, GiantBomb)
        $allExternalGameIds = [];
        foreach ($games as $gameData) {
            if (isset($gameData['external_games'])) {
                $allExternalGameIds = array_merge($allExternalGameIds, $gameData['external_games']);
            }
        }

        if (! empty($allExternalGameIds)) {
            $this->info('🔗 Fetching external game references...');
            $flattenedIds = collect($allExternalGameIds)->flatten()->filter()->unique()->values()->toArray();
            $externalGameIds = implode(',', $flattenedIds);
            $extQuery = "fields id, game, category, uid; where id = ({$externalGameIds}) & category = (2,3); limit 500;";

            $extResponse = Http::withHeaders([
                'Client-ID' => $clientId,
                'Authorization' => 'Bearer '.$token,
            ])->withBody($extQuery, 'text/plain')->post("{$this->baseUrl}/external_games");

            if ($extResponse->successful()) {
                $externalGames = $extResponse->json();

                foreach ($externalGames as $ext) {
                    $igdbGameId = (string) $ext['game'];
                    $category = $ext['category'];
                    $externalUid = $ext['uid'];

                    // Find the VideoGame by IGDB external_id
                    $videoGame = VideoGame::where('provider', $provider)
                        ->where('external_id', $igdbGameId)
                        ->first();

                    if (! $videoGame) {
                        continue;
                    }

                    $externalProvider = $category === 2 ? 'tgdb' : 'giantbomb';

                    $externalSource = VideoGameSource::firstOrCreate(
                        ['provider' => $externalProvider],
                        ['name' => $externalProvider === 'tgdb' ? 'TheGamesDB' : 'GiantBomb']
                    );

                    VideoGameTitleSource::updateOrCreate(
                        [
                            'video_game_title_id' => $videoGame->video_game_title_id,
                            'video_game_source_id' => $externalSource->id,
                            'provider' => $externalProvider,
                        ],
                        [
                            'external_id' => (string) $externalUid,
                            'provider_item_id' => (string) $externalUid,
                            'name' => $videoGame->name,
                            'updated_at' => $now,
                        ]
                    );

                    $this->line("  └─ {$videoGame->name}: Linked {$externalProvider} ID {$externalUid}");
                }
            }
        }

        $this->info('✅ Refresh complete. Please refresh materialized views.');

        return self::SUCCESS;
    }
}
