<?php

namespace App\Console\Commands;

use App\Services\Media\RAWG\RawgService;
use App\Models\VideoGame;
use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;

class PersistGameMediaCommand extends Command
{
    protected $signature = 'games:persist-media 
        {--source=rawg : Data source (rawg, playstation, xbox, steam, all)}
        {--year=2024,2025 : Years to fetch}
        {--limit=20 : Games per year}
        {--dry-run : Show what would be persisted without saving}';

    protected $description = 'Fetch game data from external APIs and persist to database';

    public function handle(RawgService $rawg)
    {
        $source = $this->option('source');
        $years = explode(',', $this->option('year'));
        $limit = (int) $this->option('limit');
        $dryRun = $this->option('dry-run');

        $this->info('🎮 Fetching and Persisting Game Media');
        $this->info("Source: {$source}");
        $this->info("Dry run: " . ($dryRun ? 'Yes' : 'No'));
        $this->newLine();

        $stats = [
            'games_processed' => 0,
            'games_created' => 0,
            'games_updated' => 0,
            'images_saved' => 0,
            'videos_saved' => 0,
            'errors' => 0,
        ];

        if ($source === 'rawg' || $source === 'all') {
            $this->info('📥 Fetching from RAWG...');
            foreach ($years as $year) {
                $year = trim($year);
                $this->line("Processing {$year}...");
                
                $response = $rawg->getAllGames([
                    'dates' => "{$year}-01-01,{$year}-12-31",
                    'ordering' => '-rating',
                ], page: 1, pageSize: $limit);

                foreach ($response['results'] ?? [] as $game) {
                    try {
                        $stats['games_processed']++;
                        
                        if ($dryRun) {
                            $this->line("  [DRY RUN] Would persist: {$game['name']}");
                            continue;
                        }

                        $result = $this->persistRAWGGame($game);
                        
                        if ($result['created']) {
                            $stats['games_created']++;
                            $this->line("  ✅ Created: {$game['name']}");
                        } else {
                            $stats['games_updated']++;
                            $this->line("  🔄 Updated: {$game['name']}");
                        }
                        
                        $stats['images_saved'] += $result['images_count'];
                        $stats['videos_saved'] += $result['videos_count'];
                        
                    } catch (\Throwable $e) {
                        $stats['errors']++;
                        $this->error("  ❌ Error: {$game['name']} - {$e->getMessage()}");
                    }
                }
            }
        }

        $this->newLine();
        $this->info('=== Summary ===');
        $this->table(
            ['Metric', 'Count'],
            [
                ['Games Processed', $stats['games_processed']],
                ['Games Created', $stats['games_created']],
                ['Games Updated', $stats['games_updated']],
                ['Images Saved', $stats['images_saved']],
                ['Videos Saved', $stats['videos_saved']],
                ['Errors', $stats['errors']],
            ]
        );

        return self::SUCCESS;
    }

    /**
     * Persist RAWG game data to database.
     */
    private function persistRAWGGame(array $game): array
    {
        $created = false;
        $imagesCount = 0;
        $videosCount = 0;

        // Find or create video game by external ID
        $videoGame = DB::transaction(function () use ($game, &$created, &$imagesCount, &$videosCount) {
            // Check if game exists by RAWG external ID
            $existingGame = DB::table('video_games')
                ->where('provider', 'rawg')
                ->where('external_id', (string) $game['id'])
                ->first();

            if ($existingGame) {
                $gameId = $existingGame->id;
                $created = false;
                
                // Update attributes with latest RAWG data
                $currentAttributes = json_decode($existingGame->attributes ?? '{}', true);
                DB::table('video_games')
                    ->where('id', $gameId)
                    ->update([
                        'attributes' => json_encode(array_merge($currentAttributes, [
                            'rawg_rating' => $game['rating'],
                            'rawg_ratings_count' => $game['ratings_count'],
                            'rawg_metacritic' => $game['metacritic'],
                            'rawg_updated_at' => now()->toIso8601String(),
                        ])),
                        'rating' => $game['rating'] ?? $existingGame->rating,
                        'rating_count' => $game['ratings_count'] ?? $existingGame->rating_count,
                        'updated_at' => now(),
                        'last_enriched_at' => now(),
                    ]);
            } else {
                // Create new game
                $gameId = DB::table('video_games')->insertGetId([
                    'name' => $game['name'],
                    'slug' => $game['slug'],
                    'provider' => 'rawg',
                    'external_id' => (string) $game['id'],
                    'description' => $game['description_raw'] ?? null,
                    'summary' => mb_substr($game['description_raw'] ?? '', 0, 500),
                    'url' => "https://rawg.io/games/{$game['slug']}",
                    'release_date' => $game['released'] ?? null,
                    'rating' => $game['rating'] ?? null,
                    'rating_count' => $game['ratings_count'] ?? null,
                    'genre' => json_encode(array_map(fn($g) => $g['name'], $game['genres'] ?? [])),
                    'developer' => json_encode(array_map(fn($d) => $d['name'], $game['developers'] ?? [])),
                    'publisher' => json_encode(array_map(fn($p) => $p['name'], $game['publishers'] ?? [])),
                    'platform' => json_encode(array_map(fn($p) => $p['platform']['name'] ?? 'Unknown', $game['platforms'] ?? [])),
                    'attributes' => json_encode([
                        'rawg_slug' => $game['slug'],
                        'rawg_rating' => $game['rating'],
                        'rawg_ratings_count' => $game['ratings_count'],
                        'rawg_metacritic' => $game['metacritic'],
                        'rawg_playtime' => $game['playtime'] ?? 0,
                        'rawg_tags' => array_slice(array_map(fn($t) => $t['name'], $game['tags'] ?? []), 0, 10),
                    ]),
                    'source_payload' => json_encode($game),
                    'created_at' => now(),
                    'updated_at' => now(),
                    'last_enriched_at' => now(),
                ]);
                $created = true;
            }

            // Save hero/background image
            if (!empty($game['background_image'])) {
                DB::table('images')->updateOrInsert(
                    [
                        'imageable_type' => 'App\\Models\\VideoGame',
                        'imageable_id' => $gameId,
                        'url' => $game['background_image'],
                    ],
                    [
                        'metadata' => json_encode(['source' => 'rawg', 'type' => 'hero']),
                        'updated_at' => now(),
                        'created_at' => now(),
                    ]
                );
                $imagesCount++;
            }

            // Save additional background
            if (!empty($game['background_image_additional'])) {
                DB::table('images')->updateOrInsert(
                    [
                        'imageable_type' => 'App\\Models\\VideoGame',
                        'imageable_id' => $gameId,
                        'url' => $game['background_image_additional'],
                    ],
                    [
                        'metadata' => json_encode(['source' => 'rawg', 'type' => 'background_additional']),
                        'updated_at' => now(),
                        'created_at' => now(),
                    ]
                );
                $imagesCount++;
            }

            // Save one reference screenshot
            if (!empty($game['short_screenshots'][0]['image'])) {
                DB::table('images')->updateOrInsert(
                    [
                        'imageable_type' => 'App\\Models\\VideoGame',
                        'imageable_id' => $gameId,
                        'url' => $game['short_screenshots'][0]['image'],
                    ],
                    [
                        'metadata' => json_encode(['source' => 'rawg', 'type' => 'screenshot']),
                        'updated_at' => now(),
                        'created_at' => now(),
                    ]
                );
                $imagesCount++;
            }

            // Save video clip
            if (!empty($game['clip'])) {
                $videoUrl = $game['clip']['clips']['full'] ?? $game['clip']['clips']['640'] ?? $game['clip']['video'] ?? null;
                
                if ($videoUrl) {
                    DB::table('videos')->updateOrInsert(
                        [
                            'videoable_type' => 'App\\Models\\VideoGame',
                            'videoable_id' => $gameId,
                            'url' => $videoUrl,
                        ],
                        [
                            'thumbnail_url' => $game['clip']['preview'] ?? null,
                            'metadata' => json_encode([
                                'source' => 'rawg',
                                'qualities' => array_keys($game['clip']['clips'] ?? []),
                            ]),
                            'updated_at' => now(),
                            'created_at' => now(),
                        ]
                    );
                    $videosCount++;
                }
            }

            return $gameId;
        });

        return [
            'created' => $created,
            'game_id' => $videoGame,
            'images_count' => $imagesCount,
            'videos_count' => $videosCount,
        ];
    }
}
