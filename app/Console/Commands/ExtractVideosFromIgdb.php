<?php

namespace App\Console\Commands;

use App\Models\VideoGame;
use App\Models\VideoGameTitleSource;
use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Str;

class ExtractVideosFromIgdb extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'media:extract-videos';

    /**
     * The console command description.
     *
     * @var string
     */
    protected $description = 'Extract video URLs (YouTube, Twitch, etc.) from IGDB raw payloads';

    /**
     * Execute the console command.
     */
    public function handle()
    {
        $this->info("Extracting videos from IGDB payloads...");

        VideoGameTitleSource::where('provider', 'igdb')
            ->whereNotNull('raw_payload')
            ->chunk(100, function ($sources) {
                foreach ($sources as $source) {
                    $payload = $source->raw_payload;

                    // Handle double-encoded JSON
                    if (is_string($payload)) {
                        if (Str::startsWith($payload, '[') || Str::startsWith($payload, '{')) {
                            $decoded = json_decode($payload, true);
                            if (is_array($decoded)) {
                                $payload = $decoded;
                            }
                        }
                    }

                    if (!is_array($payload)) {
                        continue;
                    }

                    $videoGames = VideoGame::where('video_game_title_id', $source->video_game_title_id)->get();

                    if ($videoGames->isEmpty()) {
                        continue;
                    }

                    // Extract videos from IGDB payload
                    $videos = $payload['videos'] ?? [];
                    
                    // Handle case where videos is a JSON string
                    if (is_string($videos)) {
                        if (Str::startsWith($videos, '[') || Str::startsWith($videos, '{')) {
                            $decoded = json_decode($videos, true);
                            if (is_array($decoded)) {
                                $videos = $decoded;
                            }
                        }
                    }

                    if (!is_array($videos) || empty($videos)) {
                        continue;
                    }

                    foreach ($videoGames as $game) {
                        foreach ($videos as $video) {
                            if (!is_array($video)) {
                                continue;
                            }

                            $videoId = $video['video_id'] ?? null;
                            $name = $video['name'] ?? 'Trailer';
                            
                            if (!$videoId) {
                                continue;
                            }

                            // Construct YouTube URL
                            $url = "https://www.youtube.com/watch?v={$videoId}";
                            $thumbnailUrl = "https://img.youtube.com/vi/{$videoId}/maxresdefault.jpg";

                            $this->line("Found YouTube video for {$game->name}: {$name}");

                            try {
                                DB::table('videos')->updateOrInsert(
                                    [
                                        'videoable_type' => 'App\\Models\\VideoGame',
                                        'videoable_id' => $game->id,
                                        'url' => $url,
                                    ],
                                    [
                                        'thumbnail_url' => $thumbnailUrl,
                                        'title' => $name,
                                        'metadata' => json_encode([
                                            'source' => 'igdb',
                                            'video_id' => $videoId,
                                            'type' => 'youtube',
                                        ]),
                                        'updated_at' => now(),
                                        'created_at' => now(),
                                    ]
                                );
                            } catch (\Exception $e) {
                                $this->error("Failed to save video for {$game->name}: " . $e->getMessage());
                            }
                        }
                    }
                }
            });

        $this->info("Video extraction complete.");
    }
}
