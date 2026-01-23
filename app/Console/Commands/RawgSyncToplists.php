<?php

declare(strict_types=1);

namespace App\Console\Commands;

use App\Models\VideoGameTitleSource;
use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\File;
use Illuminate\Support\Str;

class RawgSyncToplists extends Command
{
    protected $signature = 'rawg:sync-toplists {--dir=rawg-discovery : Directory in storage/app}';

    protected $description = 'Sync RAWG discovery JSON files into provider_toplists tables';

    public function handle(): int
    {
        $dir = (string) $this->option('dir');
        $path = storage_path('app/'.trim($dir, '/'));

        if (! File::isDirectory($path)) {
            $this->error("Directory not found: {$path}");

            return self::FAILURE;
        }

        $files = File::files($path);
        $topListFiles = array_filter($files, fn ($f) => str_starts_with($f->getFilename(), 'rawg_top_'));

        if (empty($topListFiles)) {
            $this->warn("No rawg_top_*.json files found in {$path}");

            return self::SUCCESS;
        }

        foreach ($topListFiles as $file) {
            $filename = $file->getFilename();
            // Expected format: rawg_top_{list_key}_{limit}.json
            if (! preg_match('/rawg_top_([a-z0-9_-]+)_(\d+)\.json/', $filename, $matches)) {
                continue;
            }

            $listKey = $matches[1];
            $limit = (int) $matches[2];
            $data = json_decode(File::get($file->getPathname()), true);

            if (! is_array($data)) {
                $this->error("Invalid JSON in {$filename}");

                continue;
            }

            $this->info("Syncing {$listKey} ({$filename})...");

            $toplist = DB::table('provider_toplists')->updateOrInsert(
                [
                    'provider_key' => 'rawg',
                    'list_key' => $listKey,
                    'snapshot_at' => now()->startOfHour(), // Coarse-grained snapshot
                ],
                [
                    'list_type' => $this->resolveListType($listKey),
                    'name' => 'RAWG '.Str::title(str_replace('_', ' ', $listKey)),
                    'updated_at' => now(),
                ]
            );

            $toplistId = DB::table('provider_toplists')
                ->where('provider_key', 'rawg')
                ->where('list_key', $listKey)
                ->orderByDesc('snapshot_at')
                ->value('id');

            // Clear old items for this snapshot if any
            DB::table('provider_toplist_items')->where('provider_toplist_id', $toplistId)->delete();

            $items = [];
            foreach ($data as $index => $record) {
                if (! isset($record['id'])) {
                    continue;
                }

                $rawgId = (int) $record['id'];
                $rank = $index + 1;

                // Try to find if we already have this game
                $source = VideoGameTitleSource::where('provider', 'rawg')
                    ->where('external_id', $rawgId)
                    ->first(['video_game_title_id']);

                $videoGameId = null;
                if ($source && $source->video_game_title_id) {
                    $videoGameId = DB::table('video_games')
                        ->where('video_game_title_id', $source->video_game_title_id)
                        ->value('id');
                }

                $items[] = [
                    'provider_toplist_id' => $toplistId,
                    'video_game_id' => $videoGameId,
                    'external_id' => $rawgId,
                    'rank' => $rank,
                    'metadata' => json_encode($record),
                    'created_at' => now(),
                    'updated_at' => now(),
                ];
            }

            if (! empty($items)) {
                DB::table('provider_toplist_items')->insert($items);
                $this->line('  Inserted '.count($items).' items.');
            }
        }

        $this->info('Done.');

        return self::SUCCESS;
    }

    private function resolveListType(string $key): string
    {
        if (in_array($key, ['trending', 'upcoming', 'popular'])) {
            return 'collection';
        }

        return 'genre';
    }
}
