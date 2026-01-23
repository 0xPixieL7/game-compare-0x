<?php

declare(strict_types=1);

namespace App\Console\Commands;

use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Str;

class IgdbSyncToplists extends Command
{
    protected $signature = 'igdb:sync-toplists';

    protected $description = 'Sync IGDB top lists from materialized views into provider_toplists tables';

    public function handle(): int
    {
        $this->info('Syncing IGDB Top Lists...');

        // 1. Popular (Overall)
        $this->syncList('popular', 'Popular', 'collection', function () {
            return DB::table('video_games_ranked_mv')
                ->orderByDesc('popularity_score')
                ->limit(50)
                ->get(['id']);
        });

        // 2. Top Rated
        $this->syncList('top_rated', 'Top Rated', 'collection', function () {
            return DB::table('video_games_ranked_mv')
                ->orderByDesc('review_score')
                ->limit(50)
                ->get(['id']);
        });

        // 3. Upcoming
        $this->syncList('upcoming', 'Upcoming', 'collection', function () {
            return DB::table('video_games_ranked_mv')
                ->where('release_date', '>', now())
                ->orderBy('release_date', 'asc')
                ->orderByDesc('popularity_score')
                ->limit(50)
                ->get(['id']);
        });

        // 4. Per Genre (Top 20 each)
        $genres = DB::table('video_games_genre_ranked_mv')
            ->distinct()
            ->pluck('genre_name');

        foreach ($genres as $genre) {
            $listKey = Str::slug($genre);
            $this->syncList($listKey, $genre, 'genre', function () use ($genre) {
                return DB::table('video_games_genre_ranked_mv')
                    ->where('genre_name', $genre)
                    ->orderByDesc('review_score')
                    ->limit(20)
                    ->get(['id']);
            });
        }

        $this->info('Done.');

        return self::SUCCESS;
    }

    private function syncList(string $listKey, string $name, string $type, callable $query): void
    {
        $this->info("Syncing IGDB {$name}...");

        $snapshotAt = now()->startOfHour();

        DB::table('provider_toplists')->updateOrInsert(
            [
                'provider_key' => 'igdb',
                'list_key' => $listKey,
                'snapshot_at' => $snapshotAt,
            ],
            [
                'list_type' => $type,
                'name' => 'IGDB '.$name,
                'updated_at' => now(),
            ]
        );

        $toplistId = DB::table('provider_toplists')
            ->where('provider_key', 'igdb')
            ->where('list_key', $listKey)
            ->where('snapshot_at', $snapshotAt)
            ->value('id');

        // Clear existing items for this snapshot
        DB::table('provider_toplist_items')->where('provider_toplist_id', $toplistId)->delete();

        $rows = $query();
        $items = [];
        foreach ($rows as $index => $row) {
            $items[] = [
                'provider_toplist_id' => $toplistId,
                'video_game_id' => $row->id,
                'external_id' => 0, // Not needed for internal IGDB sync as we have video_game_id
                'rank' => $index + 1,
                'metadata' => json_encode(['synced_from' => 'mv']),
                'created_at' => now(),
                'updated_at' => now(),
            ];
        }

        if (! empty($items)) {
            DB::table('provider_toplist_items')->insert($items);
        }
    }
}
