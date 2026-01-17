<?php

declare(strict_types=1);

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;
use Illuminate\Support\Str;

return new class extends Migration
{
    public function up(): void
    {
        if (! Schema::hasTable('video_game_profiles')) {
            Schema::create('video_game_profiles', function (Blueprint $table): void {
                $table->foreignId('video_game_title_id')
                    ->primary()
                    ->constrained('video_game_titles')
                    ->cascadeOnDelete();

                $table->string('slug')->unique();
                $table->string('name')->nullable();
                $table->text('summary')->nullable();
                $table->text('description')->nullable();
                $table->text('storyline')->nullable();
                $table->string('official_site')->nullable();
                $table->date('release_date')->nullable();
                $table->json('platforms')->nullable();
                $table->decimal('rating', 20, 10)->nullable();
                $table->unsignedInteger('rating_count')->nullable();
                $table->string('developer')->nullable();
                $table->string('publisher')->nullable();
                $table->json('genres')->nullable();
                $table->json('media')->nullable();
                $table->json('metadata')->nullable();
                $table->timestamps();

                $table->index('release_date');
            });
        }

        $columnsToCopy = [
            'video_game_title_id',
            'slug',
            'name',
            'summary',
            'description',
            'storyline',
            'url',
            'release_date',
            'platform',
            'rating',
            'rating_count',
            'developer',
            'publisher',
            'genre',
            'media',
            'source_payload',
            'created_at',
            'updated_at',
        ];

        if (Schema::hasTable('video_games') && Schema::hasColumn('video_games', 'slug')) {
            DB::table('video_games')
                ->orderBy('id')
                ->chunkById(500, function ($chunk): void {
                    $profiles = [];

                    foreach ($chunk as $game) {
                        if (! isset($game->video_game_title_id)) {
                            continue;
                        }

                        $slug = $game->slug ?: Str::slug($game->name ?? 'game-'.$game->video_game_title_id);
                        if ($slug === '') {
                            $slug = 'game-'.$game->video_game_title_id;
                        }

                        $profiles[$game->video_game_title_id] = [
                            'video_game_title_id' => $game->video_game_title_id,
                            'slug' => $slug,
                            'name' => $game->name,
                            'summary' => $game->summary,
                            'description' => $game->description,
                            'storyline' => $game->storyline,
                            'official_site' => $game->url,
                            'release_date' => $game->release_date,
                            'platforms' => $game->platform,
                            'rating' => $game->rating,
                            'rating_count' => $game->rating_count,
                            'developer' => $game->developer,
                            'publisher' => $game->publisher,
                            'genres' => $game->genre,
                            'media' => $game->media,
                            'metadata' => $game->source_payload,
                            'created_at' => $game->created_at,
                            'updated_at' => $game->updated_at,
                        ];
                    }

                    if ($profiles !== []) {
                        DB::table('video_game_profiles')->upsert(
                            array_values($profiles),
                            ['video_game_title_id'],
                            [
                                'slug',
                                'name',
                                'summary',
                                'description',
                                'storyline',
                                'official_site',
                                'release_date',
                                'platforms',
                                'rating',
                                'rating_count',
                                'developer',
                                'publisher',
                                'genres',
                                'media',
                                'metadata',
                                'updated_at',
                            ]
                        );
                    }
                });
        }

        Schema::table('video_games', function (Blueprint $table): void {
            if (! Schema::hasColumn('video_games', 'attributes')) {
                $table->json('attributes')->nullable()->after('external_id');
            }
        });

        // Keep name, rating, release_date in video_games as quick-access cache
        // Only drop columns that move exclusively to profiles
        $columnsToDrop = array_values(array_filter([
            // Schema::hasColumn('video_games', 'slug') ? 'slug' : null,
            // name is kept in video_games as cache
            // Schema::hasColumn('video_games', 'summary') ? 'summary' : null,
            // Schema::hasColumn('video_games', 'description') ? 'description' : null,
            // Schema::hasColumn('video_games', 'storyline') ? 'storyline' : null,
            // Schema::hasColumn('video_games', 'url') ? 'url' : null,
            // release_date is kept in video_games as cache
            // Schema::hasColumn('video_games', 'platform') ? 'platform' : null,
            // rating is kept in video_games as cache
            // Schema::hasColumn('video_games', 'rating_count') ? 'rating_count' : null,
            // Schema::hasColumn('video_games', 'developer') ? 'developer' : null,
            // Schema::hasColumn('video_games', 'publisher') ? 'publisher' : null,
            // Schema::hasColumn('video_games', 'genre') ? 'genre' : null,
            // Schema::hasColumn('video_games', 'media') ? 'media' : null,
            // Schema::hasColumn('video_games', 'source_payload') ? 'source_payload' : null,
        ]));

        // SQLite cannot drop columns with unique indexes - skip on SQLite
        // This only affects local development testing; production uses PostgreSQL
        if ($columnsToDrop !== [] && DB::getDriverName() !== 'sqlite') {
            Schema::table('video_games', function (Blueprint $table) use ($columnsToDrop): void {
                $table->dropColumn($columnsToDrop);
            });
        }
    }

    public function down(): void
    {
        if (! Schema::hasTable('video_game_profiles')) {
            return;
        }

        // Re-add only columns that were dropped (not name, rating, release_date which were kept)
        Schema::table('video_games', function (Blueprint $table): void {
            $table->string('slug')->unique()->nullable();
            // name stays in video_games
            $table->text('summary')->nullable();
            $table->text('description')->nullable();
            $table->text('storyline')->nullable();
            $table->string('url')->nullable();
            // release_date stays in video_games
            $table->json('platform')->nullable();
            // rating stays in video_games
            $table->unsignedInteger('rating_count')->nullable();
            $table->string('developer')->nullable();
            $table->string('publisher')->nullable();
            $table->json('genre')->nullable();
            $table->json('media')->nullable();
            $table->json('source_payload')->nullable();
        });

        DB::table('video_game_profiles')
            ->orderBy('video_game_title_id')
            ->chunkById(500, function ($profiles): void {
                foreach ($profiles as $profile) {
                    DB::table('video_games')
                        ->where('video_game_title_id', $profile->video_game_title_id)
                        ->update([
                            'slug' => $profile->slug,
                            'name' => $profile->name,
                            'summary' => $profile->summary,
                            'description' => $profile->description,
                            'storyline' => $profile->storyline,
                            'url' => $profile->official_site,
                            'release_date' => $profile->release_date,
                            'platform' => $profile->platforms,
                            'rating' => $profile->rating,
                            'rating_count' => $profile->rating_count,
                            'developer' => $profile->developer,
                            'publisher' => $profile->publisher,
                            'genre' => $profile->genres,
                            'media' => $profile->media,
                            'source_payload' => $profile->metadata,
                        ]);
                }
            });

        Schema::table('video_games', function (Blueprint $table): void {
            if (Schema::hasColumn('video_games', 'attributes')) {
                $table->dropColumn('attributes');
            }
        });

        Schema::dropIfExists('video_game_profiles');
    }
};
