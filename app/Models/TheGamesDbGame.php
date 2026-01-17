<?php

namespace App\Models;

use Illuminate\Database\Eloquent\Model;
use Illuminate\Support\Str;

class TheGamesDbGame extends Model
{
    protected $table = 'thegamesdb_games';

    protected $fillable = [
        'external_id',
        'title',
        'slug',
        'platform',
        'category',
        'players',
        'genres',
        'developer',
        'publisher',
        'release_date',
        'image_url',
        'thumb_url',
        'metadata',
        'last_synced_at',
    ];

    /**
     * @var array<string, string>
     */
    protected $casts = [
        'genres' => 'array',
        'metadata' => 'array',
        'release_date' => 'date',
        'last_synced_at' => 'datetime',
    ];

    protected static function booted(): void
    {
        static::saving(function (self $game): void {
            if (! $game->slug) {
                $game->slug = Str::slug($game->title ?? '');
            }
        });
    }
}
