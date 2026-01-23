<?php

declare(strict_types=1);

namespace App\Models;

use Illuminate\Database\Eloquent\Factories\HasFactory;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Eloquent\Relations\HasMany;

class VideoGameSource extends Model
{
    /** @use HasFactory<\Database\Factories\VideoGameSourceFactory> */
    use HasFactory;

    /**
     * NOTE: `video_game_sources` represents provider-level aggregation ONLY.
     * There should be exactly one row per upstream provider (e.g. IGDB, Steam),
     * and the `video_game_ids` JSON array holds the canonical child IDs.
     */
    protected $fillable = [
        'provider',
        'provider_key',
        'display_name',
        'category',
        'slug',
        'external_id',
        'metadata',
        'video_game_ids',
        'items_count',
    ];

    protected function casts(): array
    {
        return [
            'provider' => 'string',
            'provider_key' => 'string',
            'display_name' => 'string',
            'category' => 'string',
            'slug' => 'string',
            'external_id' => 'integer',
            'metadata' => 'array',
            'video_game_ids' => 'array',
            'items_count' => 'integer',
        ];
    }

    public function titleSources(): HasMany
    {
        return $this->hasMany(VideoGameTitleSource::class);
    }

    public function recordVideoGameId(int $videoGameId): void
    {
        // For performance and schema reasons, we don't store the full list of IDs in a JSON column anymore.
        // Just increment the count.
        try {
            $this->increment('items_count');
        } catch (\Throwable $e) {
            // Ignore stats update failures (timeouts/locks) to prioritize ingestion
            \Illuminate\Support\Facades\Log::warning('Failed to update VideoGameSource items_count: '.$e->getMessage());
        }
    }
}
