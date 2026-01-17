<?php

namespace App\Models;

use Illuminate\Database\Eloquent\Factories\HasFactory;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Eloquent\Relations\HasMany;
use Illuminate\Database\Eloquent\Relations\HasManyThrough;

class Product extends Model
{
    use HasFactory;

    /**
     * IMPORTANT:
     * `products` do NOT relate to `video_games` directly.
     * The ONLY allowed traversal is:
     *   products (1) -> video_game_titles (many) -> video_games (many)
     */
    protected $fillable = [
        'name',
        'slug',
        'type',
        'title',
        'normalized_title',
        'synopsis',
        'platform',
        'category',
        'release_date',
        'popularity_score',
        'rating',
        'external_ids',
        'metadata',
    ];

    protected function casts(): array
    {
        return [
            'title' => 'string',
            'normalized_title' => 'string',
            'synopsis' => 'string',
            'release_date' => 'date',
            'popularity_score' => 'decimal:4',
            'rating' => 'decimal:2',
            'external_ids' => 'json',
            'metadata' => 'json',
        ];
    }

    // Scope to only video games (products that represent game families)
    public function scopeVideoGames($query)
    {
        return $query->where('type', 'video_game');
    }

    // Grouping by normalized title = "product family"
    public function scopeWithNormalizedTitle($query, string $normalizedTitle)
    {
        return $query->where('normalized_title', $normalizedTitle);
    }

    public function popularityScore()
    {
        // Example calculation for popularity score
        return ($this->sales_count * 0.5) + ($this->review_count * 0.3) + ($this->average_rating * 0.2);
    }

    public function videoGameTitles(): HasMany
    {
        return $this->hasMany(VideoGameTitle::class);
    }

    public function videoGames(): HasManyThrough
    {
        return $this->hasManyThrough(
            VideoGame::class,
            VideoGameTitle::class,
            'product_id',
            'video_game_title_id',
            'id',
            'id'
        );
    }

    /**
     * Get all SKU regions (pricing entries) for this product.
     *
     * Now uses direct product_id foreign key on video_game_prices table.
     */
    public function skuRegions(): HasMany
    {
        return $this->hasMany(SkuRegion::class, 'product_id');
    }
}
