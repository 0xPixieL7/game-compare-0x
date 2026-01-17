<?php

declare(strict_types=1);

namespace App\Models;

use Illuminate\Database\Eloquent\Factories\HasFactory;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Eloquent\Relations\BelongsTo;

/**
 * SkuRegion maps to the video_game_prices table.
 *
 * This model represents region-specific pricing/SKU information for products.
 * It bridges the legacy sku_regions concept with the new video_game_prices table.
 */
class SkuRegion extends Model
{
    /** @use HasFactory<\Database\Factories\SkuRegionFactory> */
    use HasFactory;

    /**
     * Use video_game_prices as the underlying table.
     */
    protected $table = 'video_game_prices';

    protected $fillable = [
        'video_game_id',
        'product_id',
        'currency',
        'amount_minor',
        'recorded_at',
        'retailer',
        'tax_inclusive',
        'region_code',
        'sku',
        'is_active',
        'metadata',
        'country_code',
        'url',
    ];

    protected $casts = [
        'amount_minor' => 'integer',
        'recorded_at' => 'datetime',
        'tax_inclusive' => 'boolean',
        'is_active' => 'boolean',
        'metadata' => 'json',
    ];

    public function videoGame(): BelongsTo
    {
        return $this->belongsTo(VideoGame::class);
    }

    public function product(): BelongsTo
    {
        return $this->belongsTo(Product::class);
    }

    /**
     * Get the formatted price in the region's currency.
     */
    public function getFormattedPriceAttribute(): string
    {
        $amount = $this->amount_minor / 100;

        return number_format($amount, 2).' '.$this->currency;
    }
}
