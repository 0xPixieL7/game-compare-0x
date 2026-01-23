<?php

declare(strict_types=1);

namespace App\Models;

use Illuminate\Database\Eloquent\Factories\HasFactory;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Eloquent\Relations\BelongsTo;

class VideoGamePrice extends Model
{
    /** @use HasFactory<\Database\Factories\VideoGamePriceFactory> */
    use HasFactory;

    protected $fillable = [
        'video_game_id',
        'product_id',
        'currency',
        'amount_minor',
        'amount_btc',
        'btc_value_sats',
        'aggregation_count',
        'series_key',
        'recorded_at',
        'bucket',
        'window_start',
        'window_end',
        'retailer',
        'retailer_id',
        'url',
        'tax_inclusive',
        'country_code',
        'region_code',
        'condition',
        'sku',
        'is_active',
        'is_retail_buy',
        'sales_volume',
        'metadata',
    ];

    protected function casts(): array
    {
        return [
            'currency' => 'string',
            'amount_minor' => 'int',
            'amount_btc' => 'decimal:10',
            'btc_value_sats' => 'int',
            'aggregation_count' => 'int',
            'recorded_at' => 'datetime',
            'bucket' => 'string',
            'window_start' => 'datetime',
            'window_end' => 'datetime',
            'tax_inclusive' => 'bool',
            'country_code' => 'string',
            'region_code' => 'string',
            'condition' => 'string',
            'sku' => 'string',
            'is_active' => 'bool',
            'is_retail_buy' => 'bool',
            'sales_volume' => 'int',
            'metadata' => 'array',
        ];
    }

    public function videoGame(): BelongsTo
    {
        return $this->belongsTo(VideoGame::class);
    }
}
