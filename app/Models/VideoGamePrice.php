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
        'recorded_at',
        'retailer',
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
            'recorded_at' => 'datetime',
            'tax_inclusive' => 'bool',
            'country_code' => 'string',
            'metadata' => 'array',
        ];
    }

    public function videoGame(): BelongsTo
    {
        return $this->belongsTo(VideoGame::class);
    }
}
