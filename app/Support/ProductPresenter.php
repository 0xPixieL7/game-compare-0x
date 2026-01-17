<?php

declare(strict_types=1);

namespace App\Support;

use App\Models\Product;
use App\Support\Media\ProductMediaResolver;
use Illuminate\Support\Collection;

final class ProductPresenter
{
    /**
     * Build a map of aggregate information for a collection of products.
     * This avoids N+1 when presenting multiple spotlight products.
     *
     * @param  array<Product>|Collection<int, Product>  $products
     */
    public static function aggregateMap(array|Collection $products): Collection
    {
        // Normalize to collection
        $products = collect($products);

        // For now, we return an empty collection as the presenter
        // will resolve media individually or via eager loading.
        return collect();
    }

    /**
     * Transform a product into a presentation array.
     *
     * @return array<string, mixed>
     */
    public static function present(Product $product, mixed $aggregateSet = null): array
    {
        $mediaSet = ProductMediaResolver::resolve($product);
        $primaryCover = $mediaSet->primaryCoverImage();
        $primaryVideo = $mediaSet->primaryVideo();

        return [
            'id' => $product->id,
            'name' => $product->name,
            'slug' => $product->slug,
            'image' => $primaryCover?->url,
            'trailer_thumbnail' => $primaryVideo?->thumbnail,
            'trailer_url' => $primaryVideo?->embedUrl ?? $primaryVideo?->url,
            'platform' => $product->platform,
            'category' => $product->category,
            'rating' => (float) ($product->rating ?? 0.0),
            'popularity' => (float) ($product->popularity_score ?? 0.0),
            'spotlight_score' => [
                'total' => (float) ($product->rating ?? 75.0),
                'grade' => self::gradeFor((float) ($product->rating ?? 75.0) / 100),
                'verdict' => self::verdictFor((float) ($product->rating ?? 75.0) / 100),
            ],
            'spotlight_gallery' => $mediaSet->gallery()->take(5)->map(fn ($item) => $item->toArray())->all(),
            'metadata' => (array) ($product->metadata ?? []),
        ];
    }

    private static function gradeFor(float $score): string
    {
        if ($score >= 0.9) {
            return 'S';
        }
        if ($score >= 0.8) {
            return 'A';
        }
        if ($score >= 0.7) {
            return 'B';
        }
        if ($score >= 0.6) {
            return 'C';
        }
        if ($score >= 0.5) {
            return 'D';
        }

        return 'E';
    }

    private static function verdictFor(float $score): string
    {
        if ($score >= 0.85) {
            return 'Benchmark ready';
        }
        if ($score >= 0.7) {
            return 'High confidence coverage';
        }
        if ($score >= 0.55) {
            return 'Good, with gaps to fill';
        }

        return 'Spotlight warming up';
    }
}
