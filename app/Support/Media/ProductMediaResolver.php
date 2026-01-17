<?php

declare(strict_types=1);

namespace App\Support\Media;

use App\DataTransferObjects\Media\ImageMediaItem;
use App\DataTransferObjects\Media\ProductMediaSet;
use App\DataTransferObjects\Media\VideoMediaItem;
use App\Models\Product;
use Illuminate\Support\Collection;

final class ProductMediaResolver
{
    /**
     * Resolve all media for a single product.
     */
    public static function resolve(Product $product): ProductMediaSet
    {
        // Traversal path: product -> video_game_titles -> video_games -> media (images/videos)
        $videoGames = $product->videoGames()->with(['images', 'videos'])->get();

        $images = collect();
        $videos = collect();

        foreach ($videoGames as $game) {
            // images is HasOne - returns single Image model or null
            $imageModel = $game->images;
            if ($imageModel && $imageModel instanceof \App\Models\Image) {
                foreach ($imageModel->expandToIndividual() as $item) {
                    $images->push(new ImageMediaItem(
                        id: (int) ($item['id'] ?? 0),
                        source: (string) ($item['provider'] ?? 'igdb'),
                        url: (string) ($item['url'] ?? ''),
                        thumbnail: (string) ($item['thumbnail_url'] ?? ''),
                        title: (string) ($item['title'] ?? ''),
                        caption: (string) ($item['caption'] ?? ''),
                        license: (string) ($item['license'] ?? ''),
                        licenseUrl: (string) ($item['license_url'] ?? ''),
                        attribution: (string) ($item['attribution'] ?? ''),
                        width: isset($item['width']) ? (int) $item['width'] : null,
                        height: isset($item['height']) ? (int) $item['height'] : null,
                        quality: (float) ($item['quality'] ?? 0.8),
                        ordinal: (int) ($item['order'] ?? 0),
                        isPrimary: (bool) ($item['is_primary'] ?? false),
                        kind: (string) ($item['kind'] ?? 'screenshot'),
                        metadata: (array) ($item['metadata'] ?? []),
                        fetchedAt: (string) ($item['fetched_at'] ?? now()->toDateTimeString()),
                    ));
                }
            }

            // videos is HasOne - returns single Video model or null
            $videoModel = $game->videos;
            if ($videoModel && $videoModel instanceof \App\Models\Video) {
                foreach ($videoModel->expandToIndividual() as $item) {
                    $videos->push(new VideoMediaItem(
                        id: (int) ($item['id'] ?? 0),
                        source: (string) ($item['provider'] ?? 'youtube'),
                        url: (string) ($item['url'] ?? $item['youtube_watch_url'] ?? ''),
                        thumbnail: (string) ($item['thumbnail_url'] ?? ''),
                        title: (string) ($item['title'] ?? $item['name'] ?? ''),
                        caption: (string) ($item['caption'] ?? ''),
                        license: (string) ($item['license'] ?? ''),
                        licenseUrl: (string) ($item['license_url'] ?? ''),
                        attribution: (string) ($item['attribution'] ?? ''),
                        durationSeconds: isset($item['duration']) ? (int) $item['duration'] : null,
                        embedUrl: (string) ($item['youtube_embed_url'] ?? ''),
                        playUrl: (string) ($item['url'] ?? $item['youtube_watch_url'] ?? ''),
                        videoId: (string) ($item['video_id'] ?? ''),
                        ordinal: (int) ($item['order'] ?? 0),
                        metadata: (array) ($item['metadata'] ?? []),
                        fetchedAt: (string) ($item['fetched_at'] ?? now()->toDateTimeString()),
                    ));
                }
            }
        }

        return new ProductMediaSet(
            $images->unique(fn ($item) => $item->id.$item->url),
            $videos->unique(fn ($item) => $item->id.$item->url)
        );
    }

    /**
     * Batch resolve media for a collection of products.
     * In this implementation, we rely on Eloquent relationship loading.
     */
    public static function resolveMany(Collection $products, bool $force = false): void
    {
        // Eager load the chain to avoid N+1
        $products->load(['videoGames.images', 'videoGames.videos']);
    }
}
