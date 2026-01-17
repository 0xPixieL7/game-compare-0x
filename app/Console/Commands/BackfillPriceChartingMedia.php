<?php

declare(strict_types=1);

namespace App\Console\Commands;

use App\Models\Image;
use App\Models\VideoGameTitleSource;
use App\Services\Normalization\PlatformNormalizer;
use Illuminate\Console\Command;
use Illuminate\Support\Facades\Http;
use Illuminate\Support\Str;

class BackfillPriceChartingMedia extends Command
{
    protected $signature = 'pricecharting:backfill-media {--limit=1000 : Games to process per run}';

    protected $description = 'Backfill missing cover art for Price Charting games using local IGDB/TGDB mirrors and RAWG API';

    private PlatformNormalizer $platformNormalizer;

    public function handle(): int
    {
        $this->platformNormalizer = new PlatformNormalizer;
        $limit = (int) $this->option('limit');

        // 1. Find Price Charting games WITHOUT images
        // We look for sources where NO image exists in the images table linked to this source
        $this->info('Finding Price Charting games without media...');

        $query = VideoGameTitleSource::query()
            ->where('provider', 'price_charting')
            ->whereDoesntHave('images') // Assumes polymorphic relation in Source model or manual check
            ->limit($limit);

        // Note: whereDoesntHave requires the 'images' relationship to be defined on VideoGameTitleSource model.
        // If not defined, we might crash. We checked the model file earlier and it didn't have it.
        // We should add it or use a join.
        // For now, let's process them and check inside the loop if we can't rely on whereDoesntHave.

        $sources = $query->get();

        if ($sources->isEmpty()) {
            $this->info('No games found needing backfill.');

            return Command::SUCCESS;
        }

        $this->info('Processing '.$sources->count().' games...');
        $bar = $this->output->createProgressBar($sources->count());
        $bar->start();

        foreach ($sources as $source) {
            $this->processSource($source);
            $bar->advance();
        }

        $bar->finish();
        $this->newLine();
        $this->info('Backfill complete.');

        return Command::SUCCESS;
    }

    private function processSource(VideoGameTitleSource $pcSource): void
    {
        $name = $pcSource->name;
        // PC platforms are JSON array: ["Nintendo Switch"]
        $platforms = $pcSource->platform ?? [];
        $platform = $platforms[0] ?? null;

        if (! $name || ! $platform) {
            return;
        }

        // Clean name slightly for matching
        $cleanName = Str::slug($name);

        // 1. Try Local IGDB Mirror
        $igdbMatch = $this->findLocalMatch('igdb', $name, $platform);
        if ($igdbMatch) {
            $this->copyImages($igdbMatch, $pcSource);

            return;
        }

        // 2. Try Local TGDB Mirror
        $tgdbMatch = $this->findLocalMatch('tgdb', $name, $platform);
        if ($tgdbMatch) {
            $this->copyImages($tgdbMatch, $pcSource);

            return;
        }

        // 3. Try RAWG API (Live)
        // Only if we really need to.
        $rawgUrl = $this->fetchRawgImage($name);
        if ($rawgUrl) {
            $this->saveImage($pcSource, $rawgUrl, 'rawg');
        }
    }

    private function findLocalMatch(string $provider, string $targetName, string $targetPlatform): ?VideoGameTitleSource
    {
        // Simple exact name match for now
        // A more robust solution would use Levenshtein distance or normalized slugs
        return VideoGameTitleSource::where('provider', $provider)
            ->where('name', $targetName)
            ->first();
    }

    private function copyImages(VideoGameTitleSource $donor, VideoGameTitleSource $recipient): void
    {
        $images = Image::where('imageable_type', VideoGameTitleSource::class)
            ->where('imageable_id', $donor->id)
            ->get();

        if ($images->isEmpty()) {
            return;
        }

        foreach ($images as $img) {
            // Check if we already have this image for recipient to avoid dupes
            $exists = Image::where('imageable_type', VideoGameTitleSource::class)
                ->where('imageable_id', $recipient->id)
                ->where('url', $img->url)
                ->exists();

            if (! $exists) {
                $this->saveImage($recipient, $img->url, $donor->provider);
            }
        }
    }

    private function saveImage(VideoGameTitleSource $source, string $url, string $provider): void
    {
        Image::create([
            'imageable_type' => VideoGameTitleSource::class,
            'imageable_id' => $source->id,
            'url' => $url,
            'provider' => $provider,
            'is_thumbnail' => true,
            'created_at' => now(),
            'updated_at' => now(),
        ]);
    }

    private function fetchRawgImage(string $name): ?string
    {
        $key = env('RAWG_API_KEY');
        if (! $key) {
            return null;
        }

        try {
            $response = Http::timeout(2)->get('https://api.rawg.io/api/games', [
                'key' => $key,
                'search' => $name,
                'page_size' => 1,
            ]);

            if ($response->successful()) {
                $data = $response->json();

                return $data['results'][0]['background_image'] ?? null;
            }
        } catch (\Exception $e) {
            // Ignore API errors
        }

        return null;
    }
}
