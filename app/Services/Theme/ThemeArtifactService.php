<?php

declare(strict_types=1);

namespace App\Services\Theme;

use App\Models\VideoGame;
use Illuminate\Support\Facades\Log;

class ThemeArtifactService
{
    /**
     * Collect visual artifacts (colors, styles) from game media.
     */
    public function collectArtifacts(VideoGame $game): void
    {
        $imageUrl = $game->getHeroImageUrl() ?? $game->getFirstMediaUrl('cover_images');

        if (! $imageUrl) {
            return;
        }

        // Ensure the URL is absolute for GD
        if (str_starts_with($imageUrl, '//')) {
            $imageUrl = 'https:'.$imageUrl;
        }

        try {
            $colors = $this->extractColorsFromUrl($imageUrl);

            if ($colors) {
                $attributes = $game->attributes ?? [];
                $attributes['theme'] = [
                    'primary' => $colors['primary'],
                    'accent' => $colors['accent'],
                    'background' => $colors['background'],
                    'surface' => $colors['surface'],
                    'generated_at' => now()->toIso8601String(),
                ];

                $game->attributes = $attributes;
                $game->save();

                Log::info("Visual artifacts collected for game {$game->id}", ['theme' => $attributes['theme']]);
            }
        } catch (\Throwable $e) {
            Log::warning("Theme artifact collection failed for game {$game->id}: ".$e->getMessage());
        }
    }

    /**
     * Extract dominant colors using GD.
     */
    private function extractColorsFromUrl(string $url): ?array
    {
        $content = @file_get_contents($url);
        if (! $content) {
            return null;
        }

        $img = @imagecreatefromstring($content);
        if (! $img) {
            return null;
        }

        $width = imagesx($img);
        $height = imagesy($img);

        // Resize to 10x10 to get a small sample of the overall color palette
        $sample = imagecreatetruecolor(10, 10);
        imagecopyresampled($sample, $img, 0, 0, 0, 0, 10, 10, $width, $height);

        $colors = [];
        for ($x = 0; $x < 10; $x++) {
            for ($y = 0; $y < 10; $y++) {
                $rgb = imagecolorat($sample, $x, $y);
                $r = ($rgb >> 16) & 0xFF;
                $g = ($rgb >> 8) & 0xFF;
                $b = $rgb & 0xFF;
                $colors[] = [$r, $g, $b];
            }
        }

        imagedestroy($img);
        imagedestroy($sample);

        // Simple averaging for now
        $totalR = 0;
        $totalG = 0;
        $totalB = 0;
        foreach ($colors as $c) {
            $totalR += $c[0];
            $totalG += $c[1];
            $totalB += $c[2];
        }

        $avgR = (int) ($totalR / 100);
        $avgG = (int) ($totalG / 100);
        $avgB = (int) ($totalB / 100);

        // Primary: Average color
        $primary = sprintf('#%02x%02x%02x', $avgR, $avgG, $avgB);

        // Background: Very dark version of primary or deep navy if too bright
        $luminance = ($avgR * 0.299 + $avgG * 0.587 + $avgB * 0.114) / 255;

        $bgR = (int) ($avgR * 0.1);
        $bgG = (int) ($avgG * 0.1);
        $bgB = (int) ($avgB * 0.15); // Slight blue tint for depth

        $background = sprintf('#%02x%02x%02x', $bgR, $bgG, $bgB);

        // Accent: Lighter/Saturated version
        $accR = min(255, (int) ($avgR * 1.5 + 20));
        $accG = min(255, (int) ($avgG * 1.5 + 20));
        $accB = min(255, (int) ($avgB * 1.5 + 20));
        $accent = sprintf('#%02x%02x%02x', $accR, $accG, $accB);

        // Surface: Slightly lighter than background
        $surfR = min(255, $bgR + 15);
        $surfG = min(255, $bgG + 15);
        $surfB = min(255, $bgB + 20);
        $surface = sprintf('#%02x%02x%02x', $surfR, $surfG, $surfB);

        return [
            'primary' => $primary,
            'accent' => $accent,
            'background' => $background,
            'surface' => $surface,
        ];
    }
}
