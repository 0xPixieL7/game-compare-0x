<?php

declare(strict_types=1);

namespace App\Support\Platforms;

use App\Support\Strings\StringNormalizer;
use App\Support\Strings\StringSimilarity;

final class PlatformFamilyDetector
{
    /**
     * @param  array<int,string>  $platforms
     */
    public static function looksLikePlayStationFamily(array $platforms, ?string $title = null): bool
    {
        $haystack = self::toHaystack($platforms, $title);

        // Fast paths
        if (str_contains($haystack, 'playstation')) {
            return true;
        }

        if (preg_match('/\bps\s*[1-5]\b/', $haystack) === 1 || preg_match('/\bps[1-5]\b/', $haystack) === 1) {
            return true;
        }

        // Fuzzy paths for typos like "playsttaion-5".
        if (StringSimilarity::jaroWinkler($haystack, 'playstation') >= 0.84) {
            return true;
        }

        // Check token-level fuzzy match (more stable than comparing the entire haystack).
        foreach (self::tokens($platforms, $title) as $token) {
            if ($token === '') {
                continue;
            }

            if (StringSimilarity::jaroWinkler($token, 'playstation') >= 0.88) {
                return true;
            }
        }

        return false;
    }

    /**
     * @param  array<int,string>  $platforms
     */
    public static function looksLikeXboxFamily(array $platforms, ?string $title = null): bool
    {
        $haystack = self::toHaystack($platforms, $title);

        if (str_contains($haystack, 'xbox')) {
            return true;
        }

        // Common shorthand forms: xbone, xsx, xss.
        if (preg_match('/\bxbone\b/', $haystack) === 1 || preg_match('/\bxsx\b/', $haystack) === 1 || preg_match('/\bxss\b/', $haystack) === 1) {
            return true;
        }

        foreach (self::tokens($platforms, $title) as $token) {
            if ($token === '') {
                continue;
            }

            if (StringSimilarity::jaroWinkler($token, 'xbox') >= 0.88) {
                return true;
            }
        }

        return false;
    }

    /**
     * @param  array<int,string>  $platforms
     */
    private static function toHaystack(array $platforms, ?string $title): string
    {
        $raw = trim(implode(' ', array_filter($platforms, fn ($p) => is_string($p) && trim($p) !== '')));
        if (is_string($title) && trim($title) !== '') {
            $raw .= ' '.$title;
        }

        // Keep whitespace for regex boundaries, but lower it.
        return strtolower($raw);
    }

    /**
     * @param  array<int,string>  $platforms
     * @return array<int,string>
     */
    private static function tokens(array $platforms, ?string $title): array
    {
        $values = $platforms;
        if (is_string($title) && trim($title) !== '') {
            $values[] = $title;
        }

        return collect($values)
            ->filter(fn ($v) => is_string($v) && trim($v) !== '')
            ->map(fn (string $v) => StringNormalizer::forLooseComparison($v))
            ->filter()
            ->values()
            ->all();
    }
}
