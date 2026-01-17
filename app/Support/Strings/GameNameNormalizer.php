<?php

namespace App\Support\Strings;

use Illuminate\Support\Str;

class GameNameNormalizer
{
    public static function normalize(?string $name): ?string
    {
        if (! is_string($name) || trim($name) === '') {
            return null;
        }

        $clean = preg_replace('/(\[[^\]]*\]|\([^)]*\))/u', ' ', $name) ?? $name;

        $normalized = Str::of($clean)
            ->ascii()
            ->lower()
            ->replaceMatches('/[^a-z0-9]+/u', ' ')
            ->squish()
            ->value();

        return $normalized !== '' ? $normalized : null;
    }
}
