<?php

declare(strict_types=1);

namespace App\Support\Strings;

final class StringNormalizer
{
    /**
     * Normalize a string for loose comparisons across punctuation, whitespace, and case.
     *
     * Examples:
     * - "PlayStation_5" -> "playstation5"
     * - "playsttaion-5" -> "playsttaion5"
     * - "Play Station Store" -> "playstationstore"
     */
    public static function forLooseComparison(?string $value): string
    {
        if ($value === null) {
            return '';
        }

        $value = trim($value);
        if ($value === '') {
            return '';
        }

        $value = mb_strtolower($value);
        $value = preg_replace('/[^a-z0-9]+/u', '', $value) ?? '';

        return $value;
    }
}
