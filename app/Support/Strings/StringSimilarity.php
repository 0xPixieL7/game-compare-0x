<?php

declare(strict_types=1);

namespace App\Support\Strings;

final class StringSimilarity
{
    /**
     * Jaro-Winkler similarity in range [0, 1].
     *
     * Implemented in pure PHP to avoid relying on extensions.
     */
    public static function jaroWinkler(string $s1, string $s2, float $scalingFactor = 0.1): float
    {
        $s1 = (string) $s1;
        $s2 = (string) $s2;

        if ($s1 === $s2) {
            return 1.0;
        }

        $len1 = strlen($s1);
        $len2 = strlen($s2);

        if ($len1 === 0 || $len2 === 0) {
            return 0.0;
        }

        $matchDistance = (int) floor(max($len1, $len2) / 2) - 1;
        if ($matchDistance < 0) {
            $matchDistance = 0;
        }

        $s1Matches = array_fill(0, $len1, false);
        $s2Matches = array_fill(0, $len2, false);

        $matches = 0;

        for ($i = 0; $i < $len1; $i++) {
            $start = max(0, $i - $matchDistance);
            $end = min($i + $matchDistance + 1, $len2);

            for ($j = $start; $j < $end; $j++) {
                if ($s2Matches[$j]) {
                    continue;
                }

                if ($s1[$i] !== $s2[$j]) {
                    continue;
                }

                $s1Matches[$i] = true;
                $s2Matches[$j] = true;
                $matches++;
                break;
            }
        }

        if ($matches === 0) {
            return 0.0;
        }

        // Count transpositions
        $k = 0;
        $transpositions = 0;

        for ($i = 0; $i < $len1; $i++) {
            if (! $s1Matches[$i]) {
                continue;
            }

            while ($k < $len2 && ! $s2Matches[$k]) {
                $k++;
            }

            if ($k < $len2 && $s1[$i] !== $s2[$k]) {
                $transpositions++;
            }

            $k++;
        }

        $transpositions = (int) floor($transpositions / 2);

        $jaro = (
            ($matches / $len1)
            + ($matches / $len2)
            + (($matches - $transpositions) / $matches)
        ) / 3.0;

        // Winkler adjustment
        $prefix = 0;
        $maxPrefix = 4;
        $limit = min($maxPrefix, min($len1, $len2));

        for ($i = 0; $i < $limit; $i++) {
            if ($s1[$i] === $s2[$i]) {
                $prefix++;
            } else {
                break;
            }
        }

        $scalingFactor = max(0.0, min(0.25, $scalingFactor));

        return $jaro + ($prefix * $scalingFactor * (1.0 - $jaro));
    }
}
