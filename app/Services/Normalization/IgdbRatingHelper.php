<?php

declare(strict_types=1);

namespace App\Services\Normalization;

use Illuminate\Support\Arr;

/**
 * IGDB-specific rating helper that preserves the native 0-100 percentage scale.
 *
 * Unlike RatingNormalizer (which converts to 0-5 stars), this helper keeps
 * IGDB ratings on their original 0-100 scale with 8 decimal places precision.
 */
class IgdbRatingHelper
{
    /**
     * Extract IGDB rating as a 0-100 percentage with 8 decimal places precision.
     *
     * Priority order: aggregated_rating > total_rating > rating
     *
     * If popularity primitives (hypes, follows) are present, they can be used
     * to weight or adjust the rating for games with low rating counts.
     */
    public function extractPercentage(array $record): ?float
    {
        $rating = $this->firstNonNull($record, [
            'aggregated_rating',
            'total_rating',
            'rating',
        ]);

        if ($rating === null || $rating === '') {
            // If no rating exists, we could potentially derive a "popularity score"
            // but for now we return null to maintain data integrity.
            return null;
        }

        $value = (float) $rating;

        if (! is_finite($value)) {
            return null;
        }

        // Popularity-based weighting (Bayesian-ish)
        // If we have hypes/follows but very few ratings, the rating might be skewed.
        $hypes = (int) ($record['hypes'] ?? 0);
        $follows = (int) ($record['follows'] ?? 0);
        $count = $this->extractRatingCount($record) ?? 0;

        // Example: If a game has 1000 follows but only 2 ratings, we might trust the 2 ratings less.
        // For now, we just return the raw clamped value, but the primitives are available for future weighting.

        return round(max(0.0, min(100.0, $value)), 8);
    }

    /**
     * Extract rating count from IGDB record.
     */
    public function extractRatingCount(array $record): ?int
    {
        $candidate = $this->firstNonNull($record, [
            'aggregated_rating_count',
            'total_rating_count',
            'rating_count',
        ]);

        if (is_numeric($candidate)) {
            $value = (int) $candidate;

            return $value >= 0 ? $value : null;
        }

        return null;
    }

    private function firstNonNull(array $record, array $keys): mixed
    {
        foreach ($keys as $key) {
            $value = Arr::get($record, $key);

            if ($value === null) {
                continue;
            }

            // CSV payloads commonly represent missing values as empty strings.
            if (is_string($value) && trim($value) === '') {
                continue;
            }

            return $value;
        }

        return null;
    }
}
