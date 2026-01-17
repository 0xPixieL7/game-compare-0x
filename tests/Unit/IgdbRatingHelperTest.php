<?php

use App\Services\Normalization\IgdbRatingHelper;

beforeEach(function () {
    $this->helper = new IgdbRatingHelper;
});

describe('extractPercentage', function () {
    it('returns aggregated_rating when available (priority 1)', function () {
        $record = [
            'aggregated_rating' => 88.12345678,
            'total_rating' => 75.0,
            'rating' => 60.0,
        ];

        expect($this->helper->extractPercentage($record))->toBe(88.12345678);
    });

    it('falls back to total_rating when no aggregated_rating (priority 2)', function () {
        $record = [
            'aggregated_rating' => null,
            'total_rating' => 75.12345678,
            'rating' => 60.0,
        ];

        expect($this->helper->extractPercentage($record))->toBe(75.12345678);
    });

    it('falls back to rating when no total_rating (priority 3)', function () {
        $record = [
            'aggregated_rating' => null,
            'total_rating' => null,
            'rating' => 60.12345678,
        ];

        expect($this->helper->extractPercentage($record))->toBe(60.12345678);
    });

    it('returns null when no rating fields present', function () {
        $record = ['name' => 'Test Game'];

        expect($this->helper->extractPercentage($record))->toBeNull();
    });

    it('returns null when all rating fields are null', function () {
        $record = [
            'aggregated_rating' => null,
            'total_rating' => null,
            'rating' => null,
        ];

        expect($this->helper->extractPercentage($record))->toBeNull();
    });

    it('preserves 8 decimal places of precision', function () {
        $record = ['aggregated_rating' => 88.123456789];

        // Should round to 8 decimal places
        expect($this->helper->extractPercentage($record))->toBe(88.12345679);
    });

    it('clamps values above 100 to 100', function () {
        $record = ['aggregated_rating' => 150.0];

        expect($this->helper->extractPercentage($record))->toBe(100.0);
    });

    it('clamps values below 0 to 0', function () {
        $record = ['aggregated_rating' => -10.0];

        expect($this->helper->extractPercentage($record))->toBe(0.0);
    });

    it('handles string numeric values from CSV', function () {
        $record = ['aggregated_rating' => '88.12345678'];

        expect($this->helper->extractPercentage($record))->toBe(88.12345678);
    });

    it('returns null for empty string values', function () {
        $record = [
            'aggregated_rating' => '',
            'total_rating' => '',
            'rating' => '',
        ];

        expect($this->helper->extractPercentage($record))->toBeNull();
    });

    it('skips empty string and uses next available rating', function () {
        $record = [
            'aggregated_rating' => '',
            'total_rating' => 75.5,
            'rating' => 60.0,
        ];

        expect($this->helper->extractPercentage($record))->toBe(75.5);
    });

    it('returns null for non-numeric values', function () {
        $record = ['aggregated_rating' => 'not-a-number'];

        expect($this->helper->extractPercentage($record))->toBeNull();
    });

    it('returns null for infinite values', function () {
        $record = ['aggregated_rating' => INF];

        expect($this->helper->extractPercentage($record))->toBeNull();
    });
});

describe('extractRatingCount', function () {
    it('returns aggregated_rating_count when available (priority 1)', function () {
        $record = [
            'aggregated_rating_count' => 1500,
            'total_rating_count' => 1000,
            'rating_count' => 500,
        ];

        expect($this->helper->extractRatingCount($record))->toBe(1500);
    });

    it('falls back to total_rating_count when no aggregated_rating_count (priority 2)', function () {
        $record = [
            'aggregated_rating_count' => null,
            'total_rating_count' => 1000,
            'rating_count' => 500,
        ];

        expect($this->helper->extractRatingCount($record))->toBe(1000);
    });

    it('falls back to rating_count when no total_rating_count (priority 3)', function () {
        $record = [
            'aggregated_rating_count' => null,
            'total_rating_count' => null,
            'rating_count' => 500,
        ];

        expect($this->helper->extractRatingCount($record))->toBe(500);
    });

    it('returns null when no count fields present', function () {
        $record = ['name' => 'Test Game'];

        expect($this->helper->extractRatingCount($record))->toBeNull();
    });

    it('handles string numeric values from CSV', function () {
        $record = ['aggregated_rating_count' => '1500'];

        expect($this->helper->extractRatingCount($record))->toBe(1500);
    });

    it('returns null for negative values', function () {
        $record = ['aggregated_rating_count' => -10];

        expect($this->helper->extractRatingCount($record))->toBeNull();
    });

    it('returns zero for zero values', function () {
        $record = ['aggregated_rating_count' => 0];

        expect($this->helper->extractRatingCount($record))->toBe(0);
    });
});
