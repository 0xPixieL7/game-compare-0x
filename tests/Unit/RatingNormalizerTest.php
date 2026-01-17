<?php

declare(strict_types=1);

use App\Services\Normalization\RatingNormalizer;

it('maps rating aliases and normalizes to 0-5', function () {
    $n = app(RatingNormalizer::class);

    // 0-5 stars stay on the same scale
    expect($n->extractNormalizedRating(['user_ratings' => 4.5]))->toEqualWithDelta(4.5, 0.0001);

    // 0-100 -> 0-5
    expect($n->extractNormalizedRating(['aggregated_rating' => 88]))->toEqualWithDelta(4.4, 0.0001);

    // 0-10 -> 0-5
    expect($n->extractNormalizedRating(['rating' => 9.2]))->toEqualWithDelta(4.6, 0.0001);

    // String star ratings.
    expect($n->extractNormalizedRating(['product_star_rating' => '4.2 stars']))->toEqualWithDelta(4.2, 0.0001);
    expect($n->extractNormalizedRating(['product_star_rating' => '4/5 stars']))->toEqualWithDelta(4.0, 0.0001);
});

it('extracts rating count from known aliases', function () {
    $n = app(RatingNormalizer::class);

    expect($n->extractRatingCount(['rating_count' => 12]))->toBe(12);
    expect($n->extractRatingCount(['total_rating_count' => '34']))->toBe(34);
    expect($n->extractRatingCount(['aggregated_rating_count' => null]))->toBeNull();
});

it('falls back when earlier alias is an empty string (CSV behavior)', function () {
    $n = app(RatingNormalizer::class);

    // IGDB CSV includes `rating`, `aggregated_rating`, and `total_rating` headers.
    // When `rating` is present but empty, we must still consider `total_rating`.
    expect($n->extractNormalizedRating([
        'rating' => '',
        'total_rating' => '80',
    ]))->toEqualWithDelta(4.0, 0.0001);

    // Same idea for counts: a blank rating_count must not block total_rating_count.
    expect($n->extractRatingCount([
        'rating_count' => '',
        'total_rating_count' => '123',
    ]))->toBe(123);
});
