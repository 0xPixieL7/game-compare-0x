<?php

declare(strict_types=1);

use App\Services\Normalization\PlatformNormalizer;

it('normalizes common platform variants with Jaro-Winkler and preserves numeric distinctions', function () {
    $n = app(PlatformNormalizer::class);

    expect($n->normalizeOne('PS5'))->toBe('PlayStation 5');
    expect($n->normalizeOne('Playstation5'))->toBe('PlayStation 5');
    expect($n->normalizeOne('PAL-PlayStation 5'))->toBe('PlayStation 5');

    expect($n->normalizeOne('PS4'))->toBe('PlayStation 4');
    expect($n->normalizeOne('PlayStation 4'))->toBe('PlayStation 4');

    // Numeric distinctions must not collapse across generations.
    expect($n->normalizeOne('PlayStation 4'))->not->toBe('PlayStation 5');
});

it('splits composite platform strings and returns unique normalized values', function () {
    $n = app(PlatformNormalizer::class);

    $out = $n->normalizeMany([
        'Xbox Series X|S',
        'PC',
        'windows',
        'Nintendo Switch',
        'Switch',
    ]);

    expect($out)->toContain('Xbox Series X');
    expect($out)->toContain('Xbox Series S');
    expect($out)->toContain('PC');
    expect($out)->toContain('Nintendo Switch');

    // Dedupes synonyms.
    expect(array_values(array_filter($out, fn ($p) => $p === 'PC')))->toHaveCount(1);
    expect(array_values(array_filter($out, fn ($p) => $p === 'Nintendo Switch')))->toHaveCount(1);
});
