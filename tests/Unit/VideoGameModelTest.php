<?php

declare(strict_types=1);

use App\Models\VideoGame;

it('does not treat price snapshot fields as video_games columns', function () {
    $game = new VideoGame;

    expect($game->getFillable())->not->toContain('currency');
    expect($game->getFillable())->not->toContain('amount_minor');
    expect($game->getFillable())->not->toContain('recorded_at');
    expect($game->getFillable())->not->toContain('retailer');
    expect($game->getFillable())->not->toContain('tax_inclusive');
});
