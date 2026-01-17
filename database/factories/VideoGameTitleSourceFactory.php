<?php

declare(strict_types=1);

namespace Database\Factories;

use App\Models\VideoGameSource;
use App\Models\VideoGameTitle;
use App\Models\VideoGameTitleSource;
use Illuminate\Database\Eloquent\Factories\Factory;

/**
 * @extends Factory<VideoGameTitleSource>
 */
class VideoGameTitleSourceFactory extends Factory
{
    protected $model = VideoGameTitleSource::class;

    public function definition(): array
    {
        return [
            'video_game_title_id' => VideoGameTitle::factory(),
            'video_game_source_id' => VideoGameSource::factory(),
            'provider' => 'igdb',
            'provider_item_id' => (string) $this->faker->numberBetween(1, 50_000_000),
            'raw_payload' => null,
        ];
    }
}
