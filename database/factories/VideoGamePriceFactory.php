<?php

declare(strict_types=1);

namespace Database\Factories;

use App\Models\VideoGame;
use App\Models\VideoGamePrice;
use Illuminate\Database\Eloquent\Factories\Factory;

/**
 * @extends Factory<VideoGamePrice>
 */
class VideoGamePriceFactory extends Factory
{
    protected $model = VideoGamePrice::class;

    public function definition(): array
    {
        return [
            'video_game_id' => VideoGame::factory(),
            'currency' => 'USD',
            'amount_minor' => $this->faker->numberBetween(999, 69_999),
            'recorded_at' => now(),
            'retailer' => $this->faker->optional()->company(),
            'country_code' => 'US',
            'tax_inclusive' => $this->faker->boolean(30),
        ];
    }
}
