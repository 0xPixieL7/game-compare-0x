<?php

declare(strict_types=1);

namespace Database\Factories;

use App\Models\VideoGameTitle;
use Illuminate\Database\Eloquent\Factories\Factory;

/**
 * @extends Factory<VideoGameTitle>
 */
class VideoGamesFactory extends Factory
{
    protected $model = VideoGameTitle::class;

    /**
     * Define the model's default state.
     *
     * @return array<string, mixed>
     */
    public function definition(): array
    {
        return VideoGameTitle::factory()->definition();
    }
}
