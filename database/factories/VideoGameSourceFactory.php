<?php

declare(strict_types=1);

namespace Database\Factories;

use App\Models\VideoGameSource;
use Illuminate\Database\Eloquent\Factories\Factory;
use Illuminate\Support\Str;

/**
 * @extends Factory<VideoGameSource>
 */
class VideoGameSourceFactory extends Factory
{
    protected $model = VideoGameSource::class;

    public function definition(): array
    {
        $provider = 'provider_'.$this->faker->unique()->numberBetween(1, 1_000_000);

        return [
            'provider' => $provider,
            'provider_key' => $provider,
            'display_name' => Str::headline($provider),
            'category' => $this->faker->randomElement(['metadata', 'store', 'media', 'aggregator']),
            'slug' => Str::slug($provider),
            'external_id' => null,
            'metadata' => [],
            'video_game_ids' => [],
            'items_count' => 0,
        ];
    }
}
