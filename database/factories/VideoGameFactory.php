<?php

declare(strict_types=1);

namespace Database\Factories;

use App\Models\VideoGameTitle;
use Illuminate\Database\Eloquent\Factories\Factory;
use Illuminate\Support\Str;

/**
 * @extends \Illuminate\Database\Eloquent\Factories\Factory<\App\Models\VideoGame>
 */
class VideoGameFactory extends Factory
{
    /**
     * Configure the model factory.
     */
    public function configure(): static
    {
        return $this->afterCreating(function (\App\Models\VideoGame $videoGame) {
            \App\Events\GameFactoryInitialized::dispatch([$videoGame->id]);
        });
    }

    /**
     * Define the model's default state.
     *
     * @return array<string, mixed>
     */
    public function definition(): array
    {
        $platforms = ['PC', 'PlayStation 5', 'Xbox Series X|S', 'Nintendo Switch'];
        $platformCount = rand(1, 3);

        return [
            'video_game_title_id' => VideoGameTitle::factory(),
            'slug' => Str::slug(fake()->words(3, true)).'-'.fake()->randomNumber(5),
            'provider' => 'factory-provider',
            'external_id' => fake()->unique()->numberBetween(1, 9_999_999),
            'name' => function () {
                return \App\Services\DumpDataLoader::getGameName();
            },
            'storyline' => fake()->optional(0.7)->paragraph(),
            'hypes' => fake()->numberBetween(0, 10000),
            'follows' => fake()->numberBetween(0, 50000),
            'attributes' => [
                'genres' => fake()->randomElements(
                    ['Action', 'Adventure', 'RPG', 'Strategy', 'Simulation', 'Sports', 'Puzzle', 'Shooter', 'Fighting', 'Racing'],
                    fake()->numberBetween(1, 3)
                ),
                'modes' => fake()->randomElements(
                    ['Single player', 'Multiplayer', 'Co-op', 'Split screen', 'Online'],
                    fake()->numberBetween(1, 2)
                ),
            ],
            'platform' => fake()->randomElements($platforms, $platformCount),
            'rating' => fake()->optional()->randomFloat(2, 0, 100),
            'source_payload' => null,
        ];
    }
}
