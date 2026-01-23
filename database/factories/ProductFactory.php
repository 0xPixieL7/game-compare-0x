<?php

declare(strict_types=1);

namespace Database\Factories;

use Illuminate\Database\Eloquent\Factories\Factory;

/**
 * @extends \Illuminate\Database\Eloquent\Factories\Factory<\App\Models\Product>
 */
class ProductFactory extends Factory
{
    /**
     * Define the model's default state.
     *
     * @return array<string, mixed>
     */
    public function definition(): array
    {
        $title = fake()->words(3, true);

        return [
            'name' => $title,
            'slug' => \Illuminate\Support\Str::slug($title).'-'.fake()->randomNumber(5),
            'type' => 'video_game',
            'title' => $title,
            'normalized_title' => \Illuminate\Support\Str::slug($title),
            'synopsis' => fake()->optional()->paragraph(),
        ];
    }
}
