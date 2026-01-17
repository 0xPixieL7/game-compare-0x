<?php

namespace Database\Seeders;

use Illuminate\Database\Seeder;
use Illuminate\Support\Facades\DB;

class VideoGameSeeder extends Seeder
{
    /**
     * Run the database seeds.
     */
    public function run(): void
    {
        // 1. Seed Video Game Sources
        $this->command->info('Seeding video_game_sources...');

        $sources = [
            ['provider' => 'igdb'],
            ['provider' => 'steam'],
            ['provider' => 'playstation_store'],
            ['provider' => 'xbox_store'],
            ['provider' => 'nexarda'],
            ['provider' => 'giantbomb'],
            ['provider' => 'itad'],
            ['provider' => 'gog'],
            ['provider' => 'epic_games_store'],
            ['provider' => 'nintendo_eshop'],
            ['provider' => 'rawg'],
        ];

        foreach ($sources as $source) {
            DB::table('video_game_sources')->upsert(
                [
                    'provider' => $source['provider'],
                    'created_at' => now(),
                    'updated_at' => now(),
                ],
                ['provider'], // Unique key
                ['updated_at']
            );
        }

        // 2. Report on existing data (The "49k games")
        $productCount = DB::table('products')->count();
        $titleCount = DB::table('video_game_titles')->count();
        $gameCount = DB::table('video_games')->count();

        $this->command->info('Database Status:');
        $this->command->info("- Products: $productCount");
        $this->command->info("- Video Game Titles: $titleCount");
        $this->command->info("- Video Games: $gameCount");

        if ($gameCount > 0 && $gameCount === $productCount) {
            $this->command->info('✅ Hierarchy appears fully populated and synchronized.');
        } else {
            $this->command->warn('⚠️  Discrepancy detected in hierarchy counts.');
        }

        // 3. (Optional) Advanced Repair: Ensure Titles have normalized_title if missing
        // Using "data on the database" to improve quality
        DB::statement('
            UPDATE video_game_titles 
            SET normalized_title = LOWER(name) 
            WHERE normalized_title IS NULL
        ');

        DB::statement('
            UPDATE products 
            SET normalized_title = LOWER(name) 
            WHERE normalized_title IS NULL
        ');

        $this->command->info('Normalized titles updated.');
    }
}
