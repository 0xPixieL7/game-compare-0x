<?php

namespace Database\Seeders;

use App\Models\VideoGameSource;
use Illuminate\Database\Seeder;
use Illuminate\Support\Str;

class VideoGameSourceSeeder extends Seeder
{
    public function run(): void
    {
        $sources = [
            [
                'id' => 1,
                'provider' => 'igdb',
                'provider_key' => 'igdb',
                'display_name' => 'IGDB',
                'category' => 'metadata',
                'slug' => 'igdb',
            ],
            [
                'id' => 2,
                'provider' => 'steam',
                'provider_key' => 'steam',
                'display_name' => 'Steam',
                'category' => 'store',
                'slug' => 'steam',
            ],
            [
                'id' => 3,
                'provider' => 'playstation_store',
                'provider_key' => 'playstation_store',
                'display_name' => 'PlayStation Store',
                'category' => 'store',
                'slug' => 'ps-store',
            ],
            [
                'id' => 4,
                'provider' => 'xbox_store',
                'provider_key' => 'xbox_store',
                'display_name' => 'Xbox Store',
                'category' => 'store',
                'slug' => 'xbox-store',
            ],
            [
                'id' => 5,
                'provider' => 'nintendo_eshop',
                'provider_key' => 'nintendo_eshop',
                'display_name' => 'Nintendo eShop',
                'category' => 'store',
                'slug' => 'nintendo-eshop',
            ],
        ];

        foreach ($sources as $source) {
            VideoGameSource::updateOrCreate(
                ['id' => $source['id']], // Force specific ID
                $source
            );
        }
        
        $this->command->info('✅ Seeded ' . count($sources) . ' video game sources');
    }
}
