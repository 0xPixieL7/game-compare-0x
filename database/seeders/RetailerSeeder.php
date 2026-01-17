<?php

namespace Database\Seeders;

use App\Models\Retailer;
use Illuminate\Database\Seeder;

class RetailerSeeder extends Seeder
{
    public function run(): void
    {
        $retailers = [
            [
                'name' => 'Steam',
                'slug' => 'steam',
                'base_url' => 'https://store.steampowered.com',
                'domain_matcher' => 'steampowered.com',
                'config' => ['type' => 'steam'],
            ],
            [
                'name' => 'Amazon',
                'slug' => 'amazon',
                'base_url' => 'https://www.amazon.com',
                'domain_matcher' => 'amazon.com',
                'config' => ['type' => 'amazon'],
            ],
            [
                'name' => 'GOG',
                'slug' => 'gog',
                'base_url' => 'https://www.gog.com',
                'domain_matcher' => 'gog.com',
                'config' => ['type' => 'gog'],
            ],
            [
                'name' => 'PlayStation Store',
                'slug' => 'ps-store',
                'base_url' => 'https://store.playstation.com',
                'domain_matcher' => 'store.playstation.com',
                'config' => ['type' => 'ps_store'],
            ],
            [
                'name' => 'Epic Games Store',
                'slug' => 'epic-games',
                'base_url' => 'https://store.epicgames.com',
                'domain_matcher' => 'epicgames.com',
                'config' => ['type' => 'epic'],
            ],
            [
                'name' => 'Xbox Store',
                'slug' => 'xbox-store',
                'base_url' => 'https://www.xbox.com',
                'domain_matcher' => 'xbox.com',
                'config' => ['type' => 'xbox'],
            ],
            [
                'name' => 'Nintendo eShop',
                'slug' => 'nintendo-eshop',
                'base_url' => 'https://www.nintendo.com',
                'domain_matcher' => 'nintendo.com',
                'config' => ['type' => 'nintendo'],
            ],
        ];

        foreach ($retailers as $retailer) {
            Retailer::updateOrCreate(
                ['slug' => $retailer['slug']],
                $retailer
            );
        }
    }
}
