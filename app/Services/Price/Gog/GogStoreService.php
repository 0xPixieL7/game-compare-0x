<?php

declare(strict_types=1);

namespace App\Services\Price\Gog;

use Illuminate\Support\Facades\Http;
use Illuminate\Support\Facades\Log;

class GogStoreService
{
    /**
     * Get price from GOG.com
     * API: https://api.gog.com/products/{id}?expand=downloads,expanded_dlcs,related_products,in_series,description,screenshots,videos,related_pages,changelog,z_reviews,critic_reviews
     * or scraper via https://www.gog.com/en/game/{slug}
     */
    public function getPrice(string $url, string $countryCode = 'US'): ?array
    {
        // GOG URLs are usually https://www.gog.com/en/game/cyberpunk_2077
        if (!preg_match('/\/game\/([^\/\?]+)/', $url, $matches)) {
            return null;
        }
        $slug = $matches[1];

        // Public API for prices: 
        // https://api.gog.com/v2/games/{id} - requires ID, not slug.
        // We need to resolve Slug to ID first, or use the store page JSON.
        
        // Easier: GOG Store API by array of IDs, or scraping the product page JSON.
        // https://catalog.gog.com/v1/catalog?limit=20&slug={slug}
        
        $apiUrl = "https://catalog.gog.com/v1/catalog?limit=1&slug={$slug}";
        
        try {
             $response = Http::get($apiUrl);
             $data = $response->json();
             
             if (empty($data['products'])) {
                 return null;
             }
             
             $product = $data['products'][0];
             $price = $product['price'];
             
             // GOG returns price objects
             // "price": { "final": "49.99", "base": "49.99", "discount": 0, "finalMoney": { "amount": "49.99", "currency": "USD" } }
             
             $amount = $price['finalMoney']['amount'] ?? $price['final'];
             $currency = $price['finalMoney']['currency'] ?? 'USD';
             
             // Convert to minor units. GOG API usually returns float strings "49.99".
             // Check if already in minor? "finalMoney" usually has "amount": "49.99".
             
             $minor = (int)(floatval($amount) * 100);
             
             return [
                 'amount_minor' => $minor,
                 'currency' => $currency,
             ];
             
        } catch (\Exception $e) {
            Log::error("GogStoreService: " . $e->getMessage());
            return null;
        }
    }
}
