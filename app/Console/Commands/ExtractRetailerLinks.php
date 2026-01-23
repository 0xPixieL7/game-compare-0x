<?php

namespace App\Console\Commands;

use App\Models\VideoGame;
use App\Models\VideoGamePrice;
use App\Models\VideoGameTitleSource;
use Illuminate\Console\Command;
use Illuminate\Support\Str;

class ExtractRetailerLinks extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'prices:extract-retailers';

    /**
     * The console command description.
     *
     * @var string
     */
    protected $description = 'Extract Retailer links (Amazon, Steam, etc.) from IGDB raw payloads';

    /**
     * Execute the console command.
     */
    public function handle(\App\Services\CurrencyCountryService $currencyService)
    {
        $this->info('Scanning sources for Retailer links...');

        // Map domains to Retailer Names
        $retailerMap = [
            'amazon' => 'Amazon',
            'steampowered.com' => 'Steam',
            'epicgames.com' => 'Epic Games',
            'gog.com' => 'GOG',
            'playstation.com' => 'PlayStation Store',
            'xbox.com' => 'Xbox Store',
            'microsoft.com' => 'Xbox Store', // Often generic MS store links are for Xbox games
            'nintendo.com' => 'Nintendo eShop',
            'apple.com' => 'App Store',
            'google.com' => 'Google Play',
            'itch.io' => 'Itch.io',
        ];

        // Process in chunks
        VideoGameTitleSource::where('provider', 'igdb')
            ->whereNotNull('raw_payload')
            ->chunk(100, function ($sources) use ($retailerMap) {
                foreach ($sources as $source) {
                    $payload = $source->raw_payload;

                    // Handle double-encoded JSON
                    if (is_string($payload)) {
                        if (Str::startsWith($payload, '[') || Str::startsWith($payload, '{')) {
                            $decoded = json_decode($payload, true);
                            if (is_array($decoded)) {
                                $payload = $decoded;
                            }
                        }
                    }

                    if (! is_array($payload) || empty($payload['external_games'])) {
                        continue;
                    }

                    $externalGames = $payload['external_games'];

                    // Handle case where external_games is a JSON string
                    if (is_string($externalGames)) {
                        if (Str::startsWith($externalGames, '[') || Str::startsWith($externalGames, '{')) {
                            $decoded = json_decode($externalGames, true);
                            if (is_array($decoded)) {
                                $externalGames = $decoded;
                            }
                        }
                    }

                    if (! is_array($externalGames)) {
                        continue;
                    }

                    $videoGames = VideoGame::where('video_game_title_id', $source->video_game_title_id)->get();

                    if ($videoGames->isEmpty()) {
                        continue;
                    }

                    foreach ($externalGames as $ext) {
                        if (! is_array($ext) || empty($ext['url'])) {
                            continue;
                        }

                        $url = $ext['url'];
                        $retailer = null;

                        foreach ($retailerMap as $domain => $name) {
                            if (Str::contains($url, $domain)) {
                                $retailer = $name;
                                break;
                            }
                        }

                        if (! $retailer) {
                            continue;
                        }

                        // Determine region/country
                        $host = parse_url($url, PHP_URL_HOST);
                        $countryCode = null; // Default to null (Global) or implied by valid retailer

                        if ($retailer === 'Amazon') {
                            $countryCode = 'US'; // Default
                            if (Str::contains($host, '.co.uk')) $countryCode = 'UK';
                            elseif (Str::contains($host, '.fr')) $countryCode = 'FR';
                            elseif (Str::contains($host, '.de')) $countryCode = 'DE';
                            elseif (Str::contains($host, '.it')) $countryCode = 'IT';
                            elseif (Str::contains($host, '.es')) $countryCode = 'ES';
                            elseif (Str::contains($host, '.co.jp') || Str::contains($host, '.jp')) $countryCode = 'JP';
                            elseif (Str::contains($host, '.ca')) $countryCode = 'CA';
                            elseif (Str::contains($host, '.com.au')) $countryCode = 'AU';
                            elseif (Str::contains($host, '.com.br')) $countryCode = 'BR';
                            elseif (Str::contains($host, '.sa')) $countryCode = 'SA';
                            elseif (Str::contains($host, '.ae')) $countryCode = 'AE';
                            elseif (Str::contains($host, '.eg')) $countryCode = 'EG';
                            elseif (Str::contains($host, '.sg')) $countryCode = 'SG';
                            elseif (Str::contains($host, '.tr')) $countryCode = 'TR';
                            elseif (Str::contains($host, '.pl')) $countryCode = 'PL';
                            elseif (Str::contains($host, '.se')) $countryCode = 'SE';
                            elseif (Str::contains($host, '.nl')) $countryCode = 'NL';
                            elseif (Str::contains($host, '.com.be')) $countryCode = 'BE';
                        }

                        // Define target countries for global stores (Steam/Epic/GOG) to get price variance
                        $targetCountries = [$countryCode];
                        
                        // For Steam, we specifically want to track regional pricing variance
                        if ($retailer === 'Steam') {
                             // Add user-requested variance countries + majors (Egypt removed per request)
                             $targetCountries = array_unique(array_merge(
                                 array_filter([$countryCode]), 
                                 ['US', 'UK', 'DE', 'AR', 'UY', 'NZ', 'KR', 'UA', 'RU', 'SA', 'AE', 'ZA', 'TR', 'IN', 'BR']
                             ));
                        }

                        foreach ($videoGames as $game) {
                            foreach ($targetCountries as $targetCountry) {
                                // Skip if targetCountry is null (unless specialized logic handled loop)
                                if (!$targetCountry) continue;

                                $this->line("Found {$retailer} link for {$game->name} ({$targetCountry}): {$url}");

                                $price = VideoGamePrice::where('video_game_id', $game->id)
                                    ->where('retailer', $retailer)
                                    ->where('country_code', $targetCountry)
                                    ->first();

                                // Get currency from service using database
                                $currency = $currencyService->getCurrencyForCountry($targetCountry);
                                
                                // Detect Amazon product variants from URL (Digital vs Physical, editions)
                                $metadata = ['source' => 'igdb_external_games'];
                                
                                if ($retailer === 'Amazon') {
                                    // Amazon URLs sometimes indicate product type in the title or ASIN patterns
                                    // B01XXXX = Physical, B07XXXX/B08XXXX = Often Digital
                                    // Also check URL path for keywords
                                    $urlLower = strtolower($url);
                                    
                                    if (str_contains($urlLower, 'digital') || str_contains($urlLower, 'download')) {
                                        $metadata['variant'] = 'digital';
                                    } elseif (str_contains($urlLower, 'physical') || str_contains($urlLower, 'disc')) {
                                        $metadata['variant'] = 'physical';
                                    }
                                    
                                    // Detect editions
                                    if (str_contains($urlLower, 'deluxe')) {
                                        $metadata['edition'] = 'deluxe';
                                    } elseif (str_contains($urlLower, 'ultimate')) {
                                        $metadata['edition'] = 'ultimate';
                                    } elseif (str_contains($urlLower, 'standard')) {
                                        $metadata['edition'] = 'standard';
                                    }
                                    
                                    // Extract ASIN for reference
                                    if (preg_match('/\/dp\/([A-Z0-9]{10})/', $url, $matches)) {
                                        $metadata['asin'] = $matches[1];
                                    }
                                }

                                try {
                                    if ($price) {
                                         $price->url = $url;
                                         $price->currency = $currency;
                                         $price->metadata = $metadata;
                                         $price->updated_at = now();
                                         $price->save();
                                    } else {
                                        VideoGamePrice::create([
                                            'video_game_id' => $game->id,
                                            'retailer' => $retailer,
                                            'country_code' => $targetCountry,
                                            'url' => $url,
                                            'is_active' => true,
                                            'currency' => $currency,
                                            'amount_minor' => -1,
                                            'recorded_at' => now(),
                                            'metadata' => $metadata,
                                        ]);
                                    }
                                } catch (\Exception $e) {
                                    $this->error("Failed to save price for {$game->name} ({$retailer}-{$targetCountry}): " . $e->getMessage());
                                }
                            }
                        }
                    }
                }
            });

        $this->info('Retailer link extraction complete.');
    }
}
