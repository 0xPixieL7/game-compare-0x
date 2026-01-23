<?php

declare(strict_types=1);

namespace App\Services\Price\Amazon;

use Illuminate\Support\Facades\Http;
use Illuminate\Support\Facades\Log;
use Symfony\Component\DomCrawler\Crawler;

class AmazonScraperService
{
    /**
     * Scrape price from an Amazon product page.
     *
     * @param  string  $url  The Amazon product URL
     * @param  string  $countryCode  The 2-letter country code (US, UK, etc.)
     * @return array{currency:string, amount_minor:int}|null
     */
    public function getPrice(string $url, string $countryCode = 'US'): ?array
    {
        try {
            // 1. Fetch the page with browser-like headers
            $response = Http::withHeaders([
                'User-Agent' => 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept' => 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
                'Accept-Language' => 'en-US,en;q=0.9',
                'Cache-Control' => 'no-cache',
                'Pragma' => 'no-cache',
            ])->get($url);

            if ($response->failed()) {
                Log::warning("AmazonScraperService: Failed to fetch URL: {$url}");

                return null;
            }

            $html = $response->body();
            $crawler = new Crawler($html);

            // 2. Try various selectors for price
            // Common storage for found price components
            $whole = null;
            $fraction = null;
            $symbol = null;

            // Selector Strategy A: Standard "a-price" structure
            // <span class="a-price"><span class="a-offscreen">$59.99</span><span aria-hidden="true"><span class="a-price-symbol">$</span><span class="a-price-whole">59<span class="a-price-decimal">.</span></span><span class="a-price-fraction">99</span></span></span>

            // Try to find the "Apex Price" (buybox price)
            $priceNode = $crawler->filter('#corePrice_feature_div .a-price, #corePriceDisplay_desktop_feature_div .a-price, .a-price')->first();

            if ($priceNode->count() > 0) {
                // Try to get whole and fraction parts
                $wholeNode = $priceNode->filter('.a-price-whole');
                $fractionNode = $priceNode->filter('.a-price-fraction');
                $symbolNode = $priceNode->filter('.a-price-symbol');

                if ($wholeNode->count() > 0) {
                    $whole = $this->cleanNumber($wholeNode->text());
                }
                if ($fractionNode->count() > 0) {
                    $fraction = $this->cleanNumber($fractionNode->text());
                }
                if ($symbolNode->count() > 0) {
                    $symbol = trim($symbolNode->text());
                }
            }

            // Fallback Strategy B: Twister price / older layouts
            if ($whole === null) {
                $checkIds = ['#priceblock_ourprice', '#priceblock_dealprice', '#priceblock_saleprice', '.apexPriceToPay .a-offscreen'];
                foreach ($checkIds as $selector) {
                    $node = $crawler->filter($selector);
                    if ($node->count() > 0) {
                        $text = trim($node->text());
                        // Parse "$59.99" string
                        if (preg_match('/([0-9.,]+)/', $text, $matches)) {
                            // This is a rough parse, usually currency aware libraries are better but keeping it simple for now
                            $parts = explode('.', str_replace(',', '.', $matches[1])); // normalize comams
                            if (count($parts) > 1) {
                                $whole = $parts[0];
                                $fraction = $parts[1];
                            } else {
                                $whole = $parts[0];
                                $fraction = '00';
                            }
                            // Detect currency symbol
                            if (str_contains($text, '$')) {
                                $symbol = '$';
                            }
                            if (str_contains($text, '€')) {
                                $symbol = '€';
                            }
                            if (str_contains($text, '£')) {
                                $symbol = '£';
                            }
                            if (str_contains($text, '¥')) {
                                $symbol = '¥';
                            }
                            break;
                        }
                    }
                }
            }

            if ($whole === null) {
                Log::info("AmazonScraperService: Could not find price on page: {$url}");

                return null;
            }

            // 3. Construct Final Price
            $fraction = $fraction ?? '00';
            $amountMinor = ((int) $whole * 100) + (int) $fraction;

            // Japenese Yen exception (no fractions usually)
            if ($countryCode === 'JP') {
                $amountMinor = (int) $whole;
            }

            return [
                'amount_minor' => $amountMinor,
                'currency' => $this->mapSymbolToCurrency($symbol, $countryCode),
            ];

        } catch (\Exception $e) {
            Log::error('AmazonScraperService: Exception extracting price: '.$e->getMessage());

            return null;
        }
    }

    private function cleanNumber(string $text): string
    {
        // Remove non-numeric characters (except maybe dots if needed, but here we want integer parts)
        return preg_replace('/[^0-9]/', '', $text);
    }

    private function mapSymbolToCurrency(?string $symbol, string $countryCode): string
    {
        // If symbol is ambiguous ($), rely on country code
        if ($symbol === '€') {
            return 'EUR';
        }
        if ($symbol === '£') {
            return 'GBP';
        }
        if ($symbol === '¥') {
            return 'JPY';
        }

        return match ($countryCode) {
            'UK' => 'GBP',
            'JP' => 'JPY',
            'FR', 'DE', 'IT', 'ES', 'NL' => 'EUR',
            'CA' => 'CAD',
            'AU' => 'AUD',
            'BR' => 'BRL',
            default => 'USD',
        };
    }
}
