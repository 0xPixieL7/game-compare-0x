<?php

namespace App\Console\Commands;

use App\Services\Media\RAWG\RawgService;
use Illuminate\Console\Command;

class FetchTopGamesCommand extends Command
{
    protected $signature = 'rawg:top-games {--year=2024,2025 : Comma-separated years} {--limit=10 : Number of games per year}';
    protected $description = 'Fetch top-rated games from RAWG for specified years with full data in one call';

    public function handle(RawgService $rawg)
    {
        $years = explode(',', $this->option('year'));
        $limit = (int) $this->option('limit');

        $this->info('🎮 Fetching Top-Rated Games from RAWG');
        $this->newLine();

        foreach ($years as $year) {
            $year = trim($year);
            $this->info("📅 Top {$limit} Games of {$year}");
            $this->line(str_repeat('─', 80));

            // Fetch top-rated games for this year
            $response = $rawg->getAllGames([
                'dates' => "{$year}-01-01,{$year}-12-31",
                'ordering' => '-rating',
            ], page: 1, pageSize: $limit);

            if (empty($response['results'])) {
                $this->warn("No games found for {$year}");
                $this->newLine();
                continue;
            }

            $this->line("Total games in {$year}: " . number_format($response['count']));
            $this->newLine();

            // Display summary table
            $tableData = [];
            foreach ($response['results'] as $game) {
                $tableData[] = [
                    $game['name'],
                    $game['released'] ?? 'TBA',
                    $game['rating'] ?? 'N/A',
                    $game['metacritic'] ?? 'N/A',
                    count($game['short_screenshots'] ?? []),
                    isset($game['clip']) ? '✅' : '❌',
                    isset($game['background_image']) ? '✅' : '❌',
                ];
            }

            $this->table(
                ['Game', 'Released', 'Rating', 'Metacritic', 'Screenshots', 'Video', 'Cover'],
                $tableData
            );

            $this->newLine();

            // Show detailed data for first game as example
            if ($this->confirm("Show detailed data for top game?", true)) {
                $topGame = $response['results'][0];
                $this->showGameDetails($topGame);
            }

            $this->newLine(2);
        }

        return self::SUCCESS;
    }

    private function showGameDetails(array $game)
    {
        $this->newLine();
        $this->info("📊 Detailed Data for: {$game['name']}");
        $this->line(str_repeat('═', 80));

        // Basic Info
        $this->line("🎮 <fg=cyan>Basic Information</>");
        $this->line("   ID: {$game['id']}");
        $this->line("   Slug: {$game['slug']}");
        $this->line("   Released: " . ($game['released'] ?? 'TBA'));
        $this->line("   Rating: {$game['rating']}/5 ({$game['ratings_count']} ratings)");
        $this->line("   Metacritic: " . ($game['metacritic'] ?? 'N/A'));
        $this->line("   Playtime: " . ($game['playtime'] ?? 0) . " hours avg");
        $this->newLine();

        // Platforms
        $this->line("💻 <fg=cyan>Platforms</>");
        $platforms = collect($game['platforms'] ?? [])->map(fn($p) => $p['platform']['name'] ?? 'Unknown')->take(5);
        $this->line("   " . $platforms->implode(', '));
        $this->newLine();

        // Genres
        $this->line("🎭 <fg=cyan>Genres</>");
        $genres = collect($game['genres'] ?? [])->map(fn($g) => $g['name'])->implode(', ');
        $this->line("   " . ($genres ?: 'N/A'));
        $this->newLine();

        // Developers & Publishers
        $this->line("👥 <fg=cyan>Developers</>");
        $devs = collect($game['developers'] ?? [])->map(fn($d) => $d['name'])->implode(', ');
        $this->line("   " . ($devs ?: 'N/A'));
        
        $this->line("🏢 <fg=cyan>Publishers</>");
        $pubs = collect($game['publishers'] ?? [])->map(fn($p) => $p['name'])->implode(', ');
        $this->line("   " . ($pubs ?: 'N/A'));
        $this->newLine();

        // Media
        $this->newLine();
        $this->line("📸 <fg=cyan>Media Assets - Hero & Background Images</>");
        
        // Cover/Hero Image (Primary)
        if (isset($game['background_image'])) {
            $this->line("   🦸 <fg=green>Hero Image (Primary Background)</>");
            $this->line("      {$game['background_image']}");
        } else {
            $this->line("   🦸 Hero Image: ❌ Not available");
        }
        
        // Additional Background/Character Art
        if (isset($game['background_image_additional'])) {
            $this->newLine();
            $this->line("   🌄 <fg=green>Additional Background/Character Art</>");
            $this->line("      {$game['background_image_additional']}");
        }
        
        // Screenshots (minimal - just 1 for reference)
        $this->newLine();
        $screenshotCount = count($game['short_screenshots'] ?? []);
        if ($screenshotCount > 0) {
            $this->line("   📷 <fg=yellow>Reference Screenshot</> (1 of {$screenshotCount} available)");
            $this->line("      {$game['short_screenshots'][0]['image']}");
        }
        
        // Video Clip
        $this->newLine();
        $this->line("🎬 <fg=cyan>Media Assets - Videos</>");
        if (isset($game['clip'])) {
            $this->line("   ✅ <fg=green>Video Clip Available</>");
            if (isset($game['clip']['preview'])) {
                $this->line("      Preview: {$game['clip']['preview']}");
            }
            if (isset($game['clip']['clips'])) {
                $qualities = array_keys($game['clip']['clips']);
                $this->line("      Available qualities: " . implode(', ', $qualities));
                // Show only highest quality URL
                $highestQuality = end($qualities);
                $this->line("      Best quality ({$highestQuality}): {$game['clip']['clips'][$highestQuality]}");
            }
        } else {
            $this->line("   ❌ No video clip available");
        }
        
        $this->newLine();

        // Stores
        if (!empty($game['stores'])) {
            $this->line("🛒 <fg=cyan>Available at Stores</>");
            foreach (array_slice($game['stores'], 0, 5) as $store) {
                $storeName = $store['store']['name'] ?? 'Unknown';
                $this->line("   • {$storeName}");
            }
            $this->newLine();
        }

        // Tags (top 5)
        if (!empty($game['tags'])) {
            $this->line("🏷️  <fg=cyan>Top Tags</>");
            $tags = collect($game['tags'])->take(5)->map(fn($t) => $t['name'])->implode(', ');
            $this->line("   " . $tags);
            $this->newLine();
        }

        // Links
        $this->line("🔗 <fg=cyan>Links</>");
        if (isset($game['website'])) {
            $this->line("   Website: {$game['website']}");
        }
        if (isset($game['reddit_url'])) {
            $this->line("   Reddit: {$game['reddit_url']}");
        }
        $this->newLine();

        // Show JSON option
        if ($this->confirm("Show full JSON response?", false)) {
            $this->line(json_encode($game, JSON_PRETTY_PRINT));
        }
    }
}
