<?php

namespace App\Console\Commands;

use Illuminate\Console\Command;
use Illuminate\Support\Facades\Http;

class UpdateSteamAppList extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'steam:update-app-list';

    /**
     * The console command description.
     *
     * @var string
     */
    protected $description = 'Update the local Steam App List (steam_apps_pretty.json) from the Steam Web API';

    /**
     * Execute the console command.
     */
    public function handle()
    {
        $this->info('Fetching App List from Steam API...');

        try {
            // Fetch the full app list
            // Original: http://api.steampowered.com/ISteamApps/GetAppList/v0002/?format=json (Deprecated/Requires key)
            // Using a reliable mirror because the official endpoint is flaky without an API Key.
            $response = Http::get('https://nebukam.github.io/steam-db/applist.json');

            if ($response->failed()) {
                $this->warn('Mirror failed or unreachable. Trying official endpoint (might fail)...');
                $response = Http::get('http://api.steampowered.com/ISteamApps/GetAppList/v0002/?format=json');
            }

            if ($response->failed()) {
                $this->error('Failed to fetch app list from Steam API and Mirror.');

                return 1;
            }

            $data = $response->json();
            $apps = $data['applist']['apps'] ?? [];

            if (empty($apps)) {
                $this->error('No apps returned from Steam API.');

                return 1;
            }

            $count = count($apps);
            $this->info("Fetched {$count} apps. Processing and saving...");

            // Process for "pretty" format (e.g., simplistic JSON array or line-based for generic streaming)
            // The existing service expects key "name" and "appid" (lowercase).
            // The API returns "appid" and "name".

            // We will save as a pretty-printed JSON file to match the expected format 'steam_apps_pretty.json'.
            // However, 200k+ items pretty printed is huge.
            // The existing code uses streaming search on this file.
            // To optimize, we'll sort by name length or just alphabetical to ensure consistency.

            // File content structure expected by `searchLocal`:
            // It reads line by line. It looks for "name": "..." and then looks for "appid": ... in the PREVIOUS line?
            // Wait, let's re-read SteamStoreService::searchLocal logic.
            // if (stripos($line, '"name":') !== false) {
            //      ...
            //      if (preg_match('/"appid":\s*(\d+)/', $prevLine, $idMatches)) ...
            // }
            // So the format MUST be:
            // {
            //    "appid": 123,
            //    "name": "Game"
            // },
            // ...
            // The appid must come BEFORE the name in the JSON object structure for that specific parser logic.

            // Let's ensure the order.
            $formattedApps = [];
            foreach ($apps as $app) {
                // Skip empty names
                if (empty($app['name'])) {
                    continue;
                }

                $formattedApps[] = [
                    'appid' => $app['appid'],
                    'name' => $app['name'],
                ];
            }

            // Save
            $json = json_encode($formattedApps, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES);

            // Validate the order in JSON string (appid first) varies by PHP version/implementation of json_encode usually preserves array order
            // but let's be sure.
            // The parser is fragile: it relies on 'appid' line being strictly `prevLine` to `name` line.
            // `json_encode` with `JSON_PRETTY_PRINT` usually outputs:
            // {
            //     "appid": 10,
            //     "name": "Counter-Strike"
            // },
            // This matches the expectation.

            $bytes = file_put_contents(base_path('steam_apps_pretty.json'), $json);

            $this->info("Successfully saved {$bytes} bytes to steam_apps_pretty.json");
            $this->info('Total Apps: '.count($formattedApps));

            return 0;

        } catch (\Exception $e) {
            $this->error('Error: '.$e->getMessage());

            return 1;
        }
    }
}
