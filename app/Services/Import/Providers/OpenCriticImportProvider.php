<?php

declare(strict_types=1);

namespace App\Services\Import\Providers;

use App\Models\VideoGame;
use App\Services\Import\Concerns\HasProgressBar;
use App\Services\Import\Concerns\InteractsWithConsole;
use App\Services\Import\Contracts\ImportProvider;
use Illuminate\Console\Command;
use Illuminate\Support\Facades\Http;

class OpenCriticImportProvider implements ImportProvider
{
    use HasProgressBar;
    use InteractsWithConsole;

    private const PROVIDER_NAME = 'opencritic';

    private const OPENCRITIC_API_URL = 'https://api.opencritic.com/api/game/search';

    public function getName(): string
    {
        return self::PROVIDER_NAME;
    }

    public function handle(Command $command): int
    {
        $this->setCommand($command);

        $limit = (int) $command->option('limit');
        $force = (bool) $command->option('force');

        $this->info('Starting OpenCritic Import...');

        $this->startOptimizedImport();

        $query = VideoGame::query()->whereNotNull('name');

        if (! $force) {
            $query->whereNull('opencritic_score');
        }

        if ($limit > 0) {
            $query->limit($limit);
        }

        $total = $query->count();
        $this->info("Found {$total} games to process.");

        $progressBar = $this->command->getOutput()->createProgressBar($total);
        $this->configureProgressBar($progressBar);
        $progressBar->start();

        // Process in chunks of 10 for concurrency
        $query->chunk(10, function ($games) use ($progressBar) {
            $gamesMap = $games->keyBy('id');

            // 1. Parallel Search
            $responses = Http::pool(function (\Illuminate\Http\Client\Pool $pool) use ($games) {
                foreach ($games as $game) {
                    $pool->as((string) $game->id)->get(self::OPENCRITIC_API_URL, [
                        'criteria' => $game->name,
                    ]);
                }
            });

            $detailsToFetch = [];

            foreach ($responses as $gameId => $response) {
                $game = $gamesMap[$gameId] ?? null;
                if (! $game || $response->failed()) {
                    $progressBar->advance(); // Count as handled if failed here

                    continue;
                }

                $results = $response->json();
                if (empty($results)) {
                    $progressBar->advance();

                    continue;
                }

                // Find best match
                $bestMatch = null;
                foreach ($results as $result) {
                    if (strcasecmp($result['name'], $game->name) === 0) {
                        $bestMatch = $result;
                        break;
                    }
                }
                if (! $bestMatch && isset($results[0])) {
                    $bestMatch = $results[0];
                }

                if ($bestMatch) {
                    $detailsToFetch[$gameId] = $bestMatch['id'];
                } else {
                    $progressBar->advance();
                }
            }

            if (empty($detailsToFetch)) {
                return;
            }

            // 2. Parallel Details Fetch
            $detailResponses = Http::pool(function (\Illuminate\Http\Client\Pool $pool) use ($detailsToFetch) {
                foreach ($detailsToFetch as $gameId => $ocId) {
                    $pool->as((string) $gameId)->get("https://api.opencritic.com/api/game/{$ocId}");
                }
            });

            foreach ($detailResponses as $gameId => $response) {
                $game = $gamesMap[$gameId] ?? null;
                $ocId = $detailsToFetch[$gameId] ?? null;

                if ($game && $response->successful()) {
                    $data = $response->json();

                    $game->update([
                        'opencritic_id' => $ocId,
                        'opencritic_score' => $data['topCriticScore'] ?? null,
                        'opencritic_tier' => $data['tier'] ?? null,
                        'opencritic_review_count' => $data['numReviews'] ?? null,
                        'opencritic_percent_recommended' => $data['percentRecommended'] ?? null,
                        'opencritic_updated_at' => now(),
                    ]);
                }

                $progressBar->advance();
            }

            // Basic rate limit between chunks to be safe
            usleep(250000); // 0.25s
        });

        $progressBar->finish();
        $this->command->newLine();
        $this->info('OpenCritic Import Complete.');

        $this->endOptimizedImport();

        return Command::SUCCESS;
    }

    // Removed fetchAndSaveScore and fetchGameDetails as logic is inlined for concurrency
}
