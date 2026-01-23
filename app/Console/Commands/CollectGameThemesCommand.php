<?php

declare(strict_types=1);

namespace App\Console\Commands;

use App\Models\VideoGame;
use App\Services\Theme\ThemeArtifactService;
use Illuminate\Console\Command;

class CollectGameThemesCommand extends Command
{
    /**
     * @var string
     */
    protected $signature = 'games:collect-themes {--limit=100 : Number of games to process} {--force : Reprocess games that already have a theme}';

    /**
     * @var string
     */
    protected $description = 'Collect visual artifacts and themes for video games';

    public function handle(ThemeArtifactService $themeService): int
    {
        $query = VideoGame::query();

        if (!$this->option('force')) {
            $query->where(function ($q) {
                $q->whereNull('attributes->theme')
                  ->orWhereNull('attributes');
            });
        }

        $games = $query->limit((int) $this->option('limit'))->get();

        if ($games->isEmpty()) {
            $this->info('No games found needing theme collection.');
            return 0;
        }

        $this->info("Collecting themes for {$games->count()} games...");
        $bar = $this->output->createProgressBar($games->count());

        foreach ($games as $game) {
            $themeService->collectArtifacts($game);
            $bar->advance();
        }

        $bar->finish();
        $this->newLine();
        $this->info('Theme collection complete.');

        return 0;
    }
}
