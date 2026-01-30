<?php

declare(strict_types=1);

namespace App\Console\Commands;

use App\Models\VideoGame;
use App\Services\Price\EpicGames\EpicGamesStoreService;
use Illuminate\Console\Command;

class EpicTokenCheck extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'epic:token-check {--slug=}';

    /**
     * The console command description.
     *
     * @var string
     */
    protected $description = 'Validate Epic bearer token by fetching a price.';

    /**
     * Execute the console command.
     */
    public function handle(): int
    {
        $token = config('services.epic.bearer_token');
        if (! is_string($token) || $token === '') {
            $this->error('EPIC_BEARER_TOKEN is missing.');

            return self::FAILURE;
        }

        $slug = $this->option('slug');
        if (! is_string($slug) || $slug === '') {
            $slug = VideoGame::query()
                ->whereNotNull('attributes->epic_slug')
                ->selectRaw("attributes->>'epic_slug' as epic_slug")
                ->orderBy('id')
                ->value('epic_slug');
        }

        if (! is_string($slug) || $slug === '') {
            $this->error('No epic_slug found in video_games.');

            return self::FAILURE;
        }

        $price = app(EpicGamesStoreService::class)->getPrice($slug, 'US');
        if (! $price) {
            $this->error("Epic token check failed for slug: {$slug}");

            return self::FAILURE;
        }

        $amount = $price['amount_minor'] ?? null;
        $currency = $price['currency'] ?? null;
        $this->info("Epic token ok. {$slug} price: {$amount} {$currency}");

        return self::SUCCESS;
    }
}
