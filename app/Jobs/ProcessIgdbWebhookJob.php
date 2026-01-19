<?php

declare(strict_types=1);

namespace App\Jobs;

use App\Models\Product;
use App\Models\VideoGame;
use App\Models\VideoGameSource;
use App\Models\VideoGameTitle;
use App\Models\VideoGameTitleSource;
use App\Services\Normalization\IgdbRatingHelper;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Queue\Queueable;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Log;

class ProcessIgdbWebhookJob implements ShouldQueue
{
    use Queueable;

    public int $tries = 3;

    public int $timeout = 120;

    public function __construct(
        public int $eventId
    ) {}

    public function handle(): void
    {
        // 1. Fetch webhook event from DB
        $event = DB::table('webhook_events')->where('id', $this->eventId)->first();

        if (! $event) {
            Log::warning('Webhook event not found', ['event_id' => $this->eventId]);

            return;
        }

        // 2. Check if already processed (deduplication)
        if ($event->status === 'completed') {
            Log::info('Webhook event already processed', ['event_id' => $this->eventId]);

            return;
        }

        // 3. Mark as processing
        DB::table('webhook_events')
            ->where('id', $this->eventId)
            ->update([
                'status' => 'processing',
                'updated_at' => now(),
            ]);

        try {
            $payload = json_decode($event->payload, true);
            $igdbGameId = $event->igdb_game_id;

            // 4. Handle based on event type
            match ($event->event_type) {
                'create' => $this->handleCreate($igdbGameId, $payload),
                'update' => $this->handleUpdate($igdbGameId, $payload),
                'delete' => $this->handleDelete($igdbGameId),
                default => Log::warning('Unknown webhook event type', ['event_type' => $event->event_type]),
            };

            // 5. Mark as completed
            DB::table('webhook_events')
                ->where('id', $this->eventId)
                ->update([
                    'status' => 'completed',
                    'processed_at' => now(),
                    'updated_at' => now(),
                ]);

            Log::info('Webhook event processed successfully', [
                'event_id' => $this->eventId,
                'event_type' => $event->event_type,
                'igdb_game_id' => $igdbGameId,
            ]);
        } catch (\Exception $e) {
            // 6. Mark as failed
            DB::table('webhook_events')
                ->where('id', $this->eventId)
                ->update([
                    'status' => 'failed',
                    'error_message' => $e->getMessage(),
                    'updated_at' => now(),
                ]);

            Log::error('Webhook event processing failed', [
                'event_id' => $this->eventId,
                'error' => $e->getMessage(),
                'trace' => $e->getTraceAsString(),
            ]);

            throw $e; // Re-throw for retry mechanism
        }
    }

    /**
     * Handle game creation webhook.
     */
    private function handleCreate(string $igdbGameId, array $payload): void
    {
        // Check if game already exists (webhook deduplication at game level)
        $exists = VideoGame::query()
            ->where('provider', 'igdb')
            ->where('external_id', $igdbGameId)
            ->exists();

        if ($exists) {
            Log::info('Game already exists, treating as update', ['igdb_id' => $igdbGameId]);
            $this->handleUpdate($igdbGameId, $payload);

            return;
        }

        // Create game with full payload
        $this->upsertGame($igdbGameId, $payload);
    }

    /**
     * Handle game update webhook.
     */
    private function handleUpdate(string $igdbGameId, array $payload): void
    {
        // Upsert game (create if doesn't exist, update if exists)
        $this->upsertGame($igdbGameId, $payload);
    }

    /**
     * Handle game deletion webhook.
     */
    private function handleDelete(string $igdbGameId): void
    {
        DB::transaction(function () use ($igdbGameId) {
            // Soft delete or hard delete based on business logic
            $deleted = VideoGame::query()
                ->where('provider', 'igdb')
                ->where('external_id', $igdbGameId)
                ->delete();

            Log::info('Game deleted via webhook', [
                'igdb_id' => $igdbGameId,
                'deleted_count' => $deleted,
            ]);
        });
    }

    /**
     * Upsert game from webhook payload.
     * Uses same logic as IgdbLiveFetchCommand for consistency.
     */
    private function upsertGame(string $igdbGameId, array $payload): void
    {
        DB::transaction(function () use ($igdbGameId, $payload) {
            // 1. Get or create product
            $productName = $payload['name'] ?? null;
            if (! $productName) {
                throw new \InvalidArgumentException('Game payload missing name field');
            }

            $product = Product::query()->firstOrCreate(
                ['name' => $productName],
                ['slug' => \Illuminate\Support\Str::slug($productName)]
            );

            // 2. Get or create video game title
            $slug = \Illuminate\Support\Str::slug($productName);
            $title = VideoGameTitle::query()->firstOrCreate(
                ['product_id' => $product->id, 'slug' => $slug],
                ['name' => $productName]
            );

            // 3. Get or create provider source
            $source = VideoGameSource::query()->firstOrCreate(
                ['provider' => 'igdb'],
                ['name' => 'IGDB', 'base_url' => 'https://www.igdb.com']
            );

            // 4. Extract rating using helper
            $ratingHelper = new IgdbRatingHelper;
            $rating = $ratingHelper->extractPercentage($payload);
            $ratingCount = $ratingHelper->extractRatingCount($payload);

            // 5. Upsert video game
            VideoGame::query()->updateOrCreate(
                [
                    'provider' => 'igdb',
                    'external_id' => $igdbGameId,
                ],
                [
                    'video_game_title_id' => $title->id,
                    'name' => $productName,
                    'slug' => $slug,
                    'summary' => $payload['summary'] ?? null,
                    'storyline' => $payload['storyline'] ?? null,
                    'rating' => $rating,
                    'rating_count' => $ratingCount,
                    'url' => $payload['url'] ?? null,
                    'release_date' => isset($payload['first_release_date'])
                        ? date('Y-m-d H:i:s', $payload['first_release_date'])
                        : null,
                    'updated_at' => now(),
                ]
            );

            // 6. Upsert title source (stores full raw payload)
            VideoGameTitleSource::query()->updateOrCreate(
                [
                    'video_game_title_id' => $title->id,
                    'video_game_source_id' => $source->id,
                    'provider_item_id' => $igdbGameId,
                ],
                [
                    'provider' => 'igdb',
                    'external_id' => (int) $igdbGameId,
                    'raw_payload' => json_encode($payload),
                    'updated_at' => now(),
                ]
            );

            Log::info('Game upserted from webhook', [
                'igdb_id' => $igdbGameId,
                'product_id' => $product->id,
                'title_id' => $title->id,
            ]);
        });
    }

    /**
     * Handle job failure after all retries exhausted.
     */
    public function failed(\Throwable $exception): void
    {
        DB::table('webhook_events')
            ->where('id', $this->eventId)
            ->update([
                'status' => 'failed',
                'error_message' => $exception->getMessage(),
                'updated_at' => now(),
            ]);

        Log::error('Webhook job failed after retries', [
            'event_id' => $this->eventId,
            'error' => $exception->getMessage(),
        ]);
    }
}
