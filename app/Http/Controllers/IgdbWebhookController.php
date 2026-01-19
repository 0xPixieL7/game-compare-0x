<?php

declare(strict_types=1);

namespace App\Http\Controllers;

use App\Jobs\ProcessIgdbWebhookJob;
use Illuminate\Http\JsonResponse;
use Illuminate\Http\Request;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Log;

class IgdbWebhookController extends Controller
{
    /**
     * Handle incoming IGDB webhook.
     *
     * This endpoint receives create/update/delete events from IGDB.
     * It verifies the signature, stores the raw event, and queues processing.
     */
    public function handle(Request $request): JsonResponse
    {
        // 1. Verify webhook signature
        if (! $this->verifySignature($request)) {
            Log::warning('IGDB webhook signature verification failed', [
                'ip' => $request->ip(),
                'headers' => $request->headers->all(),
            ]);

            return response()->json(['error' => 'Invalid signature'], 401);
        }

        // 2. Extract payload
        $payload = $request->all();
        $igdbGameId = $payload['id'] ?? null;

        if (! $igdbGameId) {
            Log::warning('IGDB webhook missing game ID', ['payload' => $payload]);

            return response()->json(['error' => 'Missing game ID'], 400);
        }

        // 3. Determine event type from URL path
        // IGDB sends to different endpoints: /webhooks/igdb/create, /webhooks/igdb/update, /webhooks/igdb/delete
        $eventType = $request->route('eventType') ?? 'unknown';

        // 4. Store webhook event directly in DB (fast response)
        try {
            $eventId = DB::table('webhook_events')->insertGetId([
                'provider' => 'igdb',
                'event_type' => $eventType,
                'igdb_game_id' => (string) $igdbGameId,
                'payload' => json_encode($payload),
                'headers' => json_encode([
                    'X-Secret' => $request->header('X-Secret'),
                    'User-Agent' => $request->header('User-Agent'),
                    'Content-Type' => $request->header('Content-Type'),
                ]),
                'status' => 'pending',
                'created_at' => now(),
                'updated_at' => now(),
            ]);

            Log::info('IGDB webhook event stored', [
                'event_id' => $eventId,
                'event_type' => $eventType,
                'igdb_game_id' => $igdbGameId,
            ]);
        } catch (\Exception $e) {
            Log::error('Failed to store IGDB webhook event', [
                'error' => $e->getMessage(),
                'payload' => $payload,
            ]);

            return response()->json(['error' => 'Storage failed'], 500);
        }

        // 5. Queue processing job (async)
        try {
            ProcessIgdbWebhookJob::dispatch($eventId);
        } catch (\Exception $e) {
            Log::error('Failed to dispatch IGDB webhook job', [
                'event_id' => $eventId,
                'error' => $e->getMessage(),
            ]);
        }

        // 6. Return 200 quickly (IGDB expects fast response)
        return response()->json(['status' => 'received', 'event_id' => $eventId], 200);
    }

    /**
     * Verify IGDB webhook signature.
     *
     * IGDB sends X-Secret header with the secret you provided during registration.
     */
    private function verifySignature(Request $request): bool
    {
        $receivedSecret = $request->header('X-Secret');
        $expectedSecret = config('services.igdb.webhook_secret');

        if (! $expectedSecret) {
            Log::error('IGDB webhook secret not configured in services.igdb.webhook_secret');

            return false;
        }

        return hash_equals($expectedSecret, $receivedSecret ?? '');
    }
}
