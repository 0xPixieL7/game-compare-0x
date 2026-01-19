<?php

declare(strict_types=1);

namespace App\Console\Commands;

use Illuminate\Console\Command;
use Illuminate\Support\Facades\Http;

class RegisterIgdbWebhooksCommand extends Command
{
    protected $signature = 'igdb:register-webhooks
                           {--list : List all registered webhooks}
                           {--unregister= : Unregister webhook by ID}';

    protected $description = 'Register IGDB webhooks for real-time game updates';

    private string $baseUrl = 'https://api.igdb.com/v4';

    private ?string $accessToken = null;

    public function handle(): int
    {
        if (! $this->obtainAccessToken()) {
            return self::FAILURE;
        }

        if ($this->option('list')) {
            return $this->listWebhooks();
        }

        if ($unregisterId = $this->option('unregister')) {
            return $this->unregisterWebhook((int) $unregisterId);
        }

        return $this->registerWebhooks();
    }

    private function obtainAccessToken(): bool
    {
        $clientId = config('services.igdb.client_id');
        $clientSecret = config('services.igdb.client_secret');

        if (! $clientId || ! $clientSecret) {
            $this->error('IGDB_CLIENT_ID and IGDB_CLIENT_SECRET must be set in .env');

            return false;
        }

        $this->info('🔑 Obtaining OAuth token from Twitch...');

        $response = Http::asForm()->post('https://id.twitch.tv/oauth2/token', [
            'client_id' => $clientId,
            'client_secret' => $clientSecret,
            'grant_type' => 'client_credentials',
        ]);

        if (! $response->successful()) {
            $this->error('Failed to obtain OAuth token: '.$response->body());

            return false;
        }

        $this->accessToken = $response->json('access_token');
        $this->info('✓ OAuth token obtained');
        $this->newLine();

        return true;
    }

    private function registerWebhooks(): int
    {
        $webhookSecret = config('services.igdb.webhook_secret');
        if (! $webhookSecret) {
            $this->error('IGDB_WEBHOOK_SECRET must be set in .env');
            $this->info('Generate a random secret: openssl rand -hex 32');

            return self::FAILURE;
        }

        $appUrl = config('app.url');
        if (! $appUrl || $appUrl === 'http://localhost') {
            $this->warn('APP_URL is set to localhost. Webhooks require a public URL.');
            $this->info('Update APP_URL in .env to your public domain (e.g., https://yourdomain.com)');

            return self::FAILURE;
        }

        $this->info('🔗 Registering IGDB webhooks...');
        $this->info("   App URL: {$appUrl}");
        $this->info('   Secret: '.substr($webhookSecret, 0, 8).'...');
        $this->newLine();

        // Register webhooks for all three event types
        $eventTypes = ['create', 'update', 'delete'];
        $results = [];

        foreach ($eventTypes as $eventType) {
            $webhookUrl = "{$appUrl}/webhooks/igdb/{$eventType}";

            $this->info("Registering {$eventType} webhook...");

            $response = Http::asForm()
                ->withHeaders([
                    'Client-ID' => config('services.igdb.client_id'),
                    'Authorization' => "Bearer {$this->accessToken}",
                ])
                ->post("{$this->baseUrl}/games/webhooks/", [
                    'url' => $webhookUrl,
                    'secret' => $webhookSecret,
                    'method' => $eventType,
                ]);

            if ($response->successful()) {
                $webhook = $response->json();
                $this->info("✓ {$eventType} webhook registered (ID: {$webhook['id']})");
                $results[$eventType] = $webhook;
            } elseif ($response->status() === 409) {
                $this->warn("⚠ {$eventType} webhook already exists");
                $results[$eventType] = 'already_exists';
            } else {
                $this->error("✗ {$eventType} webhook failed: ".$response->body());
                $results[$eventType] = 'failed';
            }
        }

        $this->newLine();
        $this->info('✅ Webhook registration complete!');
        $this->table(
            ['Event Type', 'Status', 'Webhook ID'],
            collect($results)->map(fn ($result, $type) => [
                $type,
                is_array($result) ? 'registered' : $result,
                is_array($result) ? $result['id'] : '-',
            ])->values()->all()
        );

        $this->newLine();
        $this->info('Next steps:');
        $this->line('  1. Ensure your queue worker is running: php artisan queue:listen');
        $this->line('  2. Monitor webhook events: SELECT * FROM webhook_events ORDER BY created_at DESC LIMIT 10;');
        $this->line('  3. List webhooks: php artisan igdb:register-webhooks --list');

        return self::SUCCESS;
    }

    private function listWebhooks(): int
    {
        $this->info('📋 Fetching registered webhooks...');

        $response = Http::withHeaders([
            'Client-ID' => config('services.igdb.client_id'),
            'Authorization' => "Bearer {$this->accessToken}",
        ])->get("{$this->baseUrl}/webhooks/");

        if (! $response->successful()) {
            $this->error('Failed to fetch webhooks: '.$response->body());

            return self::FAILURE;
        }

        $webhooks = $response->json();

        if (empty($webhooks)) {
            $this->warn('No webhooks registered.');
            $this->info('Register webhooks: php artisan igdb:register-webhooks');

            return self::SUCCESS;
        }

        $this->table(
            ['ID', 'URL', 'Method', 'Active', 'Created At'],
            collect($webhooks)->map(fn ($webhook) => [
                $webhook['id'],
                $webhook['url'],
                $this->mapSubCategory($webhook['sub_category'] ?? 0),
                $webhook['active'] ? '✓' : '✗',
                date('Y-m-d H:i:s', strtotime($webhook['created_at'])),
            ])->all()
        );

        return self::SUCCESS;
    }

    private function unregisterWebhook(int $webhookId): int
    {
        $this->info("🗑 Unregistering webhook ID {$webhookId}...");

        $response = Http::withHeaders([
            'Client-ID' => config('services.igdb.client_id'),
            'Authorization' => "Bearer {$this->accessToken}",
        ])->delete("{$this->baseUrl}/webhooks/{$webhookId}");

        if ($response->successful()) {
            $this->info("✓ Webhook {$webhookId} unregistered successfully");

            return self::SUCCESS;
        }

        $this->error('Failed to unregister webhook: '.$response->body());

        return self::FAILURE;
    }

    private function mapSubCategory(int $subCategory): string
    {
        return match ($subCategory) {
            0 => 'create',
            1 => 'update',
            2 => 'delete',
            default => 'unknown',
        };
    }
}
