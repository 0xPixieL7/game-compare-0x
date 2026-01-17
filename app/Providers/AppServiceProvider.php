<?php

namespace App\Providers;

use Illuminate\Support\ServiceProvider;

class AppServiceProvider extends ServiceProvider
{
    /**
     * Register any application services.
     */
    public function register(): void
    {
        $this->app->bind(\App\Services\PriceCharting\PriceChartingClient::class, function ($app) {
            return new \App\Services\PriceCharting\PriceChartingClient(
                config('services.price_charting.token')
            );
        });

        $this->app->singleton(\App\Services\Tgdb\TgdbClient::class, function ($app) {
            return new \App\Services\Tgdb\TgdbClient;
        });
    }

    /**
     * Bootstrap any application services.
     */
    public function boot(): void
    {
        // Register Observers
        \App\Models\VideoGame::observe(\App\Observers\VideoGameObserver::class);
        \App\Models\VideoGameTitleSource::observe(\App\Observers\VideoGameTitleSourceObserver::class);

        // Rate Limiters for Enrichment Jobs
        \Illuminate\Support\Facades\RateLimiter::for('tgdb', function ($job) {
            return \Illuminate\Cache\RateLimiting\Limit::perSecond(4);
        });

        \Illuminate\Support\Facades\RateLimiter::for('steam', function ($job) {
            return \Illuminate\Cache\RateLimiting\Limit::perMinute(200);
        });

        \Illuminate\Support\Facades\RateLimiter::for('igdb', function ($job) {
            return \Illuminate\Cache\RateLimiting\Limit::perSecond(4);
        });

        \Illuminate\Support\Facades\RateLimiter::for('psstore', function ($job) {
            return \Illuminate\Cache\RateLimiting\Limit::perSecond(2);
        });

        \Illuminate\Support\Facades\RateLimiter::for('xbox', function ($job) {
            return \Illuminate\Cache\RateLimiting\Limit::perSecond(5);
        });
    }
}
