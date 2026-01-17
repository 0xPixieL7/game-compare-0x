<?php

declare(strict_types=1);

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    public function up(): void
    {
        if (Schema::hasTable('video_game_sources') && ! Schema::hasColumn('video_game_sources', 'metadata')) {
            Schema::table('video_game_sources', function (Blueprint $table): void {
                $table->json('metadata')->nullable()->after('items_count');
            });
        }

        if (Schema::hasTable('video_game_prices')) {
            Schema::table('video_game_prices', function (Blueprint $table): void {
                if (! Schema::hasColumn('video_game_prices', 'bucket')) {
                    $table->string('bucket', 32)->default('snapshot')->after('recorded_at');
                }

                if (! Schema::hasColumn('video_game_prices', 'window_start')) {
                    $table->timestamp('window_start')->nullable()->after('bucket');
                }

                if (! Schema::hasColumn('video_game_prices', 'window_end')) {
                    $table->timestamp('window_end')->nullable()->after('window_start');
                }

                if (! Schema::hasColumn('video_game_prices', 'btc_value_sats')) {
                    $table->unsignedBigInteger('btc_value_sats')->nullable()->after('amount_minor');
                }

                if (! Schema::hasColumn('video_game_prices', 'aggregation_count')) {
                    $table->unsignedInteger('aggregation_count')->default(1)->after('btc_value_sats');
                }

                if (! Schema::hasColumn('video_game_prices', 'series_key')) {
                    $table->string('series_key')->nullable()->after('aggregation_count');
                }
            });

            Schema::table('video_game_prices', function (Blueprint $table): void {
                $table->index(['video_game_id', 'bucket']);
                $table->index(['video_game_id', 'bucket', 'window_start']);
            });
        }
    }

    public function down(): void
    {
        if (Schema::hasTable('video_game_sources') && Schema::hasColumn('video_game_sources', 'metadata')) {
            Schema::table('video_game_sources', function (Blueprint $table): void {
                $table->dropColumn('metadata');
            });
        }

        if (Schema::hasTable('video_game_prices')) {
            Schema::table('video_game_prices', function (Blueprint $table): void {
                foreach (['video_game_prices_video_game_id_bucket_index', 'video_game_prices_video_game_id_bucket_window_start_index'] as $index) {
                    DB::statement("DROP INDEX IF EXISTS {$index}");
                }

                foreach (['series_key', 'aggregation_count', 'btc_value_sats', 'window_end', 'window_start', 'bucket'] as $column) {
                    if (Schema::hasColumn('video_game_prices', $column)) {
                        $table->dropColumn($column);
                    }
                }
            });
        }
    }
};
