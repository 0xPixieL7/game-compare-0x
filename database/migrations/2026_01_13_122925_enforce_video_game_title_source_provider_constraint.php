<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    /**
     * Run the migrations.
     */
    public function up(): void
    {
        // Ensure existing rows reflect the provider of their owning source.
        DB::statement(<<<'SQL'
            UPDATE video_game_title_sources AS vts
            SET provider = vgs.provider
            FROM video_game_sources AS vgs
            WHERE vts.video_game_source_id = vgs.id
              AND (vts.provider IS NULL OR vts.provider <> vgs.provider)
        SQL);

        // Provide a deterministic (intentionally invalid) default that will immediately
        // violate the composite FK if code forgets to set the provider explicitly.
        $driver = Schema::getConnection()->getDriverName();

        if ($driver === 'pgsql') {
            DB::statement("ALTER TABLE video_game_title_sources ALTER COLUMN provider SET DEFAULT '__invalid_provider__'");
            DB::statement('ALTER TABLE video_game_title_sources ALTER COLUMN provider SET NOT NULL');
        } elseif ($driver === 'sqlite') {
            // SQLite cannot modify existing columns without rebuilding the table; skip the DDL and
            // rely on code/tests to guarantee provider is populated during inserts.
        } else {
            DB::statement("ALTER TABLE video_game_title_sources MODIFY provider VARCHAR(255) NOT NULL DEFAULT '__invalid_provider__'");
        }

        // Ensure the source table exposes a unique pair so we can reference it.
        Schema::table('video_game_sources', function (Blueprint $table): void {
            $table->unique(['id', 'provider'], 'video_game_sources_id_provider_unique');
        });

        Schema::table('video_game_title_sources', function (Blueprint $table): void {
            $table->foreign(['video_game_source_id', 'provider'], 'vg_title_sources_source_provider_fk')
                ->references(['id', 'provider'])
                ->on('video_game_sources')
                ->cascadeOnDelete();
        });
    }

    /**
     * Reverse the migrations.
     */
    public function down(): void
    {
        Schema::table('video_game_title_sources', function (Blueprint $table): void {
            $table->dropForeign('vg_title_sources_source_provider_fk');
        });

        Schema::table('video_game_sources', function (Blueprint $table): void {
            $table->dropUnique('video_game_sources_id_provider_unique');
        });

        $driver = Schema::getConnection()->getDriverName();

        if ($driver === 'pgsql') {
            DB::statement('ALTER TABLE video_game_title_sources ALTER COLUMN provider DROP DEFAULT');
        } elseif ($driver === 'sqlite') {
            // Nothing to roll back; SQLite never altered the column definition in "up".
        } else {
            DB::statement('ALTER TABLE video_game_title_sources MODIFY provider VARCHAR(255) NOT NULL');
        }
    }
};
