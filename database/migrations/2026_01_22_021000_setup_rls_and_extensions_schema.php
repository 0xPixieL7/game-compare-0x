<?php

declare(strict_types=1);

use Illuminate\Database\Migrations\Migration;
use Illuminate\Support\Facades\DB;

return new class extends Migration
{
    /**
     * Organize extensions and implement RLS policies for security
     */
    public function up(): void
    {
        // 1. Create extensions schema if it doesn't exist
        DB::statement('CREATE SCHEMA IF NOT EXISTS extensions');

        // 2. Move pg_trgm extension to extensions schema if it exists
        DB::statement('CREATE EXTENSION IF NOT EXISTS pg_trgm SCHEMA extensions');

        // 3. Set search_path to include extensions schema
        DB::statement('ALTER DATABASE postgres SET search_path TO public, extensions');

        // 4. Enable Row Level Security on sensitive tables
        $tables = [
            'users',
            'video_game_prices',
            'video_game_user_prices',
            'user_game_libraries',
            'user_wishlists',
        ];

        foreach ($tables as $table) {
            DB::statement("ALTER TABLE {$table} ENABLE ROW LEVEL SECURITY");
        }

        // 5. Create RLS policies for user-specific data

        // Users can only view/update their own profile
        DB::statement('
            CREATE POLICY users_own_data ON users
            FOR ALL
            USING (id = current_setting(\'app.user_id\', true)::bigint)
        ');

        // Users can view all prices but only insert/update their own contributions
        DB::statement('
            CREATE POLICY prices_read_all ON video_game_prices
            FOR SELECT
            USING (true)
        ');

        DB::statement('
            CREATE POLICY user_prices_own_data ON video_game_user_prices
            FOR ALL
            USING (user_id = current_setting(\'app.user_id\', true)::bigint)
        ');

        // Game libraries - users can only access theirowner data
        DB::statement('
            CREATE POLICY libraries_own_data ON user_game_libraries
            FOR ALL
            USING (user_id = current_setting(\'app.user_id\', true)::bigint)
        ');

        // Wishlists - users can only access their own
        DB::statement('
            CREATE POLICY wishlists_own_data ON user_wishlists
            FOR ALL
            USING (user_id = current_setting(\'app.user_id\', true)::bigint)
        ');

        // 6. Create policy for public read-only access to game data
        $publicTables = [
            'video_games',
            'video_game_titles',
            'video_game_sources',
            'video_game_title_sources',
            'images',
            'videos',
        ];

        foreach ($publicTables as $table) {
            DB::statement("ALTER TABLE {$table} ENABLE ROW LEVEL SECURITY");
            DB::statement("
                CREATE POLICY {$table}_public_read ON {$table}
                FOR SELECT
                USING (true)
            ");
        }
    }

    /**
     * Reverse the migrations.
     */
    public function down(): void
    {
        // Drop RLS policies
        DB::statement('DROP POLICY IF EXISTS users_own_data ON users');
        DB::statement('DROP POLICY IF EXISTS prices_read_all ON video_game_prices');
        DB::statement('DROP POLICY IF EXISTS user_prices_own_data ON video_game_user_prices');
        DB::statement('DROP POLICY IF EXISTS libraries_own_data ON user_game_libraries');
        DB::statement('DROP POLICY IF EXISTS wishlists_own_data ON user_wishlists');

        $publicTables = ['video_games', 'video_game_titles', 'video_game_sources',
            'video_game_title_sources', 'images', 'videos'];

        foreach ($publicTables as $table) {
            DB::statement("DROP POLICY IF EXISTS {$table}_public_read ON {$table}");
            DB::statement("ALTER TABLE {$table} DISABLE ROW LEVEL SECURITY");
        }

        // Disable RLS
        $tables = ['users', 'video_game_prices', 'video_game_user_prices',
            'user_game_libraries', 'user_wishlists'];

        foreach ($tables as $table) {
            DB::statement("ALTER TABLE {$table} DISABLE ROW LEVEL SECURITY");
        }

        // Note: We don't move extensions back or drop the extensions schema
        // as other databases might be using it
    }
};
