<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    /**
     * Run the migrations.
     */
    public function up(): void
    {
        Schema::table('video_games', function (Blueprint $table) {
            $table->string('external_id')->change();
        });
    }

    /**
     * Reverse the migrations.
     */
    public function down(): void
    {
        Schema::table('video_games', function (Blueprint $table) {
            // Reverting to BigInteger might fail if non-numeric data exists, 
            // but for down() we attempt to restore the previous state.
            // Using DB::statement to cast if possible, or just standard change.
            DB::statement('ALTER TABLE video_games ALTER COLUMN external_id TYPE bigint USING (trim(external_id)::bigint)');
        });
    }
};
