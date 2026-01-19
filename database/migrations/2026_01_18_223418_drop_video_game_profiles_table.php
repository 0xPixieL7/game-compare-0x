<?php

declare(strict_types=1);

use Illuminate\Database\Migrations\Migration;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    /**
     * Run the migrations.
     */
    public function up(): void
    {
        Schema::dropIfExists('video_game_profiles');
    }

    /**
     * Reverse the migrations.
     */
    public function down(): void
    {
        // This table is redundant and superseded by 'products'.
        // No restoration logic provided to keep schema clean.
    }
};
