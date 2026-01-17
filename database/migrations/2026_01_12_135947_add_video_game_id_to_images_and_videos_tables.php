<?php

declare(strict_types=1);

use Illuminate\Database\Migrations\Migration;

/**
 * CONSOLIDATED: Columns now included in create migrations:
 * - 2026_01_12_030100_create_images_table.php
 * - 2026_01_12_030200_create_videos_table.php
 */
return new class extends Migration
{
    public function up(): void
    {
        // No-op: consolidated into create migrations
    }

    public function down(): void
    {
        // No-op
    }
};
