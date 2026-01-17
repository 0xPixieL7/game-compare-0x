<?php

declare(strict_types=1);

use Illuminate\Database\Migrations\Migration;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    public function up(): void
    {
        // No-op: schema is consolidated into `2026_01_12_013519_create_products_table.php`.
        // Kept only to avoid breaking historical migration order in early-stage development.
    }

    public function down(): void
    {
        // No-op.
    }
};
