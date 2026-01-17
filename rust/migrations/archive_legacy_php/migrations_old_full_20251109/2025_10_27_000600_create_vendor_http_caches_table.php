<?php

use Illuminate\Database\Migrations\Migration;

return new class extends Migration
{
    public function up(): void
    {
        // Intentionally no-op; HTTP cache persistence removed.
    }

    public function down(): void
    {
        // Intentionally no-op.
    }
};
