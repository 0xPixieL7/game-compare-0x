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
        Schema::create('webhook_events', function (Blueprint $table) {
            $table->id();
            $table->string('provider')->default('igdb')->index();
            $table->string('event_type')->index(); // create, update, delete
            $table->string('igdb_game_id')->index(); // IGDB external ID
            $table->jsonb('payload'); // Full webhook payload
            $table->jsonb('headers')->nullable(); // Request headers for debugging
            $table->string('status')->default('pending')->index(); // pending, processing, completed, failed
            $table->text('error_message')->nullable();
            $table->timestamp('processed_at')->nullable()->index();
            $table->timestamps();

            // Composite index for deduplication checks
            $table->index(['provider', 'igdb_game_id', 'event_type', 'created_at']);
        });
    }

    /**
     * Reverse the migrations.
     */
    public function down(): void
    {
        Schema::dropIfExists('webhook_events');
    }
};
