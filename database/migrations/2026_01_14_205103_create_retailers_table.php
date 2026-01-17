<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    public function up(): void
    {
        Schema::create('retailers', function (Blueprint $table) {
            $table->id();
            $table->string('name'); // Amazon, Steam, etc.
            $table->string('slug')->unique();
            $table->string('base_url')->nullable();
            $table->string('domain_matcher'); // e.g. "amazon.com", "steampowered.com"
            $table->boolean('is_active')->default(true);
            $table->json('config')->nullable(); // Scraper config (selectors, etc.)
            $table->timestamps();
        });
    }

    public function down(): void
    {
        Schema::dropIfExists('retailers');
    }
};
