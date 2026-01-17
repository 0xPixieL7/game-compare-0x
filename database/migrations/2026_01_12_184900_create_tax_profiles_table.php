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
        Schema::dropIfExists('tax_profiles');

        Schema::create('tax_profiles', function (Blueprint $table) {
            $table->id();
            $table->char('region_code', 2)->unique()->comment('ISO 3166-1 alpha-2 country/region code');
            $table->decimal('vat_rate', 5, 4)->default(0)->comment('VAT/sales tax rate (0.20 = 20%)');
            $table->timestamp('effective_from')->nullable()->comment('When this tax rate became effective');
            $table->text('notes')->nullable()->comment('Notes about tax application');
            $table->timestamps();

            // Indexes
            $table->index(['region_code', 'effective_from']);
            $table->index('effective_from');
        });
    }

    /**
     * Reverse the migrations.
     */
    public function down(): void
    {
        Schema::dropIfExists('tax_profiles');
    }
};
