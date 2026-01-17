<?php

use Illuminate\Support\Facades\DB;

require __DIR__.'/vendor/autoload.php';
$app = require_once __DIR__.'/bootstrap/app.php';
$kernel = $app->make(Illuminate\Contracts\Console\Kernel::class);
$kernel->bootstrap();

echo "Cleaning up duplicates...\n";

$affected = DB::delete("
    DELETE FROM video_game_title_sources
    WHERE id IN (
        SELECT id
        FROM (
            SELECT id,
            ROW_NUMBER() OVER (partition BY external_id, provider ORDER BY updated_at DESC, id DESC) as rnum
            FROM video_game_title_sources
            WHERE provider = 'price_charting'
        ) t
        WHERE t.rnum > 1
    )
");

echo "Deleted $affected duplicate rows.\n";
