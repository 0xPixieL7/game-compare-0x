<?php

error_reporting(E_ALL & ~E_DEPRECATED);

$file = 'storage/igdb-dumps/newest/1768716000_games.csv';
$handle = fopen($file, 'r');
$count = 0;
$headers = fgetcsv($handle);
$lastRow = null;

while (($row = fgetcsv($handle)) !== false) {
    $count++;
    $lastRow = $row;
}

echo "Total rows read: {$count}\n";
if ($lastRow) {
    // Assuming 'id' is the first column (index 0) based on previous `head` output
    echo 'Last Row ID: '.($lastRow[0] ?? 'unknown')."\n";
    echo 'Last Row Name: '.($lastRow[1] ?? 'unknown')."\n";
}
