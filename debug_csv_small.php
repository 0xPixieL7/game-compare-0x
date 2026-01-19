<?php

error_reporting(E_ALL & ~E_DEPRECATED);

$file = 'storage/igdb-dumps/newest/1768282113_games.csv';
$handle = fopen($file, 'r');
$count = 0;
$headers = fgetcsv($handle);

while (($row = fgetcsv($handle)) !== false) {
    $count++;
}

echo "File: {$file}\n";
echo "Total rows read: {$count}\n";
