<?php

error_reporting(E_ALL & ~E_DEPRECATED);

$file = 'storage/igdb-dumps/newest/1768716000_games.csv';
$handle = fopen($file, 'r');
$count = 0;
$headers = fgetcsv($handle);

echo 'Headers: '.count($headers)."\n";

while (($row = fgetcsv($handle)) !== false) {
    $count++;
}

echo "Total rows read: {$count}\n";
