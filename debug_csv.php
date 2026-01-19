<?php

$file = 'storage/igdb-dumps/newest/1768716000_games.csv';
$handle = fopen($file, 'r');
$count = 0;
$headers = fgetcsv($handle);

echo 'Headers: '.count($headers)."\n";

while (($row = fgetcsv($handle)) !== false) {
    $count++;
    if ($count % 10000 === 0) {
        echo "Read {$count} rows...\r";
    }
}

echo "\nTotal rows read: {$count}\n";
