<?php

namespace App\Console\Commands;

use App\Services\Price\Steam\SteamStoreService;
use App\Services\Price\Xbox\XboxStoreService;
use App\Services\Price\PlayStation\PlayStationStoreService;
use Illuminate\Console\Command;

class TestVideoExtraction extends Command
{
    protected $signature = 'test:videos {--steam-id=1091500 : Steam App ID to test} {--xbox-id=9NBLGGH4X6GK : Xbox BigId to test} {--ps-id=UP0001-CUSA00744_00-GTAVDIGITALDOWNL : PlayStation product ID}';
    protected $description = 'Test video extraction from Steam, Xbox, and PlayStation to verify what types of videos are returned';

    public function handle(
        SteamStoreService $steam,
        XboxStoreService $xbox,
        PlayStationStoreService $ps
    ) {
        $this->info('=== Video Extraction Test ===');
        $this->newLine();

        // Test Steam
        $steamId = $this->option('steam-id');
        $this->info("🎮 Testing Steam (App ID: {$steamId})...");
        $steamData = $steam->getFullDetails($steamId, 'US');
        
        if ($steamData && isset($steamData['media']['movies'])) {
            $this->table(
                ['#', 'Video Name', 'Has WebM', 'Has MP4', 'Has HLS'],
                collect($steamData['media']['movies'])->map(fn($movie, $i) => [
                    $i + 1,
                    $movie['name'] ?? 'Unnamed',
                    $movie['webm_max'] ? '✅' : '❌',
                    $movie['mp4_max'] ? '✅' : '❌',
                    $movie['hls_max'] ? '✅' : '❌',
                ])->toArray()
            );
            
            $this->newLine();
            $this->info("Steam returned " . count($steamData['media']['movies']) . " videos");
            $this->line("Sample video names: " . implode(', ', array_slice(array_column($steamData['media']['movies'], 'name'), 0, 3)));
        } else {
            $this->warn('No videos found for Steam');
        }

        $this->newLine(2);

        // Test Xbox
        $xboxId = $this->option('xbox-id');
        $this->info("🎮 Testing Xbox (BigId: {$xboxId})...");
        $xboxData = $xbox->getFullDetails($xboxId, 'US');
        
        if ($xboxData && isset($xboxData['media']['videos'])) {
            $this->table(
                ['#', 'Video Title', 'Has Thumbnail', 'Duration'],
                collect($xboxData['media']['videos'])->map(fn($video, $i) => [
                    $i + 1,
                    $video['title'] ?? 'Unnamed',
                    $video['thumbnail'] ? '✅' : '❌',
                    isset($video['duration']) ? $video['duration'] . 's' : 'N/A',
                ])->toArray()
            );
            
            $this->newLine();
            $this->info("Xbox returned " . count($xboxData['media']['videos']) . " videos");
        } else {
            $this->warn('No videos found for Xbox');
        }

        $this->newLine(2);

        // Test PlayStation
        $psId = $this->option('ps-id');
        $this->info("🎮 Testing PlayStation (Product: {$psId})...");
        $psData = $ps->getFullDetails($psId, 'US', 'en');
        
        if ($psData && isset($psData['media']['videos'])) {
            $this->table(
                ['#', 'Has URL', 'Has Thumbnail', 'Type'],
                collect($psData['media']['videos'])->map(fn($video, $i) => [
                    $i + 1,
                    $video['url'] ? '✅' : '❌',
                    $video['thumbnail'] ? '✅' : '❌',
                    $video['type'] ?? 'Unknown',
                ])->toArray()
            );
            
            $this->newLine();
            $this->info("PlayStation returned " . count($psData['media']['videos']) . " videos");
        } else {
            $this->warn('No videos found for PlayStation');
        }

        $this->newLine(2);
        $this->info('=== Full Raw Response (JSON) ===');
        $this->newLine();
        
        if ($this->confirm('Show full Steam response?', false)) {
            $this->line(json_encode($steamData, JSON_PRETTY_PRINT));
        }
        
        if ($this->confirm('Show full Xbox response?', false)) {
            $this->line(json_encode($xboxData, JSON_PRETTY_PRINT));
        }
        
        if ($this->confirm('Show full PlayStation response?', false)) {
            $this->line(json_encode($psData, JSON_PRETTY_PRINT));
        }

        return self::SUCCESS;
    }
}
