<?php

namespace App\Events;

use App\Models\VideoGame;
use Illuminate\Broadcasting\InteractsWithSockets;
use Illuminate\Foundation\Events\Dispatchable;
use Illuminate\Queue\SerializesModels;

class VideoGameViewed
{
    use Dispatchable, InteractsWithSockets, SerializesModels;

    public VideoGame $videoGame;
    public bool $forceRefresh;

    /**
     * Create a new event instance.
     */
    public function __construct(VideoGame $videoGame, bool $forceRefresh = false)
    {
        $this->videoGame = $videoGame;
        $this->forceRefresh = $forceRefresh;
    }
}
