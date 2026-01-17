<?php

namespace App\Observers;

use App\Models\VideoGameTitle;

class VideoGameTitleObserver
{
    /**
     * Handle the VideoGameTitle "created" event.
     */
    public function created(VideoGameTitle $videoGameTitle): void
    {
        //
    }

    /**
     * Handle the VideoGameTitle "updated" event.
     */
    public function updated(VideoGameTitle $videoGameTitle): void
    {
        //
    }

    /**
     * Handle the VideoGameTitle "deleted" event.
     */
    public function deleted(VideoGameTitle $videoGameTitle): void
    {
        //
    }

    /**
     * Handle the VideoGameTitle "restored" event.
     */
    public function restored(VideoGameTitle $videoGameTitle): void
    {
        //
    }

    /**
     * Handle the VideoGameTitle "force deleted" event.
     */
    public function forceDeleted(VideoGameTitle $videoGameTitle): void
    {
        //
    }
}
