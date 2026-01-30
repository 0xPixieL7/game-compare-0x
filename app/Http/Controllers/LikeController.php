<?php

namespace App\Http\Controllers;

use App\Models\VideoGame;
use Illuminate\Http\RedirectResponse;
use Illuminate\Http\Request;

class LikeController extends Controller
{
    public function toggle(Request $request, VideoGame $game): RedirectResponse
    {
        $user = $request->user();

        if ($user->likes()->where('video_game_id', $game->id)->exists()) {
            $user->likes()->detach($game->id);
        } else {
            $user->likes()->attach($game->id);
        }

        return back();
    }
}
