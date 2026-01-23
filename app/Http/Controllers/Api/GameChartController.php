<?php

declare(strict_types=1);

namespace App\Http\Controllers\Api;

use App\Http\Controllers\Controller;
use App\Models\VideoGame;
use App\Models\VideoGamePrice;
use Illuminate\Http\JsonResponse;
use Illuminate\Http\Request;
use Illuminate\Support\Facades\DB;

class GameChartController extends Controller
{
    /**
     * Get price history for a game, optionally rebased to BTC.
     */
    public function priceHistory(Request $request, VideoGame $game): JsonResponse
    {
        $currency = $request->input('currency', 'USD');
        $rebaseBtc = $request->boolean('btc', false);
        
        $history = VideoGamePrice::query()
            ->where('video_game_id', $game->id)
            ->where('currency', $currency)
            ->orderBy('recorded_at', 'asc')
            ->get();

        if ($history->isEmpty()) {
            return response()->json([
                'labels' => [],
                'datasets' => [],
            ]);
        }

        $labels = $history->map(fn($p) => $p->recorded_at->format('Y-m-d H:i'))->toArray();
        
        if ($rebaseBtc) {
            $data = $history->map(fn($p) => (float) $p->amount_btc)->toArray();
            $label = "Price in BTC";
        } else {
            $data = $history->map(fn($p) => $p->amount_minor / 100)->toArray();
            $label = "Price in {$currency}";
        }

        return response()->json([
            'labels' => $labels,
            'datasets' => [
                [
                    'name' => $label,
                    'data' => $data,
                ]
            ],
            'currency' => $rebaseBtc ? 'BTC' : $currency,
        ]);
    }
}
