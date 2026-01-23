import { show as dashboardShow } from '@/actions/App/Http/Controllers/DashboardController';
import { AppleTvCard } from '@/components/apple-tv-card';
import { useTransitionNav } from '@/components/transition/TransitionProvider';
import Image from '@/components/ui/image';
import { Sparkles, Star } from 'lucide-react';
import { type FC } from 'react';

// Discriminated union supporting both Game and GameListItem types
type GameCardData = {
    id: number;
    name: string;
    canonical_name?: string | null;
    rating?: number | null;
    slug?: string;
} & (
    | {
          cover_url: string;
          cover_url_high_res?: string;
          latest_price?: number | string | null;
          currency?: string | null;
      }
    | {
          media: {
              hero_url?: string | null;
              cover_url_high_res?: string | null;
              cover_url?: string | null;
              cover_url_thumb?: string | null;
          };
          pricing?: {
              amount_major?: number;
              currency?: string;
          } | null;
      }
);

interface GameCardProps {
    game: GameCardData;
    className?: string;
}

export const GameCard: FC<GameCardProps> = ({ game, className = '' }) => {
    const { navigateCardToDetail, isRunning } = useTransitionNav();

    // Handle different data shapes from different routes
    const isListItem = 'cover_url' in game;

    // High-res priority: High-res Cover -> Cover -> Hero -> Thumb
    const coverUrl = isListItem
        ? game.cover_url_high_res || game.cover_url
        : game.media?.cover_url_high_res ||
          game.media?.cover_url ||
          game.media?.hero_url ||
          game.media?.cover_url_thumb ||
          '/placeholder-game.jpg';

    const rating = game.rating;
    const name = isListItem ? game.name : game.canonical_name || game.name;
    const price = isListItem ? game.latest_price : game.pricing?.amount_major;
    const currency = isListItem ? game.currency : game.pricing?.currency;

    // Use Wayfinder for the link
    const wayfinderRoute = dashboardShow(game.id);
    const href = wayfinderRoute?.url || `/dashboard/${game.id}`;

    // Dynamic Label Strategy
    let label = 'NEW';
    let labelColor = 'text-blue-400 border-blue-500/30 bg-blue-500/10';

    if (rating) {
        if (rating >= 90) {
            label = 'MASTERPIECE';
            labelColor =
                'text-yellow-400 border-yellow-500/30 bg-yellow-500/10';
        } else if (rating >= 80) {
            label = 'MUST PLAY';
            labelColor =
                'text-emerald-400 border-emerald-500/30 bg-emerald-500/10';
        } else if (rating >= 70) {
            label = 'TOP RATED';
            labelColor = 'text-blue-400 border-blue-500/30 bg-blue-500/10';
        }
    }

    // View Transition Name (must match detail page)
    const vtName = `game-cover-${game.id}`;

    return (
        <button
            disabled={isRunning}
            onClick={() => navigateCardToDetail(href)}
            className={`group/card block h-full w-full text-left transition-all duration-300 disabled:opacity-50 ${className}`}
        >
            <AppleTvCard className="h-full w-full overflow-hidden rounded-2xl border border-white/10 bg-[#0a0a0a] shadow-2xl">
                {/* Background Artwork */}
                <div className="absolute inset-0 z-0">
                    <Image
                        src={coverUrl}
                        alt={name}
                        fill
                        className="transition-transform duration-700 group-hover/atv:scale-110"
                        style={{ viewTransitionName: vtName }}
                    />
                    <div className="absolute inset-0 bg-gradient-to-t from-black via-black/20 to-transparent opacity-80" />
                </div>

                {/* Content Overlay */}
                <div className="relative z-10 flex h-full flex-col justify-between p-4">
                    {/* Top Labels */}
                    <div className="flex items-center justify-between">
                        <div
                            className={`flex items-center gap-1.5 rounded-full border px-2 py-0.5 font-mono text-[9px] font-bold tracking-wider backdrop-blur-md ${labelColor}`}
                        >
                            <div className="h-1 w-1 animate-pulse rounded-full bg-current" />
                            {label}
                        </div>

                        {rating && (
                            <div className="flex items-center gap-1 rounded-full border border-white/10 bg-black/40 px-2 py-0.5 text-[10px] font-black text-white backdrop-blur-md">
                                <Star className="h-3 w-3 fill-yellow-400 text-yellow-400" />
                                <span>{Math.round(rating)}</span>
                            </div>
                        )}
                    </div>

                    {/* Bottom Info */}
                    <div className="space-y-2">
                        <h3 className="line-clamp-2 text-sm font-black tracking-tight text-white drop-shadow-2xl">
                            {name}
                        </h3>

                        <div className="flex items-center justify-between border-t border-white/10 pt-2">
                            <div className="flex items-center gap-1 text-[9px] font-bold text-slate-400 uppercase">
                                <Sparkles className="h-2.5 w-2.5" />
                                Verified
                            </div>

                            {price ? (
                                <span className="text-xs font-black text-blue-400">
                                    {currency} {price}
                                </span>
                            ) : (
                                <span className="font-mono text-[10px] font-bold tracking-tighter text-white/40 uppercase">
                                    Analyze
                                </span>
                            )}
                        </div>
                    </div>
                </div>
            </AppleTvCard>
        </button>
    );
};
