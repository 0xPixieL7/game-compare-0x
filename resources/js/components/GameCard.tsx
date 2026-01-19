import { show as dashboardShow } from '@/actions/App/Http/Controllers/DashboardController';
import { Link } from '@inertiajs/react';
import { Star } from 'lucide-react';
import { type FC, useEffect, useRef } from 'react';

// Discriminated union supporting both Game and GameListItem types
type GameCardData = {
    id: number;
    name: string;
    canonical_name?: string | null;
    rating?: number | null;
} & (
    | {
          cover_url: string;
          cover_url_high_res?: string;
          latest_price?: number | string | null;
          currency?: string | null;
      }
    | {
          media: {
              hero_url?: string;
              cover_url_high_res?: string;
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

export const GameCard: FC<GameCardProps> = ({
    game,
    className = '',
}) => {
    const cardRef = useRef<HTMLDivElement>(null);

    // Set view transition name using ref (React inline styles don't support this CSS property)
    useEffect(() => {
        const element = cardRef.current;
        if (element) {
            element.style.viewTransitionName = `game-${game.id}`;
        }
        return () => {
            if (element?.style) {
                element.style.viewTransitionName = '';
            }
        };
    }, [game.id]);

    // Handle different data shapes from different routes
    const isListItem = 'cover_url' in game;

    // High-res priority: Hero -> High-res Cover -> Cover -> Thumb
    const coverUrl = isListItem
        ? game.cover_url_high_res || game.cover_url
        : game.media?.hero_url ||
          game.media?.cover_url_high_res ||
          game.media?.cover_url ||
          game.media?.cover_url_thumb ||
          '/placeholder-game.jpg';

    const rating = isListItem ? game.rating : game.rating;
    const name = isListItem ? game.name : game.canonical_name || game.name;
    // releaseDate unused
    const price = isListItem ? game.latest_price : game.pricing?.amount_major;
    const currency = isListItem ? game.currency : game.pricing?.currency;

    // Use Wayfinder for the link
    // Fallback to legacy path if Wayfinder route fails or doesn't exist
    const wayfinderRoute = dashboardShow(game.id);
    const href = wayfinderRoute?.url || `/dashboard/${game.id}`;

    return (
        <Link
            href={href}
            className={`jewel-case-container mx-auto block w-full max-w-[280px] ${className}`}
        >
            <div ref={cardRef} className="jewel-case group/cd">
                {/* Inner Art Container */}
                <div className="jewel-case-art">
                    <img
                        src={coverUrl}
                        alt={name}
                        className="jewel-case-img"
                        loading="lazy"
                    />

                    {/* Dark gradient overlay for text readability if no bottom bar */}
                    <div className="absolute inset-0 bg-gradient-to-t from-black/80 via-transparent to-transparent opacity-60" />
                </div>

                {/* Frosted Info Bar */}
                <div className="jewel-case-info flex flex-col gap-1 p-3">
                    <h3 className="line-clamp-1 text-sm font-bold text-white drop-shadow-md">
                        {name}
                    </h3>

                    <div className="flex items-center justify-between">
                        {/* Rating Pill */}
                        <div className="flex items-center gap-1 rounded-full border border-white/10 bg-black/40 px-2 py-0.5 text-[10px] font-bold text-white">
                            <Star className="size-3 fill-yellow-400 text-yellow-400" />
                            <span>{rating ? Math.round(rating) : '—'}</span>
                        </div>

                        {/* Price */}
                        {price ? (
                            <span className="text-xs font-black text-cyan-300 drop-shadow-sm">
                                {currency} {price}
                            </span>
                        ) : (
                            <span className="text-[10px] font-bold tracking-wider text-white/50 uppercase">
                                Compare
                            </span>
                        )}
                    </div>
                </div>
            </div>
        </Link>
    );
};
