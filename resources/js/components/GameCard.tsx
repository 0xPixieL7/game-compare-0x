import { type Game } from '@/types';
import { Link } from '@inertiajs/react';
import { type FC } from 'react';

interface GameCardProps {
    game: Game;
    className?: string;
}

export const GameCard: FC<GameCardProps> = ({ game, className = '' }) => {
    // Prefer thumbnail for list view performance
    const coverUrl =
        game.media.cover_url_thumb ||
        game.media.cover_url ||
        '/placeholder-game.jpg';

    const formatBtc = (amount: number) => {
        return `₿${amount.toFixed(8)}`;
    };

    const formatFiat = (amount: number, currency: string) => {
        return new Intl.NumberFormat('en-US', {
            style: 'currency',
            currency: currency,
        }).format(amount);
    };

    return (
        <Link
            href={`/dashboard/${game.id}`} // Assuming detailed view is here
            className={`group relative block h-full overflow-hidden rounded-xl bg-gray-900 shadow-lg transition-all hover:z-10 hover:scale-105 hover:shadow-2xl hover:ring-2 hover:ring-blue-500/50 ${className}`}
        >
            {/* Cover Image */}
            <div className="aspect-[2/3] w-full overflow-hidden">
                <img
                    src={coverUrl}
                    alt={game.name}
                    className="h-full w-full object-cover transition-transform duration-500 group-hover:scale-110"
                    loading="lazy"
                />

                {/* Gradient Overlay */}
                <div className="absolute inset-0 bg-linear-to-t from-black/90 via-black/40 to-transparent opacity-60 transition-opacity duration-300 group-hover:opacity-80" />
            </div>

            {/* Content Overlay */}
            <div className="absolute inset-0 flex flex-col justify-end p-4">
                <h3 className="mb-1 line-clamp-2 text-sm font-bold text-white drop-shadow-md">
                    {game.canonical_name || game.name}
                </h3>

                {/* Meta Row: Rating + Date */}
                <div className="mb-2 flex items-center justify-between text-xs text-gray-300">
                    {game.rating && (
                        <div className="flex items-center gap-1 rounded bg-black/40 px-1.5 py-0.5 backdrop-blur-sm">
                            <span className="text-yellow-400">★</span>
                            <span>{Math.round(game.rating)}</span>
                        </div>
                    )}
                    {game.release_date && (
                        <span className="opacity-80">
                            {new Date(game.release_date).getFullYear()}
                        </span>
                    )}
                </div>

                {/* Pricing */}
                {game.pricing ? (
                    <div className="flex flex-col gap-1">
                        <div className="flex items-center justify-between">
                            <span className="text-xs font-medium text-emerald-400">
                                {game.pricing.is_free
                                    ? 'FREE'
                                    : formatFiat(
                                          game.pricing.amount_major,
                                          game.pricing.currency,
                                      )}
                            </span>
                            {/* Retailer Badge */}
                            {game.pricing.retailer && (
                                <span className="rounded bg-white/10 px-1.5 py-0.5 text-[10px] text-white/80 uppercase">
                                    {game.pricing.retailer}
                                </span>
                            )}
                        </div>

                        {/* BTC Price */}
                        {game.pricing.btc_price !== null &&
                            !game.pricing.is_free && (
                                <div className="flex items-center gap-1 font-mono text-xs text-orange-400">
                                    <span className="opacity-75"></span>
                                    {formatBtc(game.pricing.btc_price)}
                                </div>
                            )}
                    </div>
                ) : (
                    <div className="mt-1 text-xs text-gray-500 italic">
                        Login for prices
                    </div>
                )}
            </div>
        </Link>
    );
};
