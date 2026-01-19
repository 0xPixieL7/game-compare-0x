import { type Game } from '@/types';
import { Link } from '@inertiajs/react';
import { Gamepad2, TrendingDown, TrendingUp } from 'lucide-react';

interface PrismCardProps {
    game: Game;
    href: string;
}

export default function PrismCard({ game, href }: PrismCardProps) {
    const price = game.pricing?.amount_major;
    const btc = game.pricing?.btc_price;
    const rating = game.rating ? Math.round(game.rating) : null;
    const tone = game.pricing?.is_free ? 'positive' : 'neutral';

    const toneClasses = {
        positive: 'text-emerald-300',
        neutral: 'text-white',
    };

    return (
        <Link
            href={href}
            className="group/prism relative flex h-full flex-col overflow-hidden rounded-3xl border border-white/10 bg-white/5 text-white shadow-[0_20px_60px_-35px_rgba(59,130,246,0.65)] transition-transform duration-500 hover:-translate-y-1 focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-4 focus-visible:outline-blue-400"
        >
            <div className="absolute inset-0">
                {game.media?.cover_url ? (
                    <img
                        src={game.media.cover_url}
                        alt={game.canonical_name || game.name}
                        className="h-full w-full object-cover opacity-80 transition-transform duration-700 group-hover/prism:scale-110"
                        loading="lazy"
                    />
                ) : null}
                <div className="absolute inset-0 bg-gradient-to-t from-black via-black/30 to-transparent" />
            </div>

            <div className="relative z-10 mt-auto space-y-3 p-4">
                <div className="flex items-center gap-2 text-xs tracking-[0.28em] text-white/60 uppercase">
                    <Gamepad2 className="h-4 w-4" />
                    {game.genres?.[0] ?? 'Featured'}
                </div>
                <h3 className="line-clamp-2 text-base font-semibold">
                    {game.canonical_name || game.name}
                </h3>
                <div className="flex items-center justify-between">
                    <div className="flex items-center gap-2 text-xs text-white/70">
                        {rating ? (
                            <span>{rating} rating</span>
                        ) : (
                            <span>No rating</span>
                        )}
                    </div>
                    {price ? (
                        <div className="text-right">
                            <div
                                className={`text-sm font-semibold ${toneClasses[tone]}`}
                            >
                                {game.pricing?.currency} {price}
                            </div>
                            {btc ? (
                                <div className="text-xs text-blue-200/80">
                                    {btc.toFixed(6)} BTC
                                </div>
                            ) : null}
                        </div>
                    ) : (
                        <span className="text-xs tracking-[0.2em] text-white/50 uppercase">
                            Compare
                        </span>
                    )}
                </div>
            </div>

            <div className="pointer-events-none absolute inset-0 opacity-0 transition-opacity duration-500 group-hover/prism:opacity-100">
                <div className="absolute top-12 -right-12 h-24 w-24 rounded-full bg-blue-500/40 blur-2xl" />
                <div className="absolute bottom-12 -left-12 h-24 w-24 rounded-full bg-violet-500/40 blur-2xl" />
                <div className="absolute right-4 bottom-4 flex items-center gap-2 rounded-full border border-white/10 bg-black/60 px-3 py-1 text-[10px] tracking-[0.2em] text-white/70 uppercase">
                    {game.pricing?.is_free ? (
                        <TrendingDown className="h-3 w-3 text-emerald-300" />
                    ) : (
                        <TrendingUp className="h-3 w-3 text-blue-300" />
                    )}
                    Signal
                </div>
            </div>
        </Link>
    );
}
