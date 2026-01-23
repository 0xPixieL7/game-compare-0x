import { type Game } from '@/types';
import { Link } from '@inertiajs/react';
import { Gamepad2, Play, TrendingDown, TrendingUp } from 'lucide-react';
import { MouseEvent, useRef, useState } from 'react';

interface PrismCardProps {
    game: Game;
    href: string;
}

export default function PrismCard({ game, href }: PrismCardProps) {
    const [isPlaying, setIsPlaying] = useState(false);
    const videoRef = useRef<HTMLVideoElement>(null);

    const price = game.pricing?.amount_major;
    const btc = game.pricing?.btc_price;
    const rating = game.rating ? Math.round(game.rating) : null;
    const tone = game.pricing?.is_free ? 'positive' : 'neutral';

    const toneClasses = {
        positive: 'text-emerald-300',
        neutral: 'text-white',
    };

    // Cast media to any to access potential high-res properties not yet in the interface
    const media = game.media as any;
    const videoUrl = media?.trailers?.[0]?.url;

    // Priority: Hero -> High-res Cover -> Cover
    const imageUrl =
        media?.hero_url || media?.cover_url_high_res || media?.cover_url;

    const handleClick = (e: MouseEvent) => {
        if (videoUrl) {
            e.preventDefault();
            e.stopPropagation();

            if (videoRef.current) {
                if (isPlaying) {
                    videoRef.current.pause();
                } else {
                    videoRef.current.play();
                }
                setIsPlaying(!isPlaying);
            }
        }
    };

    return (
        <Link
            href={href}
            onClick={handleClick}
            className="group/prism relative flex h-full flex-col overflow-hidden rounded-3xl border border-white/10 bg-white/5 text-white shadow-[0_20px_60px_-35px_rgba(59,130,246,0.65)] transition-transform duration-500 hover:-translate-y-1 focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-4 focus-visible:outline-blue-400"
        >
            <div className="absolute inset-0">
                {imageUrl ? (
                    <img
                        src={imageUrl}
                        alt={game.canonical_name || game.name}
                        className={`h-full w-full object-cover transition-transform duration-700 group-hover/prism:scale-110 ${
                            isPlaying ? 'opacity-0' : 'opacity-80'
                        }`}
                        loading="lazy"
                    />
                ) : null}

                {videoUrl && (
                    <video
                        ref={videoRef}
                        src={videoUrl}
                        className={`absolute inset-0 h-full w-full object-cover transition-opacity duration-500 ${
                            isPlaying ? 'opacity-100' : 'opacity-0'
                        }`}
                        loop
                        muted={false}
                        playsInline
                        onEnded={() => setIsPlaying(false)}
                    />
                )}

                <div
                    className={`pointer-events-none absolute inset-0 bg-gradient-to-t from-black via-black/30 to-transparent transition-opacity duration-300 ${isPlaying ? 'opacity-0' : 'opacity-100'}`}
                />

                {/* Play button overlay */}
                {videoUrl && !isPlaying && (
                    <div className="absolute inset-0 flex items-center justify-center opacity-0 transition-opacity duration-300 group-hover/prism:opacity-100">
                        <div className="rounded-full bg-white/20 p-4 backdrop-blur-sm">
                            <Play className="h-8 w-8 fill-white text-white" />
                        </div>
                    </div>
                )}
            </div>

            <div
                className={`relative z-10 mt-auto space-y-3 p-4 transition-opacity duration-300 ${isPlaying ? 'opacity-0' : 'opacity-100'}`}
            >
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

            <div
                className={`pointer-events-none absolute inset-0 opacity-0 transition-opacity duration-500 group-hover/prism:opacity-100 ${isPlaying ? '!opacity-0' : ''}`}
            >
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
