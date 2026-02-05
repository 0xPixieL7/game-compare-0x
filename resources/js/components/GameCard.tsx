import { AppleTvCard } from '@/components/apple-tv-card';
import { SelectedGameData, useGameCard } from '@/components/game-detail-modal';
import Image from '@/components/ui/image';
import { useUserPreferences } from '@/Utils/userPreferences';
import { usePage, router } from '@inertiajs/react';
import { motion } from 'framer-motion';
import { Heart, Sparkles, Star } from 'lucide-react';
import React, { type FC, useRef } from 'react';

// Discriminated union supporting both Game and GameListItem types
type GameCardData = {
    id: number;
    name: string;
    canonical_name?: string | null;
    rating?: number | null;
    slug?: string;
    theme?: {
        primary: string;
        accent?: string;
        background?: string;
        surface?: string;
    } | null;
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
    const cardRef = useRef<HTMLDivElement>(null);
    const { openGameCard, isOpen } = useGameCard();
    const { props } = usePage();
    const isAuthenticated = !!(props.auth as any)?.user;
    const { isGameInList, addGameToList, removeGameFromList } =
        useUserPreferences(isAuthenticated);

    const isFavorite = isGameInList('favorites', game.id);

    // Handle different data shapes from different routes
    const isListItem = 'cover_url' in game;

    // High-res priority: High-res Cover -> Cover -> Hero -> Thumb
    const rawCoverUrl = isListItem
        ? game.cover_url_high_res || game.cover_url
        : game.media?.cover_url_high_res ||
          game.media?.cover_url ||
          game.media?.hero_url ||
          game.media?.cover_url_thumb ||
          '/placeholder-game.jpg';

    // Ensure we use high quality images if it's IGDB
    const coverUrl = rawCoverUrl.includes('igdb.com')
        ? rawCoverUrl
              .replace('t_thumb', 't_1080p')
              .replace('t_cover_big', 't_1080p')
        : rawCoverUrl;

    const rating = game.rating;
    const name = isListItem ? game.name : game.canonical_name || game.name;
    const price = isListItem ? game.latest_price : game.pricing?.amount_major;
    const currency = isListItem ? game.currency : game.pricing?.currency;

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

    const toggleFavorite = (e: React.MouseEvent) => {
        e.preventDefault();
        e.stopPropagation();
        if (isFavorite) {
            removeGameFromList('favorites', game.id);
        } else {
            addGameToList('favorites', game.id);
        }
    };

    const handleCardClick = (e: React.MouseEvent) => {
        e.preventDefault();

        // Prepare the game data for the modal
        const gameData: SelectedGameData = {
            id: game.id,
            name,
            coverUrl,
            rating: game.rating,
            theme: game.theme ?? undefined,
        };

        router.visit(`/games/${game.id}`);
    };

    // Preload cover image on hover for instant transitions
    const handleMouseEnter = React.useCallback(() => {
        if (typeof window !== 'undefined') {
            const link = document.createElement('link');
            link.rel = 'prefetch';
            link.as = 'image';
            link.href = coverUrl;
            document.head.appendChild(link);
        }
    }, [coverUrl]);

    return (
        <div ref={cardRef} className={`relative ${className}`}>
            <motion.button
                layoutId={`game-card-${game.id}`}
                onClick={handleCardClick}
                onMouseEnter={handleMouseEnter}
                className="group/card block h-full w-full text-left transition-all duration-500"
                whileHover={{ scale: 1.02 }}
                whileTap={{ scale: 0.98 }}
            >
                <AppleTvCard className="h-full w-full overflow-hidden rounded-2xl border border-white/10 bg-white/5 shadow-2xl backdrop-blur-2xl transition-all duration-500 group-hover/card:border-white/30 group-hover/card:shadow-blue-500/20 group-hover/card:bg-white/10">
                    {/* Background Artwork */}
                    <div className="absolute inset-0 z-0">
                        <motion.div
                            layoutId={`game-cover-${game.id}`}
                            className="absolute inset-0"
                        >
                            <Image
                                src={coverUrl}
                                alt={name}
                                fill
                                className="object-contain transition-transform duration-700 group-hover/card:scale-105"
                            />
                        </motion.div>
                        {/* Spatial Gradient Overlay (Glass Effect) */}
                        <div className="absolute inset-0 bg-gradient-to-t from-black/80 via-black/20 to-transparent transition-opacity duration-500 group-hover/card:opacity-80" />

                        {/* Interactive Spatial Shine */}
                        <div className="pointer-events-none absolute inset-0 z-20 opacity-0 transition-opacity duration-500 group-hover/card:opacity-100">
                            <div className="absolute inset-0 bg-gradient-to-tr from-white/10 via-transparent to-white/5" />
                        </div>
                    </div>

                    {/* Content Overlay */}
                    <div className="relative z-10 flex h-full flex-col justify-between p-4">
                        {/* Top Section */}
                        <div className="flex items-center justify-between">
                            <div
                                className={`flex items-center gap-1.5 rounded-full border px-2 py-0.5 font-mono text-[9px] font-black tracking-wider backdrop-blur-md transition-all duration-300 ${labelColor}`}
                            >
                                <div className="h-1 w-1 animate-pulse rounded-full bg-current" />
                                {label}
                            </div>

                            {rating && (
                                <motion.div
                                    layoutId={`game-rating-${game.id}`}
                                    className="flex items-center gap-1 rounded-full border border-white/10 bg-black/40 px-2.5 py-1 text-[10px] font-black text-white backdrop-blur-md transition-all duration-300 group-hover/card:border-white/20"
                                >
                                    <Star className="h-3 w-3 fill-yellow-400 text-yellow-400" />
                                    <span>{Math.round(rating)}</span>
                                </motion.div>
                            )}
                        </div>

                        {/* Bottom Section */}
                        <div className="relative space-y-3">
                            {/* Game Name Badge (Normal) */}
                            <motion.div
                                layoutId={`game-title-${game.id}`}
                                className="inline-flex items-center gap-2 rounded-full border border-white/20 bg-black/80 px-3 py-1.5 backdrop-blur-md transition-opacity duration-300 group-hover/card:opacity-0"
                            >
                                <h3 className="line-clamp-1 text-xs font-bold tracking-tight text-white">
                                    {name}
                                </h3>
                            </motion.div>

                            {/* Full Name Hover Label (Swapped in) */}
                            <div className="pointer-events-none absolute inset-x-0 bottom-[3.25rem] z-50 flex translate-y-2 justify-center opacity-0 transition-all duration-300 group-hover/card:translate-y-0 group-hover/card:opacity-100">
                                <div className="max-w-full rounded-xl border border-white/10 bg-black/90 px-4 py-2 text-center shadow-2xl backdrop-blur-xl">
                                    <span className="text-sm leading-tight font-bold text-white shadow-black drop-shadow-md">
                                        {name}
                                    </span>
                                </div>
                            </div>

                            <div className="flex items-center justify-between border-t border-white/10 pt-3">
                                <div className="flex items-center gap-1.5 text-[9px] font-bold tracking-widest text-slate-400 uppercase">
                                    <Sparkles className="h-3 w-3 text-blue-400/80" />
                                    <span>Verified</span>
                                </div>

                                {price ? (
                                    <div className="rounded-full bg-blue-500/10 px-2 py-0.5">
                                        <span className="text-xs font-black text-blue-400">
                                            {currency} {price}
                                        </span>
                                    </div>
                                ) : (
                                    <span className="font-mono text-[9px] font-black tracking-tighter text-white/30 uppercase group-hover/card:text-white/50">
                                        ANALYZE
                                    </span>
                                )}
                            </div>
                        </div>
                    </div>
                </AppleTvCard>
            </motion.button>

            {/* Favorite Button - Separate from main button to prevent navigation */}
            <button
                onClick={toggleFavorite}
                className={`absolute top-12 right-4 z-20 flex h-8 w-8 items-center justify-center rounded-full border border-white/10 backdrop-blur-md transition-all duration-300 hover:scale-110 active:scale-95 ${
                    isFavorite
                        ? 'border-rose-400/50 bg-rose-500 text-white shadow-lg shadow-rose-500/20'
                        : 'bg-black/40 text-white/70 hover:bg-black/60 hover:text-white'
                }`}
            >
                <Heart
                    className={`h-4 w-4 ${isFavorite ? 'fill-current' : ''}`}
                />
            </button>
        </div>
    );
};
