import { AppleTvCard } from '@/components/apple-tv-card';
import { type Game, type GameListItem } from '@/types';
import { Link } from '@inertiajs/react';
import { Star } from 'lucide-react';
import { type FC } from 'react';

// Flexible prop type that supports both Game and GameListItem
interface GameCardProps {
    game: any; // Using any for compatibility between slightly different frontend types
    className?: string;
    variant?: 'default' | 'compact';
}

export const GameCard: FC<GameCardProps> = ({ game, className = '', variant = 'default' }) => {
    // Handle different data shapes from different routes
    const isListItem = 'cover_url' in game;
    
    const coverUrl = isListItem 
        ? game.cover_url 
        : (game.media?.cover_url || game.media?.cover_url_thumb || '/placeholder-game.jpg');
    
    const rating = isListItem ? game.rating : game.rating;
    const name = isListItem ? game.name : (game.canonical_name || game.name);
    const releaseDate = isListItem ? game.release_date : game.release_date;
    const price = isListItem ? game.latest_price : game.pricing?.amount_major;
    const currency = isListItem ? game.currency : game.pricing?.currency;

    const href = isListItem ? `/games/${game.id}` : `/dashboard/${game.id}`;

    return (
        <Link href={href} className={`block h-full cursor-default ${className}`}>
            <AppleTvCard className="h-full">
                {/* Media Layer */}
                <div className="absolute inset-0 z-0 h-full w-full">
                    <img
                        src={coverUrl}
                        alt={name}
                        className="h-full w-full object-cover transition-transform duration-700 group-hover/atv:scale-110"
                        loading="lazy"
                    />
                    {/* Shadow/Overlay */}
                    <div className="absolute inset-0 bg-gradient-to-t from-black via-black/20 to-transparent opacity-80" />
                </div>

                {/* Content Layer (Parallax-ish) */}
                <div className="absolute inset-0 z-10 flex flex-col justify-end p-4 text-white">
                    <div className="transform transition-transform duration-500 group-hover/atv:-translate-y-2">
                        <h3 className="line-clamp-2 text-sm font-black tracking-tight drop-shadow-lg lg:text-base">
                            {name}
                        </h3>
                        
                        <div className="mt-2 flex items-center justify-between">
                            <div className="flex items-center gap-1.5 rounded-full bg-white/10 px-2 py-0.5 text-[10px] font-bold backdrop-blur-md border border-white/10">
                                <Star className="size-3 fill-yellow-400 text-yellow-400" />
                                <span>{rating ? Math.round(rating) : '—'}</span>
                            </div>
                            
                            {price ? (
                                <div className="text-right">
                                    <span className="text-xs font-black text-cyan-400 drop-shadow-md">
                                        {currency} {price}
                                    </span>
                                </div>
                            ) : (
                              <span className="text-[10px] text-gray-400 uppercase tracking-tighter font-bold">Compare</span>
                            )}
                        </div>
                    </div>
                </div>
            </AppleTvCard>
        </Link>
    );
};

