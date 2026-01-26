import { SpotlightCarousel } from '@/components/compare/spotlight-carousel';
import EndlessCarousel from '@/components/EndlessCarousel';
import { GameCard } from '@/components/GameCard';
import Header from '@/components/Header';
import { useUserPreferences } from '@/Utils/userPreferences';
import { Head, usePage } from '@inertiajs/react';
import { motion } from 'framer-motion';
import { Search } from 'lucide-react';
import React, { useEffect, useState } from 'react';

interface Game {
    id: number;
    name: string;
    canonical_name: string;
    rating: number;
    release_date: string;
    media: {
        cover_url: string;
        cover_url_thumb: string;
        screenshots: Array<{ url: string; width: number; height: number }>;
        trailers: Array<{
            url?: string;
            thumbnail?: string;
            name?: string;
            video_id?: string;
        }>;
    };
}

interface CarouselRow {
    id: string;
    title: string;
    type: 'user_list' | 'recent' | 'genre' | 'top_rated' | 'new_releases';
    games: Game[];
    genre?: string;
    description: string;
}

interface Props {
    hero: any;
    spotlightGames: any[];
    carouselRows: CarouselRow[];
    searchResults: Game[];
    search: string;
    meta: {
        total_rows: number;
        query_time: number;
    };
}

export default function DashboardIndex({
    hero,
    spotlightGames,
    carouselRows,
    searchResults,
    search,
    meta,
}: Props) {
    const { props } = usePage();
    const isAuthenticated = !!(props.auth as any)?.user;
    const [searchTerm, setSearchTerm] = useState(search);
    const [isLoading, setIsLoading] = useState(false);
    const [populatedRows, setPopulatedRows] = useState<CarouselRow[]>([]);

    // User preferences hook (REACTIVE)
    const preferences = useUserPreferences(isAuthenticated);

    // Populate user preference and recent rows on the frontend
    useEffect(() => {
        const processedRows = carouselRows.map((row) => {
            if (row.type === 'user_list') {
                const favoritesList = preferences
                    .getLists()
                    .find((l) => l.id === 'favorites');
                const wishList = preferences
                    .getLists()
                    .find((l) => l.id === 'wishlist');

                const allUserGameIds = new Set([
                    ...(favoritesList?.games || []),
                    ...(wishList?.games || []),
                ]);

                // Find matching games from all carousel rows
                const userGames: Game[] = [];
                carouselRows.forEach((otherRow) => {
                    if (
                        otherRow.type !== 'user_list' &&
                        otherRow.type !== 'recent'
                    ) {
                        otherRow.games.forEach((game) => {
                            if (
                                allUserGameIds.has(game.id) &&
                                !userGames.find((g) => g.id === game.id)
                            ) {
                                userGames.push(game);
                            }
                        });
                    }
                });

                return { ...row, games: userGames };
            }

            if (row.type === 'recent') {
                const recentGameIds = preferences.getRecentlyViewed();
                const recentGames: Game[] = [];

                recentGameIds.forEach((gameId) => {
                    carouselRows.forEach((otherRow) => {
                        if (
                            otherRow.type !== 'user_list' &&
                            otherRow.type !== 'recent'
                        ) {
                            const game = otherRow.games.find(
                                (g) => g.id === gameId,
                            );
                            if (
                                game &&
                                !recentGames.find((g) => g.id === game.id)
                            ) {
                                recentGames.push(game);
                            }
                        }
                    });
                });

                return { ...row, games: recentGames };
            }

            return row;
        });

        setPopulatedRows(processedRows);
    }, [carouselRows, preferences.lists, preferences.recentlyViewed]);

    const handleSearch = (e: React.FormEvent) => {
        e.preventDefault();
        if (!searchTerm.trim()) return;
        setIsLoading(true);
        window.location.href = `/dashboard?search=${encodeURIComponent(searchTerm)}`;
    };

    const validRows = populatedRows.filter((row) => row.games.length > 0);

    return (
        <>
            <Head title="Game Dashboard" />

            <div className="min-h-screen bg-[#050505] text-white selection:bg-blue-500 selection:text-white">
                <Header />

                {/* Sub-header with search integrated into a premium bar */}
                <div className="sticky top-16 z-40 border-b border-white/5 bg-black/40 backdrop-blur-xl">
                    <div className="mx-auto max-w-7xl px-4 sm:px-6 lg:px-8">
                        <div className="flex h-14 items-center justify-between">
                            <div className="flex items-center space-x-4">
                                <h1 className="text-xl font-bold tracking-tight text-white/90">
                                    Browse Universe
                                </h1>
                                <div className="hidden items-center gap-1.5 rounded-full border border-blue-500/10 bg-blue-500/5 px-3 py-1 text-[9px] font-bold tracking-[0.2em] text-blue-400 uppercase sm:flex">
                                    <div className="h-1.5 w-1.5 animate-pulse rounded-full bg-blue-400" />
                                    {isAuthenticated
                                        ? 'Titan Node Active'
                                        : 'Guest Signal'}
                                </div>
                            </div>

                            {/* Search */}
                            <form
                                onSubmit={handleSearch}
                                className="flex items-center"
                            >
                                <div className="group relative">
                                    <div className="absolute inset-0 bg-blue-500/5 opacity-0 blur-md transition-opacity group-focus-within:opacity-100" />
                                    <input
                                        type="text"
                                        value={searchTerm}
                                        onChange={(e) =>
                                            setSearchTerm(e.target.value)
                                        }
                                        placeholder="Scan protocols..."
                                        className="relative w-48 rounded-full border border-white/5 bg-white/5 px-4 py-1.5 text-xs text-white placeholder-gray-500 transition-all focus:w-64 focus:border-blue-500/30 focus:bg-white/10 focus:outline-none sm:w-64"
                                    />
                                    <button
                                        type="submit"
                                        disabled={isLoading}
                                        className="absolute top-1/2 right-3 -translate-y-1/2 transform text-gray-500 transition-colors hover:text-white"
                                    >
                                        <Search className="h-3.5 w-3.5" />
                                    </button>
                                </div>
                            </form>
                        </div>
                    </div>
                </div>

                <main className="relative z-10">
                    {/* Spotlight Section */}
                    {!search && (
                        <div className="relative -mt-16 mb-8 overflow-hidden">
                            <SpotlightCarousel
                                spotlight={spotlightGames}
                                hero={hero}
                            />
                            <div className="pointer-events-none absolute right-0 bottom-0 left-0 h-64 bg-gradient-to-t from-[#050505] to-transparent" />
                        </div>
                    )}

                    <div className="mx-auto max-w-[100rem] px-4 pb-24 sm:px-6 lg:px-12">
                        {/* Search Results */}
                        {search && searchResults.length > 0 && (
                            <div className="mb-12 pt-8">
                                <h2 className="mb-8 text-2xl font-bold tracking-tight text-white">
                                    Signals detected for{' '}
                                    <span className="text-blue-400">
                                        "{search}"
                                    </span>
                                </h2>
                                <div className="grid grid-cols-2 gap-6 sm:grid-cols-2 md:grid-cols-3 lg:grid-cols-4 xl:grid-cols-4 2xl:grid-cols-5">
                                    {searchResults.map((game, i) => (
                                        <motion.div
                                            key={game.id}
                                            initial={{ opacity: 0, y: 20 }}
                                            animate={{ opacity: 1, y: 0 }}
                                            transition={{ delay: i * 0.05 }}
                                            onClick={() =>
                                                preferences.addToRecentlyViewed(
                                                    game.id,
                                                )
                                            }
                                        >
                                            <GameCard
                                                game={game}
                                                className="aspect-[3/4]"
                                            />
                                        </motion.div>
                                    ))}
                                </div>
                            </div>
                        )}

                        {/* Search state message */}
                        {search && searchResults.length === 0 && (
                            <div className="py-48 text-center">
                                <div className="text-2xl font-black tracking-widest text-gray-700 uppercase">
                                    Zero Results Detected
                                </div>
                                <p className="mt-4 font-medium text-gray-500">
                                    Try refining your search parameters or
                                    provider IDs.
                                </p>
                                <button
                                    onClick={() => window.history.back()}
                                    className="mt-8 rounded-full border border-white/10 bg-white/5 px-6 py-2 text-xs font-bold tracking-widest text-white uppercase hover:bg-white/10"
                                >
                                    Return to Nexus
                                </button>
                            </div>
                        )}

                        {/* Carousel Rows */}
                        {!search && (
                            <div className="space-y-12">
                                {validRows.map((row, i) => {
                                    const carouselGames = row.games.map(
                                        (game) => ({
                                            ...game,
                                            media: {
                                                ...game.media,
                                                cover_url:
                                                    game.media
                                                        .cover_url_thumb ||
                                                    game.media.cover_url,
                                                cover_url_thumb:
                                                    game.media
                                                        .cover_url_thumb ||
                                                    game.media.cover_url,
                                                cover: {
                                                    url:
                                                        game.media
                                                            .cover_url_thumb ||
                                                        game.media.cover_url,
                                                    width: 0,
                                                    height: 0,
                                                },
                                            },
                                        }),
                                    ) as any[];

                                    return (
                                        <motion.div
                                            key={row.id}
                                            initial={{ opacity: 0, x: -20 }}
                                            whileInView={{ opacity: 1, x: 0 }}
                                            viewport={{
                                                once: true,
                                                margin: '-100px',
                                            }}
                                            transition={{
                                                duration: 0.8,
                                                delay: i * 0.1,
                                            }}
                                            onClick={(e) => {
                                                const target =
                                                    e.target as HTMLElement;
                                                const gameLink =
                                                    target.closest('a');
                                                if (gameLink) {
                                                    const href =
                                                        gameLink.getAttribute(
                                                            'href',
                                                        );
                                                    const match =
                                                        href?.match(
                                                            /\/dashboard\/(\d+)/,
                                                        );
                                                    if (match) {
                                                        preferences.addToRecentlyViewed(
                                                            parseInt(match[1]),
                                                        );
                                                    }
                                                }
                                            }}
                                            className="group relative"
                                        >
                                            <div className="absolute top-0 bottom-0 -left-6 hidden w-1 rounded-full bg-gradient-to-b from-blue-500/40 to-transparent opacity-0 transition-opacity group-hover:opacity-100 lg:block" />
                                            <EndlessCarousel
                                                title={row.title}
                                                games={carouselGames}
                                                className="mb-4"
                                            />
                                        </motion.div>
                                    );
                                })}
                            </div>
                        )}

                        {/* Performance & Meta */}
                        {!search && (
                            <motion.div
                                initial={{ opacity: 0 }}
                                whileInView={{ opacity: 1 }}
                                viewport={{ once: true }}
                                className="mt-32 rounded-3xl border border-white/5 bg-white/[0.01] p-8 backdrop-blur-2xl"
                            >
                                <div className="flex flex-wrap items-center justify-between gap-8 text-[10px] font-black tracking-[0.2em] text-gray-500 uppercase">
                                    <div className="flex items-center gap-6">
                                        <span className="flex items-center gap-2">
                                            <span className="h-1.5 w-1.5 rounded-full bg-blue-500" />
                                            {meta.total_rows} Datastreams
                                            Synthesized
                                        </span>
                                        <span className="h-4 w-px bg-white/10" />
                                        <span>Version 2.4.9 Omega</span>
                                    </div>
                                    <div className="flex items-center gap-3">
                                        <div className="h-2 w-2 animate-pulse rounded-full bg-emerald-500/50 shadow-[0_0_10px_rgba(16,185,129,0.5)]" />
                                        <span>
                                            System Latency:{' '}
                                            {(meta.query_time * 1000).toFixed(
                                                2,
                                            )}
                                            ms
                                        </span>
                                    </div>
                                </div>
                            </motion.div>
                        )}
                    </div>
                </main>
            </div>
        </>
    );
}
