import AppLayout from '@/layouts/app-layout';
import { Skeleton } from '@/components/ui/skeleton';
import {
    FeaturedGame,
    GameListItem,
    PageProps,
    PaginatedCollection,
    BreadcrumbItem
} from '@/types';
import { GameCard } from '@/components/GameCard';
import { Deferred, Head, Link } from '@inertiajs/react';
import {
    Calendar,
    Filter,
    Grid2X2,
    List,
    Play,
    Search,
    Star,
} from 'lucide-react';
import { useState } from 'react';

interface Props extends PageProps {
    featuredGame: FeaturedGame | null;
    games?: PaginatedCollection<GameListItem>;
    filters: {
        sort: string;
    };
}

const breadcrumbs: BreadcrumbItem[] = [
    {
        title: 'Video Games',
        href: '/games',
    },
];

const GamesGridSkeleton = () => (
    <div className="grid gap-6 grid-cols-2 md:grid-cols-3 lg:grid-cols-4 xl:grid-cols-5">
        {Array.from({ length: 10 }).map((_, i) => (
            <div key={i} className="bg-neutral-900 border border-neutral-800 rounded-2xl overflow-hidden aspect-[3/4]">
                <Skeleton className="w-full h-full bg-neutral-800" />
            </div>
        ))}
    </div>
);

export default function Index({ featuredGame, games, filters }: Props) {
    const [viewMode, setViewMode] = useState<'grid' | 'list'>('grid');

    return (
        <AppLayout breadcrumbs={breadcrumbs}>
            <Head title="Explore Video Games" />

            <div className="flex h-full flex-1 flex-col gap-8 p-6 lg:p-10 bg-neutral-950 text-neutral-100 overflow-y-auto">
                {/* Header & Search */}
                <div className="flex flex-col md:flex-row justify-between items-start md:items-center gap-6">
                    <div>
                        <h1 className="text-4xl font-extrabold tracking-tight bg-gradient-to-r from-indigo-400 to-cyan-400 bg-clip-text text-transparent">
                            Game Universe
                        </h1>
                        <p className="text-neutral-400 mt-2 font-medium">
                            Discover your next obsession.
                        </p>
                    </div>

                    <div className="relative w-full md:w-96 group">
                        <Search className="absolute left-3 top-1/2 -translate-y-1/2 size-4 text-neutral-500 group-focus-within:text-indigo-400 transition-colors" />
                        <input
                            type="text"
                            placeholder="Search games, genres, platforms..."
                            className="w-full bg-neutral-900/50 border border-neutral-800 rounded-full py-2.5 pl-10 pr-4 focus:outline-none focus:ring-2 focus:ring-indigo-500/50 focus:border-indigo-500 transition-all placeholder:text-neutral-600 shadow-xl"
                        />
                    </div>
                </div>

                {/* Featured Section */}
                {featuredGame && (
                    <section className="relative group overflow-hidden rounded-3xl border border-neutral-800 shadow-2xl bg-neutral-900">
                        <div
                            className="absolute inset-0 opacity-20 blur-3xl scale-110"
                            style={{
                                backgroundImage: `url(${featuredGame.cover_url})`,
                                backgroundSize: 'cover',
                            }}
                        />

                        <div className="relative flex flex-col xl:flex-row gap-8 p-6 lg:p-10">
                            <div className="flex-1 aspect-video rounded-2xl overflow-hidden shadow-2xl border border-white/5 bg-black relative group/player">
                                {featuredGame.trailer_url ? (
                                    <iframe
                                        src={`${featuredGame.trailer_url}?autoplay=0&mute=1&controls=0&loop=1`}
                                        className="w-full h-full pointer-events-none"
                                        title={featuredGame.name}
                                    />
                                ) : (
                                    <img
                                        src={featuredGame.cover_url}
                                        className="w-full h-full object-cover"
                                        alt={featuredGame.name}
                                    />
                                )}

                                <div className="absolute inset-0 bg-gradient-to-t from-black/80 via-transparent to-transparent opacity-0 group-hover/player:opacity-100 transition-opacity duration-500 flex items-end p-6">
                                    <button className="flex items-center gap-2 bg-indigo-600 hover:bg-indigo-500 text-white px-6 py-3 rounded-full font-bold shadow-xl transition-all transform hover:scale-105 active:scale-95">
                                        <Play className="fill-current size-5" />
                                        Watch Trailer
                                    </button>
                                </div>
                            </div>

                            <div className="flex-1 flex flex-col justify-center">
                                <div className="flex items-center gap-3 mb-4">
                                    <span className="bg-indigo-500/10 text-indigo-400 border border-indigo-500/20 px-3 py-1 rounded-full text-xs font-bold uppercase tracking-widest leading-none">
                                        Featured Game
                                    </span>
                                    <div className="flex items-center gap-1 text-amber-400">
                                        <Star className="fill-current size-4" />
                                        <span className="font-bold">
                                            {featuredGame.rating}
                                        </span>
                                    </div>
                                </div>

                                <h2 className="text-4xl lg:text-6xl font-black mb-6 leading-[1.1]">
                                    {featuredGame.name}
                                </h2>

                                <p className="text-neutral-400 text-lg leading-relaxed mb-8 line-clamp-4 max-w-2xl">
                                    {featuredGame.description}
                                </p>

                                <div className="flex flex-wrap gap-4">
                                    <Link
                                        href={`/games/${featuredGame.id}`}
                                        className="inline-flex items-center justify-center gap-2 bg-white text-black px-8 py-4 rounded-xl font-bold shadow-xl hover:bg-neutral-200 transition-all transform active:scale-95"
                                    >
                                        Compare Prices
                                    </Link>
                                    <button className="inline-flex items-center justify-center gap-2 bg-neutral-800 text-white px-8 py-4 rounded-xl font-bold shadow-xl border border-neutral-700 hover:bg-neutral-700 transition-all transform active:scale-95">
                                        Add to Wishlist
                                    </button>
                                </div>
                            </div>
                        </div>
                    </section>
                )}

                {/* Playlist / Grid Section */}
                <div className="mt-8">
                    <div className="flex justify-between items-center mb-8 border-b border-neutral-800 pb-5">
                        <div className="flex items-center gap-8">
                            <h3 className="text-2xl font-bold">
                                Latest Discoveries
                            </h3>
                            <nav className="hidden md:flex items-center gap-6">
                                <button className="text-indigo-400 font-bold relative after:absolute after:bottom-[-21px] after:left-0 after:w-full after:h-1 after:bg-indigo-400 after:rounded-full">
                                    All Games
                                </button>
                                <button className="text-neutral-500 hover:text-neutral-300 font-medium transition-colors">
                                    Trending
                                </button>
                                <button className="text-neutral-500 hover:text-neutral-300 font-medium transition-colors">
                                    Top Sellers
                                </button>
                                <button className="text-neutral-500 hover:text-neutral-300 font-medium transition-colors">
                                    New Releases
                                </button>
                            </nav>
                        </div>

                        <div className="flex items-center gap-4 bg-neutral-900/50 p-1.5 rounded-xl border border-neutral-800 shadow-inner">
                            <button
                                onClick={() => setViewMode('grid')}
                                className={`p-2 rounded-lg transition-all ${viewMode === 'grid' ? 'bg-neutral-800 text-white shadow-lg' : 'text-neutral-500 hover:text-neutral-300'}`}
                            >
                                <Grid2X2 className="size-5" />
                            </button>
                            <button
                                onClick={() => setViewMode('list')}
                                className={`p-2 rounded-lg transition-all ${viewMode === 'list' ? 'bg-neutral-800 text-white shadow-lg' : 'text-neutral-500 hover:text-neutral-300'}`}
                            >
                                <List className="size-5" />
                            </button>
                            <div className="w-px h-6 bg-neutral-800 mx-1" />
                            <button className="p-2 text-neutral-500 hover:text-neutral-300 transition-colors">
                                <Filter className="size-5" />
                            </button>
                        </div>
                    </div>

                    {/* Games Grid with Deferral */}
                    <Deferred data="games" fallback={<GamesGridSkeleton />}>
                        {games && (
                            <>
                                <div
                                    className={`grid gap-6 ${viewMode === 'grid' ? 'grid-cols-2 md:grid-cols-3 lg:grid-cols-4 xl:grid-cols-5' : 'grid-cols-1'}`}
                                >
                                    {games.data.map((game) => (
                                        <GameCard 
                                            key={game.id} 
                                            game={game} 
                                            className={viewMode === 'list' ? 'h-32' : 'aspect-[3/4]'}
                                        />
                                    ))}
                                </div>

                                {/* Pagination */}
                                <div className="mt-12 flex justify-center pb-12">
                                    <div className="flex items-center gap-2">
                                        {games.links.map((link, i) =>
                                            link.url ? (
                                                <Link
                                                    key={i}
                                                    href={link.url}
                                                    className={`w-10 h-10 flex items-center justify-center rounded-lg font-bold transition-all ${link.active ? 'bg-indigo-600 text-white shadow-lg shadow-indigo-500/20' : 'bg-neutral-900 border border-neutral-800 text-neutral-500 hover:border-neutral-700'}`}
                                                    dangerouslySetInnerHTML={{
                                                        __html: link.label,
                                                    }}
                                                />
                                            ) : (
                                                <span
                                                    key={i}
                                                    className="w-10 h-10 flex items-center justify-center rounded-lg text-neutral-600"
                                                    dangerouslySetInnerHTML={{
                                                        __html: link.label,
                                                    }}
                                                />
                                            ),
                                        )}
                                    </div>
                                </div>
                            </>
                        )}
                    </Deferred>
                </div>
            </div>

            <div className="fixed inset-0 pointer-events-none bg-[radial-gradient(circle_at_top_right,rgba(79,70,229,0.05),transparent_50%),radial-gradient(circle_at_bottom_left,rgba(6,182,212,0.05),transparent_50%)]" />
        </AppLayout>
    );
}
