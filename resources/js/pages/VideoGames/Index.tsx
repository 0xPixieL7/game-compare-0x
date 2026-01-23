import { GameCard } from '@/components/GameCard';
import MediaPlayer from '@/components/MediaPlayer';
import { Skeleton } from '@/components/ui/skeleton';
import AppLayout from '@/layouts/app-layout';
import {
    BreadcrumbItem,
    FeaturedGame,
    GameListItem,
    PageProps,
    PaginatedCollection,
} from '@/types';
import { Deferred, Head, Link } from '@inertiajs/react';
import { Filter, Grid2X2, List, Search, Star } from 'lucide-react';
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
    <div className="grid grid-cols-2 gap-6 md:grid-cols-3 lg:grid-cols-4 xl:grid-cols-5">
        {Array.from({ length: 10 }).map((_, i) => (
            <div
                key={i}
                className="aspect-[2/3] overflow-hidden rounded-2xl border border-neutral-800 bg-neutral-900"
            >
                <Skeleton className="h-full w-full bg-neutral-800" />
            </div>
        ))}
    </div>
);

export default function Index({ featuredGame, games, filters }: Props) {
    const [viewMode, setViewMode] = useState<'grid' | 'list'>('grid');

    return (
        <AppLayout breadcrumbs={breadcrumbs}>
            <Head title="Explore Video Games" />

            <div className="flex h-full flex-1 flex-col gap-8 overflow-y-auto bg-neutral-950 p-6 text-neutral-100 lg:p-10">
                {/* Header & Search */}
                <div className="flex flex-col items-start justify-between gap-6 md:flex-row md:items-center">
                    <div>
                        <h1 className="bg-gradient-to-r from-indigo-400 to-cyan-400 bg-clip-text text-4xl font-extrabold tracking-tight text-transparent">
                            Game Universe
                        </h1>
                        <p className="mt-2 font-medium text-neutral-400">
                            Discover your next obsession.
                        </p>
                    </div>

                    <div className="group relative w-full md:w-96">
                        <Search className="absolute top-1/2 left-3 size-4 -translate-y-1/2 text-neutral-500 transition-colors group-focus-within:text-indigo-400" />
                        <input
                            type="text"
                            placeholder="Search games, genres, platforms..."
                            className="w-full rounded-full border border-neutral-800 bg-neutral-900/50 py-2.5 pr-4 pl-10 shadow-xl transition-all placeholder:text-neutral-600 focus:border-indigo-500 focus:ring-2 focus:ring-indigo-500/50 focus:outline-none"
                        />
                    </div>
                </div>

                {/* Featured Section */}
                {featuredGame && (
                    <section className="group relative overflow-hidden rounded-3xl border border-neutral-800 bg-neutral-900 shadow-2xl">
                        <div
                            className="absolute inset-0 scale-110 opacity-20 blur-3xl"
                            style={{
                                backgroundImage: `url(${featuredGame.cover_url})`,
                                backgroundSize: 'cover',
                            }}
                        />

                        <div className="relative flex flex-col gap-8 p-6 lg:p-10 xl:flex-row">
                            <div className="group/player relative aspect-video flex-1 overflow-hidden rounded-2xl border border-white/5 bg-black shadow-2xl">
                                {featuredGame.trailer_url ? (
                                    <MediaPlayer
                                        url={featuredGame.trailer_url}
                                        thumbnail={featuredGame.cover_url}
                                        title={featuredGame.name}
                                        className="h-full w-full"
                                    />
                                ) : (
                                    <img
                                        src={featuredGame.cover_url}
                                        className="h-full w-full object-cover"
                                        alt={featuredGame.name}
                                    />
                                )}
                            </div>

                            <div className="flex flex-1 flex-col justify-center">
                                <div className="mb-4 flex items-center gap-3">
                                    <span className="rounded-full border border-indigo-500/20 bg-indigo-500/10 px-3 py-1 text-xs leading-none font-bold tracking-widest text-indigo-400 uppercase">
                                        Featured Game
                                    </span>
                                    <div className="flex items-center gap-1 text-amber-400">
                                        <Star className="size-4 fill-current" />
                                        <span className="font-bold">
                                            {featuredGame.rating}
                                        </span>
                                    </div>
                                </div>

                                <h2 className="mb-6 text-4xl leading-[1.1] font-black lg:text-6xl">
                                    {featuredGame.name}
                                </h2>

                                <p className="mb-8 line-clamp-4 max-w-2xl text-lg leading-relaxed text-neutral-400">
                                    {featuredGame.description}
                                </p>

                                <div className="flex flex-wrap gap-4">
                                    <Link
                                        href={`/games/${featuredGame.id}`}
                                        className="inline-flex transform items-center justify-center gap-2 rounded-xl bg-white px-8 py-4 font-bold text-black shadow-xl transition-all hover:bg-neutral-200 active:scale-95"
                                    >
                                        Compare Prices
                                    </Link>
                                    <button className="inline-flex transform items-center justify-center gap-2 rounded-xl border border-neutral-700 bg-neutral-800 px-8 py-4 font-bold text-white shadow-xl transition-all hover:bg-neutral-700 active:scale-95">
                                        Add to Wishlist
                                    </button>
                                </div>
                            </div>
                        </div>
                    </section>
                )}

                {/* Playlist / Grid Section */}
                <div className="mt-8">
                    <div className="mb-8 flex items-center justify-between border-b border-neutral-800 pb-5">
                        <div className="flex items-center gap-8">
                            <h3 className="text-2xl font-bold">
                                Latest Discoveries
                            </h3>
                            <nav className="hidden items-center gap-6 md:flex">
                                <button className="relative font-bold text-indigo-400 after:absolute after:bottom-[-21px] after:left-0 after:h-1 after:w-full after:rounded-full after:bg-indigo-400">
                                    All Games
                                </button>
                                <button className="font-medium text-neutral-500 transition-colors hover:text-neutral-300">
                                    Trending
                                </button>
                                <button className="font-medium text-neutral-500 transition-colors hover:text-neutral-300">
                                    Top Sellers
                                </button>
                                <button className="font-medium text-neutral-500 transition-colors hover:text-neutral-300">
                                    New Releases
                                </button>
                            </nav>
                        </div>

                        <div className="flex items-center gap-4 rounded-xl border border-neutral-800 bg-neutral-900/50 p-1.5 shadow-inner">
                            <button
                                onClick={() => setViewMode('grid')}
                                className={`rounded-lg p-2 transition-all ${viewMode === 'grid' ? 'bg-neutral-800 text-white shadow-lg' : 'text-neutral-500 hover:text-neutral-300'}`}
                            >
                                <Grid2X2 className="size-5" />
                            </button>
                            <button
                                onClick={() => setViewMode('list')}
                                className={`rounded-lg p-2 transition-all ${viewMode === 'list' ? 'bg-neutral-800 text-white shadow-lg' : 'text-neutral-500 hover:text-neutral-300'}`}
                            >
                                <List className="size-5" />
                            </button>
                            <div className="mx-1 h-6 w-px bg-neutral-800" />
                            <button className="p-2 text-neutral-500 transition-colors hover:text-neutral-300">
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
                                            className={
                                                viewMode === 'list'
                                                    ? 'h-32'
                                                    : 'aspect-[2/3]'
                                            }
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
                                                    className={`flex h-10 w-10 items-center justify-center rounded-lg font-bold transition-all ${link.active ? 'bg-indigo-600 text-white shadow-lg shadow-indigo-500/20' : 'border border-neutral-800 bg-neutral-900 text-neutral-500 hover:border-neutral-700'}`}
                                                    dangerouslySetInnerHTML={{
                                                        __html: link.label,
                                                    }}
                                                />
                                            ) : (
                                                <span
                                                    key={i}
                                                    className="flex h-10 w-10 items-center justify-center rounded-lg text-neutral-600"
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

            <div className="pointer-events-none fixed inset-0 bg-[radial-gradient(circle_at_top_right,rgba(79,70,229,0.05),transparent_50%),radial-gradient(circle_at_bottom_left,rgba(6,182,212,0.05),transparent_50%)]" />
        </AppLayout>
    );
}
