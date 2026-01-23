import MediaPlayer from '@/components/MediaPlayer';
import PriceHistoryChart from '@/components/PriceHistoryChart';
import AppLayout from '@/layouts/app-layout';
import { GameModel, GameShowMedia, GameShowPrice } from '@/types';
import { Head } from '@inertiajs/react';
import { useEffect, useState } from 'react';

export default function Show({
    game,
    prices,
    media,
}: {
    game: GameModel;
    prices: GameShowPrice[];
    media: GameShowMedia;
}) {
    // Helper to format currency
    const formatPrice = (amount: number, currency: string) => {
        return new Intl.NumberFormat(undefined, {
            style: 'currency',
            currency: currency,
        }).format(amount);
    };

    // Collect all available background candidates (High-res preferred)
    // Priority: High-Res Cover (Database) -> Hero -> Background -> Screenshots
    const backgrounds = [
        media.cover, // New high-res cover from backend
        media.hero,
        media.background,
        ...(media.screenshots || []),
    ].filter((url): url is string => !!url && typeof url === 'string');

    // Deduplicate URLs
    const uniqueBackgrounds = Array.from(new Set(backgrounds));

    const [currentBgIndex, setCurrentBgIndex] = useState(0);

    // Cycle backgrounds every 8 seconds if we have more than one
    useEffect(() => {
        if (uniqueBackgrounds.length <= 1) return;

        const interval = setInterval(() => {
            setCurrentBgIndex((prev) => (prev + 1) % uniqueBackgrounds.length);
        }, 8000);

        return () => clearInterval(interval);
    }, [uniqueBackgrounds.length]);

    // View Transition Name (must match GameCard)
    const vtName = `game-cover-${game.id}`;

    // Theme artifacts
    const theme = game.theme || {
        primary: '#3b82f6', // blue-500 fallback
        accent: '#60a5fa',
        background: '#030712', // gray-950
        surface: '#111827',
    };

    const themeStyles = {
        '--game-primary': theme.primary,
        '--game-accent': theme.accent,
        '--game-background': theme.background,
        '--game-surface': theme.surface,
    } as React.CSSProperties;

    return (
        <AppLayout
            breadcrumbs={[
                { title: 'Library', href: '/games' },
                { title: game.name, href: `/games/${game.id}` },
            ]}
        >
            <Head title={game.name} />

            <div
                className="relative min-h-screen text-white selection:bg-[var(--game-primary)] selection:text-white"
                style={{
                    ...themeStyles,
                    backgroundColor: 'var(--game-background)',
                }}
            >
                {/* 1. Cinematic Hero Background Carousel */}
                <div className="fixed inset-0 z-0 h-[80vh] w-full overflow-hidden">
                    <div className="absolute inset-0 z-10 bg-gradient-to-b from-transparent via-[var(--game-background)]/20 to-[var(--game-background)]" />
                    <div className="absolute inset-0 z-10 bg-gradient-to-r from-[var(--game-background)] via-[var(--game-background)]/10 to-transparent" />

                    {uniqueBackgrounds.map((bg, index) => (
                        <div
                            key={bg}
                            className={`absolute inset-0 h-full w-full transition-opacity duration-[2000ms] ease-in-out ${
                                index === currentBgIndex
                                    ? 'opacity-60'
                                    : 'opacity-0'
                            }`}
                        >
                            <img
                                src={bg}
                                alt=""
                                className="h-full w-full object-cover"
                            />
                        </div>
                    ))}

                    {uniqueBackgrounds.length === 0 && (
                        <div className="absolute inset-0 bg-gray-900 opacity-60" />
                    )}
                </div>

                <div className="relative z-10 px-4 py-12 sm:px-6 lg:px-8">
                    <div className="mx-auto max-w-7xl">
                        <div className="grid grid-cols-1 gap-12 lg:grid-cols-12">
                            {/* Left Column: Poster & Key Info */}
                            <div className="lg:col-span-4 xl:col-span-3">
                                <div className="sticky top-24 space-y-8">
                                    {/* Poster / Box Art */}
                                    <div className="group relative aspect-[2/3] overflow-hidden rounded-xl bg-gray-800 shadow-2xl ring-1 ring-white/10 transition-transform duration-500 hover:scale-[1.02]">
                                        {media.poster || media.cover ? (
                                            <img
                                                src={
                                                    media.poster ||
                                                    media.cover ||
                                                    ''
                                                }
                                                alt={game.name}
                                                className="h-full w-full object-cover shadow-inner"
                                                style={{
                                                    viewTransitionName: vtName,
                                                }}
                                            />
                                        ) : (
                                            <div className="flex h-full w-full items-center justify-center text-gray-500">
                                                No Image
                                            </div>
                                        )}
                                    </div>

                                    {/* Quick Stats */}
                                    <div className="rounded-xl border border-white/10 bg-[var(--game-surface)]/40 p-6 ring-1 ring-[var(--game-primary)]/10 backdrop-blur-md">
                                        <h3 className="mb-4 text-xs font-bold tracking-widest text-[var(--game-accent)]/80 uppercase">
                                            Game Info
                                        </h3>

                                        <dl className="space-y-4 text-sm">
                                            <div>
                                                <dt className="text-gray-500">
                                                    Released
                                                </dt>
                                                <dd className="font-medium text-white">
                                                    {game.release_date || 'TBA'}
                                                </dd>
                                            </div>
                                            <div>
                                                <dt className="text-gray-500">
                                                    Developer
                                                </dt>
                                                <dd className="font-medium text-white">
                                                    {typeof game.developer ===
                                                    'string'
                                                        ? game.developer
                                                        : Array.isArray(
                                                                game.developer,
                                                            )
                                                          ? game.developer.join(
                                                                ', ',
                                                            )
                                                          : 'Unknown'}
                                                </dd>
                                            </div>
                                            <div>
                                                <dt className="text-gray-500">
                                                    Rating
                                                </dt>
                                                <dd className="flex items-center gap-2 font-medium text-white">
                                                    <span
                                                        className={`inline-flex h-2 w-2 rounded-full ${Number(game.rating) >= 80 ? 'bg-green-500' : Number(game.rating) >= 50 ? 'bg-yellow-500' : 'bg-red-500'}`}
                                                    />
                                                    {game.rating
                                                        ? `${game.rating}%`
                                                        : 'N/A'}
                                                </dd>
                                            </div>
                                        </dl>
                                    </div>
                                </div>
                            </div>

                            {/* Right Column: Content */}
                            <div className="lg:col-span-8 xl:col-span-9">
                                {/* Header Section */}
                                <div className="mb-10">
                                    {media.logo ? (
                                        <img
                                            src={media.logo}
                                            alt={game.name}
                                            className="mb-6 max-h-32 w-auto object-contain lg:max-h-40"
                                        />
                                    ) : (
                                        <h1 className="mb-4 text-4xl font-black tracking-tight text-white lg:text-6xl">
                                            {game.name}
                                        </h1>
                                    )}

                                    <p className="max-w-3xl text-lg leading-relaxed text-gray-300">
                                        {typeof game.summary === 'string'
                                            ? game.summary
                                            : 'No summary available.'}
                                    </p>
                                </div>

                                {/* Price Matrix */}
                                <div className="mb-12">
                                    <div className="mb-6 flex items-center justify-between">
                                        <h2 className="text-2xl font-bold text-white">
                                            Global Prices
                                        </h2>
                                        <span className="rounded-full border border-[var(--game-primary)]/20 bg-[var(--game-primary)]/10 px-3 py-1 text-xs font-medium text-[var(--game-accent)]">
                                            {prices.length} Regions Tracked
                                        </span>
                                    </div>

                                    <div className="overflow-hidden rounded-xl border border-white/5 bg-[var(--game-surface)]/60 shadow-xl backdrop-blur-sm">
                                        <div className="grid grid-cols-1 divide-y divide-white/5 sm:grid-cols-2 lg:grid-cols-3 lg:divide-x lg:divide-y-0">
                                            {prices.slice(0, 6).map((price) => (
                                                <a
                                                    key={price.id}
                                                    href={price.url || '#'}
                                                    target="_blank"
                                                    rel="noopener noreferrer"
                                                    className="group flex items-center justify-between p-4 transition hover:bg-white/5"
                                                >
                                                    <div className="flex items-center gap-3">
                                                        <div className="flex h-8 w-8 items-center justify-center rounded bg-gray-800 text-xs font-bold text-gray-400">
                                                            {price.country_code}
                                                        </div>
                                                        <div>
                                                            <div className="text-sm font-medium text-gray-200 group-hover:text-white">
                                                                {price.retailer}
                                                            </div>
                                                            <div className="text-xs text-gray-500">
                                                                {price.currency}
                                                            </div>
                                                        </div>
                                                    </div>
                                                    <div className="text-right">
                                                        <div className="font-mono font-bold text-green-400">
                                                            {formatPrice(
                                                                price.amount,
                                                                price.currency,
                                                            )}
                                                        </div>
                                                        {price.discount_percent >
                                                            0 && (
                                                            <div className="text-xs text-red-400">
                                                                -
                                                                {
                                                                    price.discount_percent
                                                                }
                                                                %
                                                            </div>
                                                        )}
                                                    </div>
                                                </a>
                                            ))}
                                        </div>
                                        {prices.length > 6 && (
                                            <div className="border-t border-white/5 bg-white/5 p-3 text-center">
                                                <button className="text-sm font-medium text-[var(--game-accent)] transition-colors hover:text-[var(--game-primary)]">
                                                    View all {prices.length}{' '}
                                                    prices
                                                </button>
                                            </div>
                                        )}
                                    </div>
                                </div>

                                {/* Media Gallery */}
                                <div className="mb-12">
                                    <h2 className="mb-6 text-2xl font-bold text-white">
                                        Gallery & Media
                                    </h2>

                                    {/* Unified Media Player */}
                                    {media.trailers &&
                                        media.trailers.length > 0 && (
                                            <div className="mb-6">
                                                <MediaPlayer
                                                    url={media.trailers[0]}
                                                    thumbnail={
                                                        media.background ||
                                                        media.cover ||
                                                        undefined
                                                    }
                                                    title={`${game.name} Trailer`}
                                                    className="aspect-video w-full"
                                                />
                                            </div>
                                        )}

                                    {/* Screenshots Grid */}
                                    <div className="grid grid-cols-2 gap-4 md:grid-cols-3">
                                        {media.screenshots?.map(
                                            (url, index) => (
                                                <div
                                                    key={index}
                                                    className="group relative aspect-video overflow-hidden rounded-lg border border-white/10 bg-gray-800 transition hover:border-white/30"
                                                >
                                                    <img
                                                        src={url}
                                                        alt={`Screenshot ${index + 1}`}
                                                        className="h-full w-full object-cover transition duration-500 group-hover:scale-110"
                                                        loading="lazy"
                                                    />
                                                </div>
                                            ),
                                        )}
                                    </div>
                                </div>

                                {/* Price History */}
                                <div>
                                    <h3 className="mb-4 text-xl font-bold text-white">
                                        Price History
                                    </h3>
                                    <div className="rounded-xl border border-white/10 bg-[var(--game-background)]/60 p-6 shadow-2xl backdrop-blur-md">
                                        <PriceHistoryChart
                                            gameId={game.id}
                                            initialCurrency={
                                                prices[0]?.currency || 'USD'
                                            }
                                        />
                                    </div>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        </AppLayout>
    );
}
