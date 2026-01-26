import MediaPlayer from '@/components/MediaPlayer';
import PriceDisparityChart from '@/components/PriceDisparityChart';
import AppLayout from '@/layouts/app-layout';
import {
    GameModel,
    GameRatings,
    GameShowMedia,
    GameShowPrice,
    GameStatistics,
} from '@/types';
import { Head } from '@inertiajs/react';
import {
    ChevronDown,
    ChevronUp,
    DollarSign,
    Eye,
    Film,
    Flame,
    Globe,
    Heart,
    Image as ImageIcon,
    Star,
    Users,
    X,
    Zap,
} from 'lucide-react';
import { useEffect, useState } from 'react';

export default function Show({
    game,
    prices,
    media,
    statistics,
    ratings,
}: {
    game: GameModel;
    prices: GameShowPrice[];
    media: GameShowMedia;
    statistics: GameStatistics;
    ratings: GameRatings;
}) {
    const [showAllPrices, setShowAllPrices] = useState(false);
    const [selectedImageIndex, setSelectedImageIndex] = useState<number | null>(
        null,
    );
    const [currentBgIndex, setCurrentBgIndex] = useState(0);
    const [isLoaded, setIsLoaded] = useState(false);

    // Helper to format currency
    const formatPrice = (amount: number, currency: string) => {
        return new Intl.NumberFormat(undefined, {
            style: 'currency',
            currency: currency,
        }).format(amount);
    };

    // Collect all available background candidates (High-res preferred)
    const backgrounds = [
        media.cover,
        media.hero,
        media.background,
        ...(media.screenshots || []),
        ...(media.artworks || []),
    ].filter((url): url is string => !!url && typeof url === 'string');

    const uniqueBackgrounds = Array.from(new Set(backgrounds));

    // Cycle backgrounds every 8 seconds if we have more than one
    useEffect(() => {
        if (uniqueBackgrounds.length <= 1) return;

        const interval = setInterval(() => {
            setCurrentBgIndex((prev) => (prev + 1) % uniqueBackgrounds.length);
        }, 8000);

        return () => clearInterval(interval);
    }, [uniqueBackgrounds.length]);

    // Trigger entrance animations
    useEffect(() => {
        const timer = setTimeout(() => setIsLoaded(true), 50);
        return () => clearTimeout(timer);
    }, []);

    // Handle keyboard navigation for lightbox
    useEffect(() => {
        if (selectedImageIndex === null) return;

        const handleKeyDown = (e: KeyboardEvent) => {
            if (e.key === 'Escape') {
                setSelectedImageIndex(null);
            } else if (e.key === 'ArrowLeft' && selectedImageIndex > 0) {
                setSelectedImageIndex(selectedImageIndex - 1);
            } else if (
                e.key === 'ArrowRight' &&
                selectedImageIndex < allMediaItems.length - 1
            ) {
                setSelectedImageIndex(selectedImageIndex + 1);
            }
        };

        window.addEventListener('keydown', handleKeyDown);
        return () => window.removeEventListener('keydown', handleKeyDown);
    }, [selectedImageIndex]);

    const vtName = `game-cover-${game.id}`;

    const theme = game.theme || {
        primary: '#3b82f6',
        accent: '#60a5fa',
        background: '#030712',
        surface: '#111827',
    };

    const themeStyles = {
        '--game-primary': theme.primary,
        '--game-accent': theme.accent,
        '--game-background': theme.background,
        '--game-surface': theme.surface,
    } as React.CSSProperties;

    // Determine how many prices to show initially
    const visiblePrices = showAllPrices ? prices : prices.slice(0, 6);

    // All media items for lightbox
    const allMediaItems = [
        ...(media.screenshots || []),
        ...(media.artworks || []),
    ];

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
                {/* Cinematic Hero Background Carousel */}
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

                <div
                    className={`relative z-10 px-4 py-12 transition-all duration-1000 sm:px-6 lg:px-8 ${
                        isLoaded
                            ? 'translate-y-0 opacity-100'
                            : 'translate-y-8 opacity-0'
                    }`}
                >
                    <div className="mx-auto max-w-7xl">
                        <div className="grid grid-cols-1 gap-12 lg:grid-cols-12">
                            {/* Left Column: Poster & Key Info */}
                            <div
                                className={`transition-all delay-150 duration-700 lg:col-span-4 xl:col-span-3 ${
                                    isLoaded
                                        ? 'translate-x-0 opacity-100'
                                        : '-translate-x-8 opacity-0'
                                }`}
                            >
                                <div className="sticky top-24 space-y-8">
                                    {/* Poster / Box Art */}
                                    <div className="group relative aspect-[2/3] overflow-hidden rounded-xl bg-gray-800 shadow-2xl ring-1 ring-white/10 transition-all duration-500 hover:scale-[1.02] hover:shadow-[0_20px_60px_rgba(59,130,246,0.3)]">
                                        {media.poster || media.cover ? (
                                            <>
                                                <img
                                                    src={
                                                        media.poster ||
                                                        media.cover ||
                                                        ''
                                                    }
                                                    alt={game.name}
                                                    className="h-full w-full object-cover shadow-inner transition-transform duration-700"
                                                    style={{
                                                        viewTransitionName:
                                                            vtName,
                                                    }}
                                                />
                                                {/* Subtle glow on hover */}
                                                <div className="pointer-events-none absolute inset-0 bg-gradient-to-t from-[var(--game-primary)]/0 via-transparent to-transparent opacity-0 transition-opacity duration-500 group-hover:opacity-20" />
                                            </>
                                        ) : (
                                            <div className="flex h-full w-full items-center justify-center text-gray-500">
                                                No Image
                                            </div>
                                        )}
                                    </div>

                                    {/* Quick Stats */}
                                    <div className="animate-fade-in-up rounded-xl border border-white/10 bg-[var(--game-surface)]/40 p-6 ring-1 ring-[var(--game-primary)]/10 backdrop-blur-md">
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
                                                    Publisher
                                                </dt>
                                                <dd className="font-medium text-white">
                                                    {typeof game.publisher ===
                                                    'string'
                                                        ? game.publisher
                                                        : Array.isArray(
                                                                game.publisher,
                                                            )
                                                          ? game.publisher.join(
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
                                            {game.genres &&
                                                game.genres.length > 0 && (
                                                    <div>
                                                        <dt className="mb-2 text-gray-500">
                                                            Genres
                                                        </dt>
                                                        <dd className="flex flex-wrap gap-1">
                                                            {game.genres.map(
                                                                (
                                                                    genre,
                                                                    idx,
                                                                ) => (
                                                                    <span
                                                                        key={
                                                                            idx
                                                                        }
                                                                        className="rounded-full bg-[var(--game-primary)]/20 px-2 py-1 text-xs text-[var(--game-accent)] transition-transform hover:scale-105"
                                                                    >
                                                                        {genre}
                                                                    </span>
                                                                ),
                                                            )}
                                                        </dd>
                                                    </div>
                                                )}
                                            {game.platforms &&
                                                game.platforms.length > 0 && (
                                                    <div>
                                                        <dt className="mb-2 text-gray-500">
                                                            Platforms
                                                        </dt>
                                                        <dd className="flex flex-wrap gap-1">
                                                            {game.platforms.map(
                                                                (
                                                                    platform,
                                                                    idx,
                                                                ) => (
                                                                    <span
                                                                        key={
                                                                            idx
                                                                        }
                                                                        className="rounded-full border border-white/20 bg-white/5 px-2 py-1 text-xs text-gray-300 transition-transform hover:scale-105"
                                                                    >
                                                                        {
                                                                            platform
                                                                        }
                                                                    </span>
                                                                ),
                                                            )}
                                                        </dd>
                                                    </div>
                                                )}
                                        </dl>
                                    </div>

                                    {/* Ratings & Engagement */}
                                    <div className="animate-fade-in-up rounded-xl border border-white/10 bg-[var(--game-surface)]/40 p-6 ring-1 ring-[var(--game-primary)]/10 backdrop-blur-md [animation-delay:100ms]">
                                        <h3 className="mb-4 text-xs font-bold tracking-widest text-[var(--game-accent)]/80 uppercase">
                                            Community Metrics
                                        </h3>

                                        <div className="grid grid-cols-2 gap-4">
                                            <div className="flex items-center gap-2 transition-transform hover:scale-105">
                                                <Star className="h-4 w-4 text-yellow-500" />
                                                <div>
                                                    <div className="text-xs text-gray-500">
                                                        IGDB Score
                                                    </div>
                                                    <div className="font-bold text-white">
                                                        {ratings.igdb}%
                                                    </div>
                                                </div>
                                            </div>
                                            <div className="flex items-center gap-2 transition-transform hover:scale-105">
                                                <Users className="h-4 w-4 text-blue-500" />
                                                <div>
                                                    <div className="text-xs text-gray-500">
                                                        Ratings
                                                    </div>
                                                    <div className="font-bold text-white">
                                                        {ratings.rating_count.toLocaleString()}
                                                    </div>
                                                </div>
                                            </div>
                                            <div className="flex items-center gap-2 transition-transform hover:scale-105">
                                                <Flame className="h-4 w-4 text-orange-500" />
                                                <div>
                                                    <div className="text-xs text-gray-500">
                                                        Hypes
                                                    </div>
                                                    <div className="font-bold text-white">
                                                        {ratings.hypes.toLocaleString()}
                                                    </div>
                                                </div>
                                            </div>
                                            <div className="flex items-center gap-2 transition-transform hover:scale-105">
                                                <Heart className="h-4 w-4 text-red-500" />
                                                <div>
                                                    <div className="text-xs text-gray-500">
                                                        Follows
                                                    </div>
                                                    <div className="font-bold text-white">
                                                        {ratings.follows.toLocaleString()}
                                                    </div>
                                                </div>
                                            </div>
                                        </div>
                                    </div>

                                    {/* Statistics */}
                                    <div className="animate-fade-in-up rounded-xl border border-white/10 bg-[var(--game-surface)]/40 p-6 ring-1 ring-[var(--game-primary)]/10 backdrop-blur-md [animation-delay:200ms]">
                                        <h3 className="mb-4 text-xs font-bold tracking-widest text-[var(--game-accent)]/80 uppercase">
                                            Data Coverage
                                        </h3>

                                        <div className="space-y-3 text-sm">
                                            <div className="flex items-center justify-between transition-transform hover:translate-x-1">
                                                <div className="flex items-center gap-2 text-gray-400">
                                                    <Globe className="h-4 w-4" />
                                                    <span>Countries</span>
                                                </div>
                                                <span className="font-bold text-white">
                                                    {
                                                        statistics.unique_countries
                                                    }
                                                </span>
                                            </div>
                                            <div className="flex items-center justify-between transition-transform hover:translate-x-1">
                                                <div className="flex items-center gap-2 text-gray-400">
                                                    <DollarSign className="h-4 w-4" />
                                                    <span>Price Points</span>
                                                </div>
                                                <span className="font-bold text-white">
                                                    {statistics.total_prices}
                                                </span>
                                            </div>
                                            <div className="flex items-center justify-between transition-transform hover:translate-x-1">
                                                <div className="flex items-center gap-2 text-gray-400">
                                                    <ImageIcon className="h-4 w-4" />
                                                    <span>Screenshots</span>
                                                </div>
                                                <span className="font-bold text-white">
                                                    {
                                                        statistics.total_screenshots
                                                    }
                                                </span>
                                            </div>
                                            <div className="flex items-center justify-between transition-transform hover:translate-x-1">
                                                <div className="flex items-center gap-2 text-gray-400">
                                                    <Film className="h-4 w-4" />
                                                    <span>Videos</span>
                                                </div>
                                                <span className="font-bold text-white">
                                                    {statistics.total_videos}
                                                </span>
                                            </div>
                                            <div className="flex items-center justify-between transition-transform hover:translate-x-1">
                                                <div className="flex items-center gap-2 text-gray-400">
                                                    <Zap className="h-4 w-4" />
                                                    <span>Artworks</span>
                                                </div>
                                                <span className="font-bold text-white">
                                                    {statistics.total_artworks}
                                                </span>
                                            </div>
                                        </div>
                                    </div>
                                </div>
                            </div>

                            {/* Right Column: Content */}
                            <div
                                className={`transition-all delay-300 duration-700 lg:col-span-8 xl:col-span-9 ${
                                    isLoaded
                                        ? 'translate-x-0 opacity-100'
                                        : 'translate-x-8 opacity-0'
                                }`}
                            >
                                {/* Header Section */}
                                <div className="mb-10">
                                    {media.logo ? (
                                        <img
                                            src={media.logo}
                                            alt={game.name}
                                            className="mb-6 max-h-32 w-auto object-contain transition-transform duration-500 hover:scale-105 lg:max-h-40"
                                        />
                                    ) : (
                                        <h1 className="mb-4 text-4xl font-black tracking-tight text-white lg:text-6xl">
                                            {game.name}
                                        </h1>
                                    )}

                                    {game.summary && (
                                        <div className="mb-4">
                                            <h2 className="mb-2 text-sm font-semibold tracking-wide text-[var(--game-accent)] uppercase">
                                                Summary
                                            </h2>
                                            <p className="max-w-3xl text-lg leading-relaxed text-gray-300">
                                                {typeof game.summary ===
                                                'string'
                                                    ? game.summary
                                                    : 'No summary available.'}
                                            </p>
                                        </div>
                                    )}

                                    {game.storyline && (
                                        <div className="mt-6">
                                            <h2 className="mb-2 text-sm font-semibold tracking-wide text-[var(--game-accent)] uppercase">
                                                Storyline
                                            </h2>
                                            <p className="max-w-3xl text-base leading-relaxed text-gray-400">
                                                {game.storyline}
                                            </p>
                                        </div>
                                    )}
                                </div>

                                {/* Price Matrix */}
                                <div className="mb-12">
                                    <div className="mb-6 flex items-center justify-between">
                                        <h2 className="text-2xl font-bold text-white">
                                            Global Prices
                                        </h2>
                                        <div className="flex items-center gap-3">
                                            <span className="rounded-full border border-[var(--game-primary)]/20 bg-[var(--game-primary)]/10 px-3 py-1 text-xs font-medium text-[var(--game-accent)]">
                                                {statistics.unique_countries}{' '}
                                                Countries
                                            </span>
                                            <span className="rounded-full border border-[var(--game-primary)]/20 bg-[var(--game-primary)]/10 px-3 py-1 text-xs font-medium text-[var(--game-accent)]">
                                                {statistics.total_prices} Prices
                                            </span>
                                        </div>
                                    </div>

                                    <div className="overflow-hidden rounded-xl border border-white/5 bg-[var(--game-surface)]/60 shadow-xl backdrop-blur-sm">
                                        <div
                                            className={`grid grid-cols-1 divide-y divide-white/5 transition-all duration-500 sm:grid-cols-2 lg:grid-cols-3 lg:divide-x lg:divide-y-0 ${
                                                showAllPrices
                                                    ? 'max-h-[2000px]'
                                                    : 'max-h-[600px]'
                                            }`}
                                        >
                                            {visiblePrices.map(
                                                (price, index) => (
                                                    <a
                                                        key={price.id}
                                                        href={price.url || '#'}
                                                        target="_blank"
                                                        rel="noopener noreferrer"
                                                        className="group flex items-center justify-between p-4 transition-all hover:bg-white/5"
                                                        style={{
                                                            animation:
                                                                showAllPrices
                                                                    ? `fadeInUp 0.3s ease-out ${index * 0.03}s both`
                                                                    : 'none',
                                                        }}
                                                    >
                                                        <div className="flex items-center gap-3">
                                                            <div className="flex h-8 w-8 items-center justify-center rounded bg-gray-800 text-xs font-bold text-gray-400 transition-transform group-hover:scale-110">
                                                                {
                                                                    price.country_code
                                                                }
                                                            </div>
                                                            <div>
                                                                <div className="text-sm font-medium text-gray-200 group-hover:text-white">
                                                                    {
                                                                        price.retailer
                                                                    }
                                                                </div>
                                                                <div className="text-xs text-gray-500">
                                                                    {
                                                                        price.currency
                                                                    }
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
                                                            {price.btc_amount && (
                                                                <div className="text-xs text-gray-500">
                                                                    {price.btc_amount.toFixed(
                                                                        6,
                                                                    )}{' '}
                                                                    BTC
                                                                </div>
                                                            )}
                                                        </div>
                                                    </a>
                                                ),
                                            )}
                                        </div>

                                        {prices.length > 6 && (
                                            <div className="border-t border-white/5 bg-white/5 p-3 text-center">
                                                <button
                                                    onClick={() =>
                                                        setShowAllPrices(
                                                            !showAllPrices,
                                                        )
                                                    }
                                                    className="inline-flex items-center gap-2 text-sm font-medium text-[var(--game-accent)] transition-all hover:scale-105 hover:text-[var(--game-primary)]"
                                                >
                                                    {showAllPrices ? (
                                                        <>
                                                            <ChevronUp className="h-4 w-4 transition-transform" />
                                                            Show less
                                                        </>
                                                    ) : (
                                                        <>
                                                            <ChevronDown className="h-4 w-4 transition-transform" />
                                                            View all{' '}
                                                            {prices.length}{' '}
                                                            prices
                                                        </>
                                                    )}
                                                </button>
                                            </div>
                                        )}
                                    </div>
                                </div>

                                {/* Price Disparity Chart */}
                                <div className="mb-12">
                                    <h2 className="mb-6 text-2xl font-bold text-white">
                                        Price Disparity Analysis
                                    </h2>
                                    <div className="rounded-xl border border-white/10 bg-[var(--game-background)]/60 p-6 shadow-2xl backdrop-blur-md">
                                        <PriceDisparityChart prices={prices} />
                                    </div>
                                </div>

                                {/* Videos Section */}
                                {media.trailers &&
                                    media.trailers.length > 0 && (
                                        <div className="mb-12">
                                            <div className="mb-6 flex items-center justify-between">
                                                <h2 className="text-2xl font-bold text-white">
                                                    Videos & Trailers
                                                </h2>
                                                <span className="rounded-full border border-[var(--game-primary)]/20 bg-[var(--game-primary)]/10 px-3 py-1 text-xs font-medium text-[var(--game-accent)]">
                                                    {media.trailers.length}{' '}
                                                    Videos
                                                </span>
                                            </div>

                                            <div className="grid grid-cols-1 gap-6 md:grid-cols-2">
                                                {media.trailers.map(
                                                    (trailer, index) => (
                                                        <div
                                                            key={index}
                                                            className="overflow-hidden rounded-xl border border-white/10 bg-gray-900/50 transition-all hover:border-white/30 hover:shadow-[var(--game-primary)]/20 hover:shadow-lg"
                                                            style={{
                                                                animation: `fadeInUp 0.5s ease-out ${index * 0.1}s both`,
                                                            }}
                                                        >
                                                            <MediaPlayer
                                                                url={
                                                                    trailer.url
                                                                }
                                                                thumbnail={
                                                                    media.background ||
                                                                    media.cover ||
                                                                    undefined
                                                                }
                                                                title={
                                                                    trailer.name
                                                                }
                                                                className="aspect-video w-full"
                                                            />
                                                            <div className="p-3">
                                                                <h3 className="text-sm font-medium text-white">
                                                                    {
                                                                        trailer.name
                                                                    }
                                                                </h3>
                                                            </div>
                                                        </div>
                                                    ),
                                                )}
                                            </div>
                                        </div>
                                    )}

                                {/* Screenshots Gallery */}
                                {allMediaItems.length > 0 && (
                                    <div className="mb-12">
                                        <div className="mb-6 flex items-center justify-between">
                                            <h2 className="text-2xl font-bold text-white">
                                                Screenshots & Artwork
                                            </h2>
                                            <span className="rounded-full border border-[var(--game-primary)]/20 bg-[var(--game-primary)]/10 px-3 py-1 text-xs font-medium text-[var(--game-accent)]">
                                                {allMediaItems.length} Images
                                            </span>
                                        </div>

                                        <div className="grid grid-cols-2 gap-4 md:grid-cols-3 lg:grid-cols-4">
                                            {allMediaItems.map((url, index) => (
                                                <button
                                                    key={index}
                                                    onClick={() =>
                                                        setSelectedImageIndex(
                                                            index,
                                                        )
                                                    }
                                                    className="group relative aspect-video overflow-hidden rounded-lg border border-white/10 bg-gray-800 transition-all hover:scale-[1.02] hover:border-white/30 hover:ring-2 hover:ring-[var(--game-primary)]/50"
                                                    style={{
                                                        animation: `fadeInScale 0.4s ease-out ${index * 0.05}s both`,
                                                    }}
                                                >
                                                    <img
                                                        src={url}
                                                        alt={`Screenshot ${index + 1}`}
                                                        className="h-full w-full object-cover transition-transform duration-500 group-hover:scale-110"
                                                        loading="lazy"
                                                    />
                                                    <div className="absolute inset-0 flex items-center justify-center bg-black/0 opacity-0 transition-all duration-300 group-hover:bg-black/50 group-hover:opacity-100">
                                                        <Eye className="h-8 w-8 text-white" />
                                                    </div>
                                                </button>
                                            ))}
                                        </div>
                                    </div>
                                )}
                            </div>
                        </div>
                    </div>
                </div>

                {/* Image Lightbox */}
                {selectedImageIndex !== null && (
                    <div
                        className="animate-fade-in fixed inset-0 z-50 flex items-center justify-center bg-black/95 p-4 backdrop-blur-sm"
                        onClick={() => setSelectedImageIndex(null)}
                    >
                        <button
                            className="absolute top-4 right-4 rounded-full bg-white/10 p-2 text-white transition-all hover:rotate-90 hover:bg-white/20"
                            onClick={() => setSelectedImageIndex(null)}
                        >
                            <X className="h-6 w-6" />
                        </button>

                        {selectedImageIndex > 0 && (
                            <button
                                className="absolute top-1/2 left-4 -translate-y-1/2 rounded-full bg-white/10 p-3 text-white transition-all hover:scale-110 hover:bg-white/20"
                                onClick={(e) => {
                                    e.stopPropagation();
                                    setSelectedImageIndex(
                                        selectedImageIndex - 1,
                                    );
                                }}
                            >
                                <svg
                                    className="h-8 w-8"
                                    fill="none"
                                    stroke="currentColor"
                                    viewBox="0 0 24 24"
                                >
                                    <path
                                        strokeLinecap="round"
                                        strokeLinejoin="round"
                                        strokeWidth={2}
                                        d="M15 19l-7-7 7-7"
                                    />
                                </svg>
                            </button>
                        )}

                        <img
                            src={allMediaItems[selectedImageIndex]}
                            alt={`Screenshot ${selectedImageIndex + 1}`}
                            className="animate-zoom-in max-h-[90vh] max-w-[90vw] rounded-lg object-contain shadow-2xl"
                            onClick={(e) => e.stopPropagation()}
                        />

                        {selectedImageIndex < allMediaItems.length - 1 && (
                            <button
                                className="absolute top-1/2 right-4 -translate-y-1/2 rounded-full bg-white/10 p-3 text-white transition-all hover:scale-110 hover:bg-white/20"
                                onClick={(e) => {
                                    e.stopPropagation();
                                    setSelectedImageIndex(
                                        selectedImageIndex + 1,
                                    );
                                }}
                            >
                                <svg
                                    className="h-8 w-8"
                                    fill="none"
                                    stroke="currentColor"
                                    viewBox="0 0 24 24"
                                >
                                    <path
                                        strokeLinecap="round"
                                        strokeLinejoin="round"
                                        strokeWidth={2}
                                        d="M9 5l7 7-7 7"
                                    />
                                </svg>
                            </button>
                        )}

                        <div className="absolute bottom-4 left-1/2 -translate-x-1/2 rounded-full bg-black/70 px-4 py-2 text-sm text-white backdrop-blur-sm">
                            {selectedImageIndex + 1} / {allMediaItems.length}
                        </div>
                    </div>
                )}
            </div>

            {/* Custom Keyframe Animations */}
            <style>{`
                @keyframes fadeInUp {
                    from {
                        opacity: 0;
                        transform: translateY(20px);
                    }
                    to {
                        opacity: 1;
                        transform: translateY(0);
                    }
                }

                @keyframes fadeInScale {
                    from {
                        opacity: 0;
                        transform: scale(0.9);
                    }
                    to {
                        opacity: 1;
                        transform: scale(1);
                    }
                }

                @keyframes fadeIn {
                    from {
                        opacity: 0;
                    }
                    to {
                        opacity: 1;
                    }
                }

                @keyframes zoomIn {
                    from {
                        opacity: 0;
                        transform: scale(0.8);
                    }
                    to {
                        opacity: 1;
                        transform: scale(1);
                    }
                }

                .animate-fade-in {
                    animation: fadeIn 0.3s ease-out;
                }

                .animate-fade-in-up {
                    animation: fadeInUp 0.5s ease-out;
                }

                .animate-zoom-in {
                    animation: zoomIn 0.3s cubic-bezier(0.34, 1.56, 0.64, 1);
                }
            `}</style>
        </AppLayout>
    );
}
