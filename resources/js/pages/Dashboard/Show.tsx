import Header from '@/components/Header';
import { login, register } from '@/routes';
import { type SharedData } from '@/types';
import { Head, Link, usePage } from '@inertiajs/react';
import { useEffect, useMemo, useRef, useState } from 'react';

const loadChart = () => import('react-apexcharts');

interface MediaItem {
    url: string;
    size_variants?: string[];
    width?: number;
    height?: number;
    external_id?: string;
    checksum?: string;
}

interface TrailerItem {
    url?: string;
    thumbnail?: string;
    name?: string;
    video_id?: string;
}

interface Game {
    id: number;
    name: string;
    canonical_name: string;
    rating: number;
    release_date: string;
    description: string;
    synopsis: string;
    developer: string;
    publisher: string;
    platforms: string[];
    genres: string[];
    media: {
        cover: MediaItem | null;
        screenshots: MediaItem[];
        artworks: MediaItem[];
        trailers: TrailerItem[];
        hero_url?: string;
        cover_url_high_res: string;
        cover_url_mobile: string;
        summary: {
            images: {
                has_cover: boolean;
                has_screenshots: boolean;
                has_artworks: boolean;
                total_count: number;
                hero_url?: string;
            };
            videos: {
                has_trailers: boolean;
                total_count: number;
            };
        };
    };
}

interface PriceData {
    currency: string;
    countries: Array<{
        country: string;
        min_price: number;
        max_price: number;
        avg_price: number;
    }>;
}

interface AvailabilityData {
    country: string;
    country_code: string;
    retailer_count: number;
    currency_count: number;
    availability_score: number;
}

interface Props {
    game: Game;
    priceData: PriceData[];
    availabilityData: AvailabilityData[];
    meta: {
        query_time: number;
        cached: {
            game: boolean;
            prices: boolean;
            availability: boolean;
        };
    };
}

export default function DashboardShow({
    game,
    priceData,
    availabilityData,
    meta,
}: Props) {
    const { auth } = usePage<SharedData>().props;
    const isMember = Boolean(auth.user);
    const [activeView, setActiveView] = useState(isMember ? 'chart' : 'cover'); // 'chart' or 'cover'
    const [isModalOpen, setIsModalOpen] = useState(false);
    const [currentMediaIndex, setCurrentMediaIndex] = useState(0);
    const [isChartLoading, setIsChartLoading] = useState(true);
    const [isVideoPlaying, setIsVideoPlaying] = useState(false);
    const heroRef = useRef<HTMLDivElement>(null);

    // Background image priority: Hero (Art/Promo) > Cover
    const backgroundImage =
        game.media.hero_url ||
        game.media.cover_url_high_res ||
        game.media.cover_url_mobile;

    // Get all media items for carousel
    const allMedia = [...game.media.screenshots, ...game.media.artworks];

    useEffect(() => {
        if (!isMember) {
            setIsChartLoading(false);
            return;
        }

        // Simulate chart loading time (1-3 seconds)
        const timer = setTimeout(
            () => {
                setIsChartLoading(false);
            },
            Math.random() * 2000 + 1000,
        );

        return () => clearTimeout(timer);
    }, [isMember]);

    // View Transition Setup
    useEffect(() => {
        const element = heroRef.current;
        // Feature detection
        if ('startViewTransition' in document && element) {
            element.style.viewTransitionName = `game-${game.id}`;
        }

        return () => {
            // Cleanup
            if (element?.style) {
                element.style.viewTransitionName = '';
            }
        };
    }, [game.id]);

    // Auto-play video functionality
    useEffect(() => {
        if (activeView === 'cover' && game.media.trailers.length > 0) {
            const timer = setTimeout(() => {
                setIsVideoPlaying(true);
            }, 1000); // Auto-play after 1 second

            return () => clearTimeout(timer);
        }
    }, [activeView, game.media.trailers]);

    const priceChartOptions = useMemo(
        () => ({
            chart: {
                type: 'bar' as const,
                background: 'transparent',
                toolbar: { show: false },
            },
            theme: {
                mode: 'dark' as const,
            },
            plotOptions: {
                bar: {
                    horizontal: true,
                    dataLabels: { position: 'top' as const },
                },
            },
            dataLabels: {
                enabled: true,
                formatter: (val: number) => val.toFixed(2),
                style: { colors: ['#fff'] },
            },
            xaxis: {
                categories: priceData.map((pd) => pd.currency),
                labels: { style: { colors: '#fff' } },
            },
            yaxis: {
                labels: { style: { colors: '#fff' } },
            },
            grid: {
                borderColor: '#374151',
            },
            colors: ['#3B82F6', '#EF4444', '#10B981'],
        }),
        [priceData],
    );

    const priceChartSeries = useMemo(
        () => [
            {
                name: 'Min Price',
                data: priceData.map((pd) => pd.countries[0]?.min_price || 0),
            },
            {
                name: 'Max Price',
                data: priceData.map((pd) => pd.countries[0]?.max_price || 0),
            },
            {
                name: 'Avg Price',
                data: priceData.map((pd) => pd.countries[0]?.avg_price || 0),
            },
        ],
        [priceData],
    );

    const availabilityChartOptions = useMemo(
        () => ({
            chart: {
                type: 'donut' as const,
                background: 'transparent',
            },
            theme: {
                mode: 'dark' as const,
            },
            labels: availabilityData.map((ad) => ad.country),
            colors: ['#10B981', '#F59E0B', '#EF4444', '#8B5CF6', '#06B6D4'],
            legend: {
                labels: { colors: '#fff' },
            },
            plotOptions: {
                pie: {
                    donut: {
                        size: '70%',
                        labels: {
                            show: true,
                            total: {
                                show: true,
                                label: 'Countries',
                                color: '#fff',
                            },
                        },
                    },
                },
            },
        }),
        [availabilityData],
    );

    const availabilityChartSeries = useMemo(
        () => availabilityData.map((ad) => ad.availability_score),
        [availabilityData],
    );

    const LazyChart = ({
        options,
        series,
        type,
        height,
    }: {
        options: unknown;
        series: unknown;
        type: 'bar' | 'donut';
        height: string | number;
    }) => {
        const [ApexChart, setApexChart] = useState<any>(null);

        useEffect(() => {
            if (!isMember || ApexChart) {
                return;
            }

            loadChart().then((module) => {
                setApexChart(() => module.default);
            });
        }, [ApexChart, isMember]);

        if (!ApexChart) {
            return (
                <div
                    className="flex h-64 items-center justify-center"
                    aria-live="polite"
                >
                    <div className="text-white">Loading chart...</div>
                </div>
            );
        }

        return (
            <ApexChart
                options={options}
                series={series}
                type={type}
                height={height}
            />
        );
    };

    const openModal = (index: number = 0) => {
        setCurrentMediaIndex(index);
        setIsModalOpen(true);
    };

    const nextMedia = () => {
        setCurrentMediaIndex((prev) => (prev + 1) % allMedia.length);
    };

    const prevMedia = () => {
        setCurrentMediaIndex(
            (prev) => (prev - 1 + allMedia.length) % allMedia.length,
        );
    };

    return (
        <>
            <Head title={`${game.name} - Dashboard`} />

            <div className="min-h-screen bg-black">
                <Header />

                <div
                    ref={heroRef}
                    className="game-hero-image relative min-h-[calc(100vh-4rem)] bg-cover bg-fixed bg-center"
                    style={{
                        backgroundImage: backgroundImage
                            ? `url(${backgroundImage})`
                            : 'none',
                        backgroundColor: '#000000',
                    }}
                >
                    {/* Background Overlay - Darken for text but NO BLUR */}
                    <div className="absolute inset-0 bg-black/40"></div>

                    {/* Mobile Background Optimization - Removed Blur */}
                    <div
                        className="absolute inset-0 bg-cover bg-center md:hidden"
                        style={{
                            backgroundImage: game.media.cover_url_mobile
                                ? `url(${game.media.cover_url_mobile})`
                                : 'none',
                        }}
                    />
                    <div className="absolute inset-0 bg-black/60 md:hidden" />

                    {/* Content */}
                    <div className="relative z-10">
                        {/* Sub-header */}
                        <div className="border-b border-white/10 bg-black/80 backdrop-blur-sm">
                            <div className="mx-auto max-w-7xl px-4 sm:px-6 lg:px-8">
                                <div className="flex h-16 items-center">
                                    <Link
                                        href="/dashboard"
                                        className="mr-4 text-white transition-colors hover:text-blue-300"
                                    >
                                        ← Back to Dashboard
                                    </Link>
                                    <h1 className="truncate text-xl font-bold text-white">
                                        {game.name}
                                    </h1>

                                    {/* Performance Stats */}
                                    <div className="ml-auto text-sm text-gray-400">
                                        Loaded in{' '}
                                        {(meta.query_time * 1000).toFixed(2)}ms
                                    </div>
                                </div>
                            </div>
                        </div>

                        <div className="mx-auto max-w-7xl px-4 py-8 sm:px-6 lg:px-8">
                            <div className="grid grid-cols-1 gap-8 lg:grid-cols-3">
                                {/* Main Display Panel - Left Column */}
                                <div className="space-y-6 lg:col-span-2">
                                    {/* View Toggle Buttons */}
                                    <div className="flex flex-wrap gap-4">
                                        {isMember ? (
                                            <button
                                                onClick={() =>
                                                    setActiveView('chart')
                                                }
                                                className={`rounded-xl px-6 py-3 font-medium transition-all ${
                                                    activeView === 'chart'
                                                        ? 'bg-blue-600 text-white shadow-lg'
                                                        : 'bg-white/10 text-gray-300 hover:bg-white/20'
                                                }`}
                                            >
                                                Charts
                                            </button>
                                        ) : null}
                                        <button
                                            onClick={() =>
                                                setActiveView('cover')
                                            }
                                            className={`rounded-xl px-6 py-3 font-medium transition-all ${
                                                activeView === 'cover'
                                                    ? 'bg-blue-600 text-white shadow-lg'
                                                    : 'bg-white/10 text-gray-300 hover:bg-white/20'
                                            }`}
                                        >
                                            Cover/Trailer
                                        </button>
                                        {!isMember ? (
                                            <div className="flex items-center gap-3 rounded-xl border border-white/20 bg-white/5 px-4 py-3 text-sm text-white/80">
                                                <span>
                                                    Charts are a member feature.
                                                </span>
                                                <Link
                                                    href={register()}
                                                    className="text-blue-200 hover:text-blue-100"
                                                >
                                                    Join now
                                                </Link>
                                                <Link
                                                    href={login()}
                                                    className="text-white/70 hover:text-white"
                                                >
                                                    Log in
                                                </Link>
                                            </div>
                                        ) : null}
                                    </div>

                                    {/* Main Display */}
                                    <div className="min-h-[400px] overflow-hidden rounded-xl border border-white/20 bg-black/60">
                                        {activeView === 'chart' && isMember ? (
                                            <div className="p-6">
                                                {/* Price Analysis Chart */}
                                                <div className="mb-8">
                                                    <h3 className="mb-4 text-xl font-bold text-white">
                                                        Price Analysis
                                                    </h3>
                                                    {isChartLoading ? (
                                                        <div className="flex h-64 items-center justify-center">
                                                            <div className="text-white">
                                                                Loading chart...
                                                            </div>
                                                        </div>
                                                    ) : (
                                                        <LazyChart
                                                            options={
                                                                priceChartOptions
                                                            }
                                                            series={
                                                                priceChartSeries
                                                            }
                                                            type="bar"
                                                            height="300"
                                                        />
                                                    )}
                                                </div>

                                                {/* Availability Chart */}
                                                <div>
                                                    <h3 className="mb-4 text-xl font-bold text-white">
                                                        Availability by Region
                                                    </h3>
                                                    {isChartLoading ? (
                                                        <div className="flex h-64 items-center justify-center">
                                                            <div className="text-white">
                                                                Loading chart...
                                                            </div>
                                                        </div>
                                                    ) : (
                                                        <LazyChart
                                                            options={
                                                                availabilityChartOptions
                                                            }
                                                            series={
                                                                availabilityChartSeries
                                                            }
                                                            type="donut"
                                                            height="300"
                                                        />
                                                    )}
                                                </div>
                                            </div>
                                        ) : (
                                            <div className="relative">
                                                {/* Auto-playing Cover/Trailer */}
                                                {game.media.trailers.length >
                                                    0 && isVideoPlaying ? (
                                                    <div className="relative aspect-video">
                                                        <video
                                                            autoPlay
                                                            muted
                                                            loop
                                                            className="h-full w-full object-cover"
                                                            poster={
                                                                backgroundImage
                                                            }
                                                        >
                                                            <source
                                                                src={
                                                                    game.media
                                                                        .trailers[0]
                                                                        ?.url
                                                                }
                                                                type="video/mp4"
                                                            />
                                                            Your browser does
                                                            not support the
                                                            video tag.
                                                        </video>
                                                        <button
                                                            onClick={() =>
                                                                setIsVideoPlaying(
                                                                    false,
                                                                )
                                                            }
                                                            className="absolute top-4 right-4 rounded-full bg-black/50 p-2 text-white transition-colors hover:bg-black/70"
                                                        >
                                                            ⏸️
                                                        </button>
                                                    </div>
                                                ) : (
                                                    <div className="relative aspect-video">
                                                        <img
                                                            src={
                                                                backgroundImage
                                                            }
                                                            alt={game.name}
                                                            className="h-full w-full object-cover"
                                                        />
                                                        {game.media.trailers
                                                            .length > 0 && (
                                                            <button
                                                                onClick={() =>
                                                                    setIsVideoPlaying(
                                                                        true,
                                                                    )
                                                                }
                                                                className="group absolute inset-0 flex items-center justify-center bg-black/30 transition-colors hover:bg-black/50"
                                                            >
                                                                <div className="rounded-full bg-white/90 p-4 text-black transition-transform group-hover:scale-110">
                                                                    ▶️
                                                                </div>
                                                            </button>
                                                        )}
                                                    </div>
                                                )}
                                            </div>
                                        )}
                                    </div>

                                    {/* Image Carousel */}
                                    {allMedia.length > 0 && (
                                        <div className="rounded-xl border border-white/20 bg-black/60 p-6">
                                            <h3 className="mb-4 text-xl font-bold text-white">
                                                Screenshots & Artwork
                                            </h3>
                                            <div className="grid grid-cols-3 gap-4 sm:grid-cols-4 md:grid-cols-6">
                                                {allMedia
                                                    .slice(0, 12)
                                                    .map((media, index) => (
                                                        <button
                                                            key={index}
                                                            onClick={() =>
                                                                openModal(index)
                                                            }
                                                            className="group relative aspect-video overflow-hidden rounded-lg transition-transform hover:scale-105"
                                                        >
                                                            <img
                                                                src={media.url}
                                                                alt={`Screenshot ${index + 1}`}
                                                                className="h-full w-full object-cover"
                                                                loading="lazy"
                                                            />
                                                            <div className="absolute inset-0 flex items-center justify-center bg-black/0 transition-colors group-hover:bg-black/30">
                                                                <span className="text-white opacity-0 transition-opacity group-hover:opacity-100">
                                                                    🔍
                                                                </span>
                                                            </div>
                                                        </button>
                                                    ))}
                                            </div>
                                        </div>
                                    )}
                                </div>

                                {/* Game Info - Right Column */}
                                <div className="space-y-6">
                                    {/* Game Details */}
                                    <div className="rounded-xl border border-white/20 bg-black/60 p-6">
                                        <h2 className="mb-4 text-2xl font-bold text-white">
                                            {game.name}
                                        </h2>

                                        {/* Rating */}
                                        {game.rating && (
                                            <div className="mb-4 flex items-center">
                                                <span className="mr-2 text-xl text-yellow-400">
                                                    ⭐
                                                </span>
                                                <span className="text-lg font-semibold text-white">
                                                    {game.rating.toFixed(1)}
                                                </span>
                                                <span className="ml-2 text-sm text-gray-400">
                                                    /10
                                                </span>
                                            </div>
                                        )}

                                        {/* Release Date */}
                                        {game.release_date && (
                                            <div className="mb-4">
                                                <span className="text-gray-400">
                                                    Release Date:{' '}
                                                </span>
                                                <span className="text-white">
                                                    {new Date(
                                                        game.release_date,
                                                    ).toLocaleDateString()}
                                                </span>
                                            </div>
                                        )}

                                        {/* Developer & Publisher */}
                                        {game.developer && (
                                            <div className="mb-2">
                                                <span className="text-gray-400">
                                                    Developer:{' '}
                                                </span>
                                                <span className="text-white">
                                                    {game.developer}
                                                </span>
                                            </div>
                                        )}
                                        {game.publisher && (
                                            <div className="mb-4">
                                                <span className="text-gray-400">
                                                    Publisher:{' '}
                                                </span>
                                                <span className="text-white">
                                                    {game.publisher}
                                                </span>
                                            </div>
                                        )}

                                        {/* Genres */}
                                        {game.genres &&
                                            game.genres.length > 0 && (
                                                <div className="mb-4">
                                                    <span className="mb-2 block text-gray-400">
                                                        Genres:
                                                    </span>
                                                    <div className="flex flex-wrap gap-2">
                                                        {game.genres.map(
                                                            (genre, index) => (
                                                                <span
                                                                    key={index}
                                                                    className="rounded-full bg-blue-600/30 px-3 py-1 text-sm text-blue-300"
                                                                >
                                                                    {genre}
                                                                </span>
                                                            ),
                                                        )}
                                                    </div>
                                                </div>
                                            )}

                                        {/* Platforms */}
                                        {game.platforms &&
                                            game.platforms.length > 0 && (
                                                <div className="mb-4">
                                                    <span className="mb-2 block text-gray-400">
                                                        Platforms:
                                                    </span>
                                                    <div className="flex flex-wrap gap-2">
                                                        {game.platforms.map(
                                                            (
                                                                platform,
                                                                index,
                                                            ) => (
                                                                <span
                                                                    key={index}
                                                                    className="rounded-full bg-gray-600/30 px-3 py-1 text-sm text-gray-300"
                                                                >
                                                                    {platform}
                                                                </span>
                                                            ),
                                                        )}
                                                    </div>
                                                </div>
                                            )}

                                        {/* Description */}
                                        {game.description && (
                                            <div>
                                                <h3 className="mb-2 text-lg font-semibold text-white">
                                                    Description
                                                </h3>
                                                <p className="leading-relaxed text-gray-300">
                                                    {game.description}
                                                </p>
                                            </div>
                                        )}
                                    </div>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>

                {/* Full Screen Modal for Image Carousel */}
                {isModalOpen && (
                    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/90">
                        <div className="relative max-h-screen max-w-7xl p-4">
                            {/* Close Button */}
                            <button
                                onClick={() => setIsModalOpen(false)}
                                className="absolute top-4 right-4 z-10 rounded-full bg-black/50 p-3 text-white transition-colors hover:bg-black/70"
                            >
                                ✕
                            </button>

                            {/* Navigation Buttons */}
                            {allMedia.length > 1 && (
                                <>
                                    <button
                                        onClick={prevMedia}
                                        className="absolute top-1/2 left-4 z-10 -translate-y-1/2 rounded-full bg-black/50 p-3 text-white transition-colors hover:bg-black/70"
                                    >
                                        ←
                                    </button>
                                    <button
                                        onClick={nextMedia}
                                        className="absolute top-1/2 right-4 z-10 -translate-y-1/2 rounded-full bg-black/50 p-3 text-white transition-colors hover:bg-black/70"
                                    >
                                        →
                                    </button>
                                </>
                            )}

                            {/* Current Image */}
                            {allMedia[currentMediaIndex] && (
                                <img
                                    src={allMedia[currentMediaIndex].url}
                                    alt={`Media ${currentMediaIndex + 1}`}
                                    className="max-h-full max-w-full rounded-lg object-contain"
                                />
                            )}

                            {/* Image Counter */}
                            <div className="absolute bottom-4 left-1/2 -translate-x-1/2 rounded-full bg-black/50 px-4 py-2 text-white">
                                {currentMediaIndex + 1} / {allMedia.length}
                            </div>
                        </div>

                        {/* Thumbnail Strip */}
                        <div className="absolute bottom-4 left-1/2 max-w-7xl -translate-x-1/2">
                            <div className="flex max-w-full gap-2 overflow-x-auto rounded-lg bg-black/50 px-4 py-2">
                                {allMedia.map((media, index) => (
                                    <button
                                        key={index}
                                        onClick={() =>
                                            setCurrentMediaIndex(index)
                                        }
                                        className={`h-12 w-16 flex-shrink-0 overflow-hidden rounded border-2 transition-all ${
                                            index === currentMediaIndex
                                                ? 'border-blue-400'
                                                : 'border-transparent hover:border-gray-400'
                                        }`}
                                    >
                                        <img
                                            src={media.url}
                                            alt={`Thumbnail ${index + 1}`}
                                            className="h-full w-full object-cover"
                                        />
                                    </button>
                                ))}
                            </div>
                        </div>
                    </div>
                )}
            </div>
        </>
    );
}
