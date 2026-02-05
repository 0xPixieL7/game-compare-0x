import { useGameCard } from '@/components/game-detail-modal';
import { SpatialImage } from '@/components/ui/SpatialImage';
import { type GameMedia, type GameTheme } from '@/types';
import { useCallback, useEffect, useMemo, useRef, useState } from 'react';

interface SpotlightScore {
    total: number;
    grade: string;
    verdict: string;
    breakdown: Array<{
        label: string;
        score: number;
        summary: string;
        weight_percentage?: number;
    }>;
}

interface SpotlightGalleryItem {
    id: string | number;
    type: 'image' | 'video';
    url: string;
    thumbnail?: string | null;
    title?: string | null;
    source?: string | null;
    caption?: string;
    video_type?: string;
    duration?: number | null;
}

export interface SpotlightProduct {
    id: number;
    name: string;
    slug: string;
    image: string;
    platform_labels: string[];
    region_codes: string[];
    currencies: string[];
    retailer_names: string[];
    spotlight_score: SpotlightScore;
    spotlight_gallery: SpotlightGalleryItem[];
    trailer_url?: string | null;
    platform?: string; // Fallback
    release_date?: string; // Fallback
    background?: string;
    media?: GameMedia;
    theme?: GameTheme | null;
}

interface SpotlightCarouselProps {
    spotlight: SpotlightProduct[];
    hero?: SpotlightProduct;
}

export function SpotlightCarousel({
    spotlight = [],
    hero,
}: SpotlightCarouselProps) {
    const { openGameCard, phase } = useGameCard();
    const isRunning = phase !== 'idle';
    const [gameIndex, setGameIndex] = useState(0);
    const [mediaIndex, setMediaIndex] = useState(0);
    const [isPaused, setIsPaused] = useState(false);
    const [isMuted, setIsMuted] = useState(false);
    const [isImageLoading, setIsImageLoading] = useState(true);
    const iframeRef = useRef<HTMLIFrameElement>(null);
    const autoplayRef = useRef<NodeJS.Timeout | null>(null);
    const sidebarItemRefs = useRef<(HTMLDivElement | null)[]>([]);

    const effectiveList = useMemo(() => {
        if (!spotlight || spotlight.length === 0) return [];
        return hero
            ? [hero, ...spotlight.filter((s) => s.id !== hero.id)]
            : spotlight;
    }, [spotlight, hero]);

    const currentGame = useMemo(
        () => effectiveList[gameIndex] || ({} as SpotlightProduct),
        [effectiveList, gameIndex],
    );

    // Flatten and clean the gallery for the current game
    const currentGallery = useMemo(() => {
        if (!currentGame || !currentGame.id) return [];

        // Fallback for real API structure
        if (!currentGame.spotlight_gallery && currentGame.media) {
            const media = currentGame.media;
            const gallery: SpotlightGalleryItem[] = [];

            // Add Trailers first (priority)
            if (media.trailers && media.trailers.length > 0) {
                media.trailers.forEach((t) => {
                    if (t.video_id) {
                        gallery.push({
                            id: `trailer-${t.video_id}`,
                            type: 'video',
                            url: t.video_id,
                            title: t.name,
                            thumbnail: t.thumbnail,
                        });
                    }
                });
            }

            // Add Screenshots
            if (media.screenshots && media.screenshots.length > 0) {
                media.screenshots.forEach((s, idx) => {
                    gallery.push({
                        id: `screen-${idx}`,
                        type: 'image',
                        url: s.url
                            .replace('t_thumb', 't_720p')
                            .replace('t_cover_big', 't_720p'),
                    });
                });
            }

            // Add Cover if needed
            if (gallery.length === 0 && (media.cover_url || media.cover)) {
                gallery.push({
                    id: 'cover',
                    type: 'image',
                    url:
                        media.cover_url ||
                        media.cover?.url ||
                        '/placeholder.jpg',
                });
            }

            if (gallery.length > 0) return gallery;
        }

        const gallery = currentGame.spotlight_gallery || [];
        // Ensure we have at least one image (background or cover as fallback)
        if (gallery.length === 0) {
            const bg =
                currentGame.background ||
                currentGame.image ||
                currentGame.media?.cover_url ||
                currentGame.media?.cover?.url ||
                '/placeholder.jpg';
            return [
                {
                    id: 'fallback',
                    type: 'image' as const,
                    url: bg,
                },
            ];
        }
        return gallery;
    }, [currentGame]);

    const activeMedia = currentGallery[mediaIndex] || currentGallery[0];

    const backgroundImage = useMemo(() => {
        if (!currentGame || !currentGame.id) return '/placeholder.jpg';

        // 1. If active media is an image, use it
        if (activeMedia?.type === 'image') return activeMedia.url;

        // 2. Fallback: Find first image in gallery
        const firstImage = (currentGallery as SpotlightGalleryItem[]).find(
            (item) => item.type === 'image',
        );
        if (firstImage) {
            if (firstImage.url.includes('igdb.com')) {
                return firstImage.url.replace('t_thumb', 't_720p');
            }
            return firstImage.url;
        }

        // 3. Fallback: Props or Cover
        const bg =
            currentGame.background ||
            currentGame.image ||
            currentGame.media?.cover_url ||
            currentGame.media?.cover?.url;

        if (bg && bg.includes('igdb.com')) {
            return bg
                .replace('t_thumb', 't_720p')
                .replace('t_cover_big', 't_720p');
        }

        return bg || '/placeholder.jpg';
    }, [activeMedia, currentGallery, currentGame]);

    const getMediaDuration = useCallback((media: SpotlightGalleryItem) => {
        if (!media) return 10000;
        if (media.type === 'video') {
            const seconds =
                typeof media.duration === 'number' ? media.duration : null;
            if (!seconds || seconds <= 0) {
                return null;
            }

            return seconds * 1000 + 5000;
        }

        return 10000;
    }, []);

    const nextMedia = useCallback(() => {
        if (mediaIndex < currentGallery.length - 1) {
            setMediaIndex((prev) => prev + 1);
        } else {
            const nextGameIdx = (gameIndex + 1) % (effectiveList.length || 1);
            setGameIndex(nextGameIdx);
            setMediaIndex(0);
        }
    }, [mediaIndex, currentGallery.length, gameIndex, effectiveList.length]);

    // Auto-scroll sidebar to keep active game in view
    useEffect(() => {
        const activeElement = sidebarItemRefs.current[gameIndex];
        if (activeElement) {
            activeElement.scrollIntoView({
                behavior: 'smooth',
                block: 'nearest',
                inline: 'nearest',
            });
        }
    }, [gameIndex]);

    // Autoplay logic with smart timing
    useEffect(() => {
        if (!isPaused && activeMedia) {
            const duration = getMediaDuration(activeMedia);
            if (!duration) {
                return;
            }
            autoplayRef.current = setInterval(() => {
                nextMedia();
            }, duration);
        }
        return () => {
            if (autoplayRef.current) {
                clearInterval(autoplayRef.current);
                autoplayRef.current = null;
            }
        };
    }, [isPaused, activeMedia, getMediaDuration, nextMedia]);

    // YouTube Event Listener for Video End
    useEffect(() => {
        const handleMessage = (event: MessageEvent) => {
            if (event.origin !== 'https://www.youtube.com') return;

            try {
                const data = JSON.parse(event.data);
                if (
                    data.event === 'infoDelivery' &&
                    data.info &&
                    data.info.playerState === 0
                ) {
                    nextMedia();
                }
            } catch {
                // Ignore parse errors
            }
        };

        window.addEventListener('message', handleMessage);
        return () => window.removeEventListener('message', handleMessage);
    }, [nextMedia]);

    // Initial loading handled by key remounting and onLoad event

    if (!spotlight || spotlight.length === 0) {
        return (
            <div className="p-8 text-center text-gray-500">
                Spotlight warming up...
            </div>
        );
    }

    const goToGame = (index: number) => {
        setGameIndex(index);
        setMediaIndex(0);
        setIsImageLoading(true);
    };

    const toggleMute = (e: React.MouseEvent) => {
        e.preventDefault();
        e.stopPropagation();
        const newMute = !isMuted;
        setIsMuted(newMute);

        if (iframeRef.current?.contentWindow) {
            iframeRef.current.contentWindow.postMessage(
                JSON.stringify({
                    event: 'command',
                    func: newMute ? 'mute' : 'unMute',
                    args: [],
                }),
                '*',
            );
        }
    };

    const togglePlay = (e: React.MouseEvent) => {
        e.preventDefault();
        e.stopPropagation();
        const newPaused = !isPaused;
        setIsPaused(newPaused);

        if (iframeRef.current?.contentWindow) {
            iframeRef.current.contentWindow.postMessage(
                JSON.stringify({
                    event: 'command',
                    func: newPaused ? 'pauseVideo' : 'playVideo',
                    args: [],
                }),
                '*',
            );
        }
    };

    // Helper to build subtitle
    const buildSubtitle = (item: SpotlightProduct) => {
        const platforms =
            item.platform_labels && item.platform_labels.length
                ? item.platform_labels
                : item.platform
                  ? [item.platform]
                  : [];
        const release = item.release_date || '';
        const regions = item.region_codes ? item.region_codes.length : 0;

        const parts = [];
        if (platforms.length)
            parts.push(platforms.map((p) => p.toUpperCase()).join(' · '));
        if (release) parts.push(release);
        if (regions > 0)
            parts.push(`${regions} region${regions === 1 ? '' : 's'}`);

        return parts.join(' · ') || 'Spotlight metrics warming up';
    };

    return (
        <section className="relative isolate min-h-screen w-full overflow-hidden pt-32 pb-16">
            <div className="absolute inset-0 -z-10">
                <div className="absolute inset-0 bg-black" />
                <div className="absolute inset-0 bg-gradient-to-b from-black via-black/40 to-black" />
                <div className="absolute inset-0 bg-gradient-to-r from-black via-black/70 to-transparent" />
                <div className="relative h-full w-full overflow-hidden">
                    <div className="h-full w-full spotlight-apple-card">
                        {backgroundImage && (
                            <img
                                alt={currentGame.name || ''}
                                className={`h-full w-full object-cover object-center transition-opacity duration-1000 ${isImageLoading ? 'opacity-0' : 'opacity-60'}`}
                                loading="eager"
                                onLoad={() => setIsImageLoading(false)}
                                onError={() => setIsImageLoading(false)}
                                key={backgroundImage}
                                src={backgroundImage}
                            />
                        )}
                    </div>
                    <div
                        className="pointer-events-none absolute inset-0 z-20 grid h-full w-full"
                        style={{
                            gridTemplateColumns: 'repeat(12, 1fr)',
                            gridTemplateRows: 'repeat(6, 1fr)',
                        }}
                    >
                        {Array.from({ length: 72 }).map((_, i) => (
                            <div
                                key={i}
                                className="h-full w-full bg-black"
                                style={{
                                    transform: 'scale(0)',
                                    transformOrigin: '50% 50%',
                                }}
                            />
                        ))}
                    </div>
                </div>
                {/* Glow effects */}
                <div className="absolute top-10 -left-40 h-96 w-96 rounded-full bg-blue-500/20 blur-3xl" />
                <div className="absolute top-1/3 right-0 h-96 w-96 rounded-full bg-violet-500/20 blur-3xl" />
            </div>

            <div className="relative z-10 grid w-full gap-8 px-6 py-6 lg:grid-cols-[0.8fr_1.2fr] lg:items-stretch lg:gap-10 lg:px-12 lg:py-12">
                {/* Text Content */}
                <div className="flex flex-col justify-center space-y-8">
                    <div className="flex flex-wrap items-center gap-4">
                        <div className="flex items-center gap-2 rounded-full border border-white/10 bg-gradient-to-r from-blue-500/15 via-slate-900/50 to-violet-500/15 px-3 py-1 text-xs tracking-[0.28em] text-blue-200 uppercase">
                            <span className="text-white/70">Signal</span>
                            <span className="text-white">Puzzle</span>
                        </div>
                        <div className="flex items-center gap-2 rounded-full border border-white/10 bg-gradient-to-r from-cyan-500/15 via-slate-900/50 to-indigo-500/15 px-3 py-1 text-xs tracking-[0.28em] text-cyan-200 uppercase">
                            <span className="text-white/70">Radar</span>
                            <span className="text-white">Live</span>
                        </div>
                    </div>

                    <h1 className="text-4xl leading-tight font-black text-white sm:text-5xl lg:text-6xl">
                        {currentGame.name}
                        <span className="mt-3 block text-xl font-semibold text-blue-200/90 sm:text-2xl">
                            {buildSubtitle(currentGame)}
                        </span>
                    </h1>

                    <p className="max-w-xl text-base text-slate-200/80 sm:text-lg">
                        Track price momentum, platform volatility, and media
                        signals in a single immersive surface. Every row is
                        tuned for fast scanning and deeper discovery.
                    </p>

                    <div className="flex flex-wrap gap-4">
                        <button
                            className="inline-flex items-center justify-center gap-2 rounded-full bg-blue-500 px-6 py-3 text-sm font-semibold text-white transition hover:bg-blue-400 disabled:opacity-50"
                            onClick={() =>
                                openGameCard({
                                    id: currentGame.id,
                                    name: currentGame.name,
                                    coverUrl: currentGame.image,
                                    heroUrl:
                                        currentGame.background ||
                                        currentGame.media?.cover_url_high_res ||
                                        currentGame.media?.cover_url,
                                    rating: null,
                                    theme: currentGame.theme ?? undefined,
                                })
                            }
                            disabled={isRunning}
                        >
                            <svg
                                xmlns="http://www.w3.org/2000/svg"
                                width="24"
                                height="24"
                                viewBox="0 0 24 24"
                                fill="none"
                                stroke="currentColor"
                                strokeWidth="2"
                                strokeLinecap="round"
                                strokeLinejoin="round"
                                className="lucide lucide-compass h-4 w-4"
                            >
                                <path d="m16.24 7.76-1.804 5.411a2 2 0 0 1-1.265 1.265L7.76 16.24l1.804-5.411a2 2 0 0 1 1.265-1.265z" />
                                <circle cx="12" cy="12" r="10" />
                            </svg>
                            Start Analysis
                        </button>
                        <a
                            href="#rows"
                            className="inline-flex items-center justify-center gap-2 rounded-full border border-white/20 bg-white/5 px-6 py-3 text-sm font-semibold text-white transition hover:border-white/40"
                        >
                            <svg
                                xmlns="http://www.w3.org/2000/svg"
                                width="24"
                                height="24"
                                viewBox="0 0 24 24"
                                fill="none"
                                stroke="currentColor"
                                strokeWidth="2"
                                strokeLinecap="round"
                                strokeLinejoin="round"
                                className="lucide lucide-sparkles h-4 w-4"
                            >
                                <path d="M9.937 15.5A2 2 0 0 0 8.5 14.063l-6.135-1.582a.5.5 0 0 1 0-.962L8.5 9.936A2 2 0 0 0 9.937 8.5l1.582-6.135a.5.5 0 0 1 .963 0L14.063 8.5A2 2 0 0 0 15.5 9.937l6.135 1.581a.5.5 0 0 1 0 .964L15.5 14.063a2 2 0 0 0-1.437 1.437l-1.582 6.135a.5.5 0 0 1-.963 0z" />
                                <path d="M20 3v4" />
                                <path d="M22 5h-4" />
                                <path d="M4 17v2" />
                                <path d="M5 18H3" />
                            </svg>
                            Explore catalog
                        </a>
                    </div>

                    <div className="flex flex-wrap gap-4">
                        <div className="flex items-center gap-3 rounded-full border border-white/10 bg-white/5 px-4 py-2 text-sm text-slate-200/80 backdrop-blur">
                            <div className="flex flex-col leading-tight">
                                <span className="text-xs tracking-[0.2em] text-white/50 uppercase">
                                    Active signals
                                </span>
                                <span className="text-sm font-semibold text-white">
                                    250K+
                                </span>
                            </div>
                        </div>
                        <div className="flex items-center gap-3 rounded-full border border-emerald-400/20 bg-emerald-500/10 px-4 py-2 text-sm text-emerald-200 backdrop-blur">
                            <div className="flex flex-col leading-tight">
                                <span className="text-xs tracking-[0.2em] text-white/50 uppercase">
                                    Markets
                                </span>
                                <span className="text-sm font-semibold text-white">
                                    120+
                                </span>
                            </div>
                        </div>
                        <div className="flex items-center gap-3 rounded-full border border-rose-400/20 bg-rose-500/10 px-4 py-2 text-sm text-rose-200 backdrop-blur">
                            <div className="flex flex-col leading-tight">
                                <span className="text-xs tracking-[0.2em] text-white/50 uppercase">
                                    Platforms
                                </span>
                                <span className="text-sm font-semibold text-white">
                                    {currentGame.platform_labels?.length || 15}
                                </span>
                            </div>
                        </div>
                    </div>

                    <div className="hidden flex-wrap gap-3 lg:flex">
                        <div className="flex flex-col items-start gap-2 rounded-2xl border border-white/10 bg-black/60 px-4 py-3 text-xs tracking-[0.28em] text-white/70 uppercase">
                            <span>BTC Index</span>
                            <span className="text-lg font-semibold text-white">
                                Realtime
                            </span>
                        </div>
                        <div className="flex flex-col items-start gap-2 rounded-2xl border border-white/10 bg-black/60 px-4 py-3 text-xs tracking-[0.28em] text-white/70 uppercase">
                            <span>Latency</span>
                            <span className="text-lg font-semibold text-white">
                                Sub 2s
                            </span>
                        </div>
                    </div>

                    {/* Media Controls & Indicators */}
                    <div className="flex items-center gap-6 pt-4">
                        <div className="flex items-center gap-3">
                            <button
                                onClick={() => {
                                    const prev =
                                        (gameIndex - 1 + effectiveList.length) %
                                        effectiveList.length;
                                    goToGame(prev);
                                }}
                                className="group flex h-12 w-12 items-center justify-center rounded-full border border-white/20 bg-white/10 text-white backdrop-blur-md transition-all hover:scale-105 hover:border-white/40 hover:bg-white/20 active:scale-95"
                                aria-label="Previous game"
                            >
                                <svg
                                    xmlns="http://www.w3.org/2000/svg"
                                    width="24"
                                    height="24"
                                    viewBox="0 0 24 24"
                                    fill="none"
                                    stroke="currentColor"
                                    strokeWidth="2"
                                    strokeLinecap="round"
                                    strokeLinejoin="round"
                                >
                                    <path d="m15 18-6-6 6-6" />
                                </svg>
                            </button>
                            <button
                                onClick={() => {
                                    const next =
                                        (gameIndex + 1) % effectiveList.length;
                                    goToGame(next);
                                }}
                                className="group flex h-12 w-12 items-center justify-center rounded-full border border-white/20 bg-white/10 text-white backdrop-blur-md transition-all hover:scale-105 hover:border-white/40 hover:bg-white/20 active:scale-95"
                                aria-label="Next game"
                            >
                                <svg
                                    xmlns="http://www.w3.org/2000/svg"
                                    width="24"
                                    height="24"
                                    viewBox="0 0 24 24"
                                    fill="none"
                                    stroke="currentColor"
                                    strokeWidth="2"
                                    strokeLinecap="round"
                                    strokeLinejoin="round"
                                >
                                    <path d="m9 18 6-6-6-6" />
                                </svg>
                            </button>
                        </div>

                        <div className="flex items-center gap-3">
                            {currentGallery.map((_, i: number) => (
                                <button
                                    key={i}
                                    className={`h-1.5 rounded-full transition-all duration-500 ${i === mediaIndex ? 'w-8 bg-blue-500' : 'w-2 bg-white/20 hover:bg-white/40'}`}
                                    aria-label={`Go to slide ${i + 1}`}
                                    onClick={() => setMediaIndex(i)}
                                />
                            ))}
                        </div>
                    </div>
                </div>

                {/* Card/Media Side */}
                <div className="group relative flex items-center justify-center">
                    <div className="absolute top-12 -left-6 hidden h-40 w-40 rounded-full border border-white/10 bg-white/5 blur-2xl lg:block" />

                    <button
                        className="pointer-events-none absolute top-1/2 -left-12 z-20 -translate-y-1/2 rounded-full border border-white/10 bg-black/40 p-2 text-white opacity-0 transition-all group-hover:pointer-events-auto group-hover:left-4 group-hover:opacity-100 hover:bg-black/60"
                        aria-label="Previous media"
                        onClick={() => {
                            const prev =
                                (mediaIndex - 1 + currentGallery.length) %
                                currentGallery.length;
                            setMediaIndex(prev);
                        }}
                    >
                        <svg
                            xmlns="http://www.w3.org/2000/svg"
                            width="24"
                            height="24"
                            viewBox="0 0 24 24"
                            fill="none"
                            stroke="currentColor"
                            strokeWidth="2"
                            strokeLinecap="round"
                            strokeLinejoin="round"
                            className="lucide lucide-chevron-left h-6 w-6"
                        >
                            <path d="m15 18-6-6 6-6" />
                        </svg>
                    </button>
                    <button
                        className="pointer-events-none absolute top-1/2 -right-12 z-20 -translate-y-1/2 rounded-full border border-white/10 bg-black/40 p-2 text-white opacity-0 transition-all group-hover:pointer-events-auto group-hover:right-4 group-hover:opacity-100 hover:bg-black/60"
                        aria-label="Next media"
                        onClick={() => {
                            const next =
                                (mediaIndex + 1) % currentGallery.length;
                            setMediaIndex(next);
                        }}
                    >
                        <svg
                            xmlns="http://www.w3.org/2000/svg"
                            width="24"
                            height="24"
                            viewBox="0 0 24 24"
                            fill="none"
                            stroke="currentColor"
                            strokeWidth="2"
                            strokeLinecap="round"
                            strokeLinejoin="round"
                            className="lucide lucide-chevron-right h-6 w-6"
                        >
                            <path d="m9 18 6-6-6-6" />
                        </svg>
                    </button>

                    {/* New Spatial Image Card (Replaces Three.js implementation) */}
                    <div className="relative w-full max-w-[480px] mx-auto">
                        <SpatialImage aspectRatio="portrait"
                            src={activeMedia?.type === 'image' ? activeMedia.url : currentGame.image}
                            alt={currentGame.name}
                            videoUrl={activeMedia?.type === 'video' ? activeMedia.url : null}
                            className="h-full w-full spotlight-apple-card"
                        />
                        
                        {/* Overlay Badges */}
                        <div className="pointer-events-none absolute inset-0">
                            <div className="absolute top-6 left-6 inline-flex items-center gap-2 rounded-full border border-blue-500/30 bg-blue-500/10 px-4 py-1 text-[10px] font-bold tracking-[0.3em] text-blue-300 uppercase backdrop-blur">
                                <span className="h-1.5 w-1.5 rounded-full bg-blue-400" />
                                {activeMedia?.type === 'video' ? 'Live Trailer' : 'Featured Spotlight'}
                            </div>
                            <div className="absolute bottom-8 left-8">
                                <div className="max-w-[80%] text-3xl font-black tracking-tight text-white uppercase drop-shadow-lg">
                                    {currentGame.name}
                                </div>
                                <div className="mt-3 inline-flex items-center rounded-full border border-white/20 bg-white/10 px-4 py-2 text-xs font-bold text-white backdrop-blur">
                                    Explore Analysis
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
            </div>

            {/* Scroll Indicator */}
            <div className="mt-12 lg:absolute lg:bottom-12 lg:left-1/2 lg:mt-0 lg:-translate-x-1/2">
                <div className="pointer-events-none relative z-30 flex w-full justify-center py-8 select-none">
                    <style>
                        {`
                @keyframes neon-flicker {
                    0%, 19%, 21%, 23%, 25%, 54%, 56%, 100% { opacity: 0.99; }
                    20%, 24%, 55% { opacity: 0.4; }
                }
                .animate-neon-flicker { animation: neon-flicker 1.5s infinite; }
            `}
                    </style>
                    <div className="group relative">
                        <div className="relative rounded-xl border-2 border-white px-8 py-3 opacity-100 shadow-[0_0_15px_rgba(255,255,255,0.4),inset_0_0_10px_rgba(255,255,255,0.2)] transition-all duration-300 ease-out">
                            <div className="flex gap-1 font-mono text-2xl font-black tracking-[0.1em] whitespace-nowrap text-white md:text-3xl">
                                {['S', 'C', 'R', 'O', 'L', 'L'].map(
                                    (char, i) => (
                                        <span
                                            key={i}
                                            className="text-white opacity-100 drop-shadow-[0_0_8px_rgba(255,255,255,0.8)] transition-all duration-100"
                                        >
                                            {char}
                                        </span>
                                    ),
                                )}
                                <span className="text-white opacity-100 drop-shadow-[0_0_8px_rgba(255,255,255,0.8)] transition-all duration-100">
                                    &nbsp;
                                </span>
                                {['D', 'O', 'W', 'N'].map((char, i) => (
                                    <span
                                        key={i}
                                        className="text-white opacity-100 drop-shadow-[0_0_8px_rgba(255,255,255,0.8)] transition-all duration-100"
                                    >
                                        {char}
                                    </span>
                                ))}
                            </div>
                        </div>
                        <div className="absolute -bottom-6 left-1/2 h-3 w-3/4 -translate-x-1/2 rounded-[100%] bg-white/20 opacity-100 blur-xl transition-opacity duration-500" />
                    </div>
                </div>
            </div>
        </section>
    );
}