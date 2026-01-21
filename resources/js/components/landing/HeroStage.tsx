import { AppleTvCard } from '@/components/apple-tv-card';
import BoxReveal from '@/components/landing/BoxReveal';
import MetricBadge from '@/components/landing/MetricBadge';
import NeonCta from '@/components/landing/NeonCta';
import OrbitStat from '@/components/landing/OrbitStat';
import SignalPill from '@/components/landing/SignalPill';
import { Dialog, DialogContent, DialogTrigger } from '@/components/ui/dialog';
import { dashboard, register } from '@/routes';
import { type Game, type SharedData } from '@/types';
import { Link, usePage } from '@inertiajs/react';
import { motion } from 'framer-motion';
import {
    ChevronLeft,
    ChevronRight,
    Compass,
    Maximize2,
    Sparkles,
    Volume2,
    VolumeX,
} from 'lucide-react';
import { useCallback, useEffect, useMemo, useRef, useState } from 'react';

interface HeroStageProps {
    hero: Game | null;
    spotlightGames?: Game[];
}

export default function HeroStage({
    hero,
    spotlightGames = [],
}: HeroStageProps) {
    const { auth } = usePage<SharedData>().props;

    // Use the spotlight games provided by the controller
    const games = useMemo(() => {
        const list = Array.isArray(spotlightGames)
            ? spotlightGames
            : Object.values(spotlightGames || {});

        if (list.length > 0) return list;
        return hero ? [hero] : [];
    }, [hero, spotlightGames]);

    const [activeIndex, setActiveIndex] = useState(0);
    const activeGame = games[activeIndex];

    const heroTitle =
        activeGame?.canonical_name || activeGame?.name || 'Global game markets';
    const heroGenre = activeGame?.genres?.[0] ?? 'Market signal';
    const heroImage =
        activeGame?.media?.cover_url ?? activeGame?.media?.cover_url_thumb;

    // Force the backdrop to always be the high-res artwork/screenshot (preferred) or cover art
    // @ts-expect-error - backdrop_url is added dynamically in controller
    const heroBackdrop =
        activeGame?.backdrop_url ??
        activeGame?.media?.cover_url_high_res ??
        heroImage;

    // Trailer Logic
    const [activeVideoIndex, setActiveVideoIndex] = useState(0);
    const trailers = useMemo(
        () => activeGame?.media?.trailers || [],
        [activeGame],
    );
    const activeTrailer = trailers[activeVideoIndex];
    const videoId = activeTrailer?.video_id;

    const [isVideoOpen, setIsVideoOpen] = useState(false);
    const [isMuted, setIsMuted] = useState(false);

    // Reset video index when game changes
    useEffect(() => {
        setActiveVideoIndex(0);
    }, [activeGame?.id]);

    // Mute on scroll logic
    useEffect(() => {
        const handleScrollMute = () => {
            if (window.scrollY > 100 && !isMuted) {
                setIsMuted(true);
            } else if (window.scrollY <= 100 && isMuted) {
                setIsMuted(false);
            }
        };

        window.addEventListener('scroll', handleScrollMute, { passive: true });
        return () => window.removeEventListener('scroll', handleScrollMute);
    }, [isMuted]);

    // YouTube Autoplay/Sound Policy Helper
    // Browsers block unmuted autoplay unless there's an interaction.
    // We'll set mute=0 and add 'allow="autoplay"' to the iframe.
    // const getEmbedUrl = (id: string, muted: boolean) => {
    //    return `https://www.youtube.com/embed/${id}?autoplay=1&mute=${muted ? 1 : 0}&controls=0&loop=1&playlist=${id}&modestbranding=1&showinfo=0&rel=0&iv_load_policy=3&fs=0&disablekb=1&enablejsapi=1&origin=${window.location.origin}`;
    // };

    const nextSlide = useCallback(() => {
        setActiveIndex((current) => (current + 1) % games.length);
    }, [games.length]);

    const prevSlide = useCallback(() => {
        setActiveIndex(
            (current) => (current - 1 + games.length) % games.length,
        );
    }, [games.length]);

    const parallaxRef = useRef<HTMLImageElement>(null);
    const [enableParallax, setEnableParallax] = useState(true);

    useEffect(() => {
        if (typeof window === 'undefined') {
            return;
        }

        const mediaQuery = window.matchMedia(
            '(prefers-reduced-motion: reduce)',
        );
        setEnableParallax(!mediaQuery.matches);

        const handler = () => setEnableParallax(!mediaQuery.matches);
        mediaQuery.addEventListener('change', handler);

        return () => mediaQuery.removeEventListener('change', handler);
    }, []);

    useEffect(() => {
        if (!enableParallax || !parallaxRef.current) {
            return;
        }

        const handleScroll = () => {
            if (!parallaxRef.current) {
                return;
            }

            const offset = window.scrollY * 0.2;
            parallaxRef.current.style.transform = `translateY(${offset}px)`;
        };

        handleScroll();
        window.addEventListener('scroll', handleScroll, { passive: true });

        return () => window.removeEventListener('scroll', handleScroll);
    }, [enableParallax]);

    const containerVariants = {
        hidden: { opacity: 0 },
        visible: {
            opacity: 1,
            transition: {
                staggerChildren: 0.1,
                delayChildren: 0.3,
            },
        },
    };

    const itemVariants = {
        hidden: { opacity: 0, y: 20 },
        visible: {
            opacity: 1,
            y: 0,
            transition: { type: 'spring' as const, stiffness: 50 },
        },
    };

    return (
        <section className="relative isolate min-h-screen overflow-hidden px-6 pt-32 pb-16 sm:px-10 lg:px-16">
            <div className="absolute inset-0">
                <div className="absolute inset-0 bg-black" />
                <div className="absolute inset-0 bg-gradient-to-b from-black via-black/40 to-black" />
                <div className="absolute inset-0 bg-gradient-to-r from-black via-black/70 to-transparent" />
                {heroBackdrop ? (
                    <BoxReveal rows={6} cols={12} className="h-full w-full">
                        <img
                            ref={parallaxRef}
                            src={heroBackdrop}
                            alt=""
                            className="landing-parallax h-full w-full object-cover opacity-60"
                            loading="eager"
                        />
                    </BoxReveal>
                ) : null}
                <div className="absolute top-10 -left-40 h-96 w-96 rounded-full bg-blue-500/20 blur-3xl" />
                <div className="absolute top-1/3 right-0 h-96 w-96 rounded-full bg-violet-500/20 blur-3xl" />
            </div>

            <div className="relative z-10 grid items-center gap-16 lg:grid-cols-[0.9fr_1.1fr]">
                <motion.div
                    key={activeGame?.id ?? 'empty'}
                    className="space-y-8"
                    variants={containerVariants}
                    initial="hidden"
                    animate="visible"
                >
                    <motion.div
                        variants={itemVariants}
                        className="flex flex-wrap items-center gap-4"
                    >
                        <SignalPill label="Signal" value={heroGenre} />
                        <SignalPill label="Radar" value="Live" tone="cool" />
                    </motion.div>

                    <motion.h1
                        variants={itemVariants}
                        className="text-4xl leading-tight font-black text-white sm:text-5xl lg:text-6xl"
                    >
                        {heroTitle}
                        <span className="mt-3 block text-xl font-semibold text-blue-200/90 sm:text-2xl">
                            Cinematic pricing intelligence, remixed in BTC.
                        </span>
                    </motion.h1>

                    <motion.p
                        variants={itemVariants}
                        className="max-w-xl text-base text-slate-200/80 sm:text-lg"
                    >
                        Track price momentum, platform volatility, and media
                        signals in a single immersive surface. Every row is
                        tuned for fast scanning and deeper discovery.
                    </motion.p>

                    <motion.div
                        variants={itemVariants}
                        className="flex flex-wrap gap-4"
                    >
                        {auth.user ? (
                            <Link
                                href={dashboard()}
                                className="inline-flex items-center justify-center gap-2 rounded-full bg-white px-6 py-3 text-sm font-semibold text-black transition hover:bg-slate-200"
                            >
                                <Compass className="h-4 w-4" />
                                Open dashboard
                            </Link>
                        ) : (
                            <Link
                                href={register()}
                                className="inline-flex items-center justify-center gap-2 rounded-full bg-blue-500 px-6 py-3 text-sm font-semibold text-white transition hover:bg-blue-400"
                            >
                                <Compass className="h-4 w-4" />
                                Start tracking
                            </Link>
                        )}
                        <a
                            href="#rows"
                            className="inline-flex items-center justify-center gap-2 rounded-full border border-white/20 bg-white/5 px-6 py-3 text-sm font-semibold text-white transition hover:border-white/40"
                        >
                            <Sparkles className="h-4 w-4" />
                            Explore catalog
                        </a>
                    </motion.div>

                    <motion.div
                        variants={itemVariants}
                        className="flex flex-wrap gap-4"
                    >
                        <MetricBadge label="Active signals" value="250K+" />
                        <MetricBadge
                            label="Markets"
                            value="120+"
                            accent="positive"
                        />
                        <MetricBadge
                            label="Platforms"
                            value="15"
                            accent="alert"
                        />
                    </motion.div>

                    <motion.div
                        variants={itemVariants}
                        className="hidden flex-wrap gap-3 lg:flex"
                    >
                        <OrbitStat label="BTC Index" value="Realtime" />
                        <OrbitStat label="Latency" value="Sub 2s" />
                    </motion.div>

                    {/* Carousel Navigation */}
                    {games.length > 1 && (
                        <motion.div
                            variants={itemVariants}
                            className="flex items-center gap-3 pt-4"
                        >
                            {games.map((g, idx) => (
                                <button
                                    key={g.id}
                                    onClick={() => setActiveIndex(idx)}
                                    className={`h-1.5 rounded-full transition-all duration-500 ${
                                        idx === activeIndex
                                            ? 'w-8 bg-blue-500'
                                            : 'w-2 bg-white/20 hover:bg-white/40'
                                    }`}
                                    aria-label={`Go to slide ${idx + 1}`}
                                />
                            ))}
                        </motion.div>
                    )}
                </motion.div>

                <div className="group relative">
                    <div className="absolute top-12 -left-6 hidden h-40 w-40 rounded-full border border-white/10 bg-white/5 blur-2xl lg:block" />

                    {/* Carousel Controls */}
                    {games.length > 1 && (
                        <>
                            <button
                                onClick={prevSlide}
                                className="absolute top-1/2 -left-12 z-20 -translate-y-1/2 rounded-full border border-white/10 bg-black/40 p-2 text-white opacity-0 transition-all group-hover:left-4 group-hover:opacity-100 hover:bg-black/60"
                                aria-label="Previous game"
                            >
                                <ChevronLeft className="h-6 w-6" />
                            </button>
                            <button
                                onClick={nextSlide}
                                className="absolute top-1/2 -right-12 z-20 -translate-y-1/2 rounded-full border border-white/10 bg-black/40 p-2 text-white opacity-0 transition-all group-hover:right-4 group-hover:opacity-100 hover:bg-black/60"
                                aria-label="Next game"
                            >
                                <ChevronRight className="h-6 w-6" />
                            </button>
                        </>
                    )}

                    <motion.div
                        key={activeGame?.id ?? 'card-empty'}
                        initial={{ opacity: 0, scale: 0.95, y: 20 }}
                        animate={{ opacity: 1, scale: 1, y: 0 }}
                        exit={{ opacity: 0, scale: 0.95, y: -20 }}
                        transition={{
                            duration: 0.6,
                            ease: 'easeOut',
                        }}
                    >
                        <Dialog
                            open={isVideoOpen}
                            onOpenChange={setIsVideoOpen}
                        >
                            <DialogTrigger asChild>
                                <div
                                    className="group/card block w-full cursor-pointer border-0 bg-transparent p-0 text-left ring-0 outline-none"
                                    role="button"
                                    tabIndex={0}
                                    onClick={() => setIsVideoOpen(true)}
                                    onKeyDown={(e) => {
                                        if (
                                            e.key === 'Enter' ||
                                            e.key === ' '
                                        ) {
                                            e.preventDefault();
                                            setIsVideoOpen(true);
                                        }
                                    }}
                                >
                                    <AppleTvCard
                                        className="aspect-video w-full overflow-hidden border border-white/10 bg-black md:aspect-[4/3] lg:aspect-video"
                                        shineClassName="mix-blend-screen"
                                    >
                                        {/* Full-bleed Video Background (Unmuted Autoplay) */}
                                        {videoId ? (
                                            <div className="absolute inset-0 z-0 overflow-hidden">
                                                <iframe
                                                    src={`https://www.youtube.com/embed/${videoId}?autoplay=1&mute=0&controls=0&loop=1&playlist=${videoId}&modestbranding=1&showinfo=0&rel=0&iv_load_policy=3&fs=0&disablekb=1&enablejsapi=1&vq=hd1080&origin=${window.location.origin}`}
                                                    className="pointer-events-none absolute top-1/2 left-1/2 h-full w-full -translate-x-1/2 -translate-y-1/2 border-0 object-cover"
                                                    allow="autoplay; encrypted-media"
                                                    title="Spotlight Trailer"
                                                />
                                                <div className="absolute inset-0 z-10 bg-gradient-to-t from-black/90 via-transparent to-black/30" />

                                                {/* Mute Toggle */}
                                                <button
                                                    onClick={(e) => {
                                                        e.stopPropagation();
                                                        e.preventDefault();
                                                        setIsMuted(!isMuted);
                                                    }}
                                                    className="pointer-events-auto absolute right-8 bottom-8 z-30 flex h-14 w-14 items-center justify-center rounded-full border border-white/20 bg-black/60 text-white shadow-lg backdrop-blur-md transition-all hover:scale-110 hover:bg-black/80 active:scale-95"
                                                    aria-label={
                                                        isMuted
                                                            ? 'Unmute'
                                                            : 'Mute'
                                                    }
                                                >
                                                    {isMuted ? (
                                                        <VolumeX className="h-6 w-6" />
                                                    ) : (
                                                        <Volume2 className="h-6 w-6" />
                                                    )}
                                                </button>
                                            </div>
                                        ) : (
                                            heroImage && (
                                                <img
                                                    src={heroImage}
                                                    alt=""
                                                    className="absolute inset-0 -z-10 h-full w-full object-cover"
                                                    loading="lazy"
                                                />
                                            )
                                        )}

                                        <div className="pointer-events-none relative z-20 flex h-full flex-col justify-between p-8">
                                            <div className="pointer-events-auto flex items-center justify-between">
                                                <div className="flex items-center gap-2 text-[10px] font-bold tracking-[0.3em] text-blue-400 uppercase">
                                                    <Sparkles className="h-3 w-3" />
                                                    Featured Spotlight
                                                </div>

                                                <div className="flex gap-1">
                                                    {games.map((_, i) => (
                                                        <div
                                                            key={i}
                                                            className={`h-1 w-4 rounded-full transition-colors ${i === activeIndex ? 'bg-blue-500' : 'bg-white/20'}`}
                                                        />
                                                    ))}
                                                </div>
                                            </div>

                                            <div className="pointer-events-auto space-y-4">
                                                <div className="space-y-2">
                                                    <h2 className="text-3xl font-black tracking-tight text-white drop-shadow-2xl">
                                                        {heroTitle}
                                                    </h2>
                                                    <p className="line-clamp-2 max-w-sm text-sm font-medium text-slate-200/90 drop-shadow-md">
                                                        {activeGame?.description ||
                                                            activeGame?.synopsis ||
                                                            'Cinematic pricing intelligence remixed.'}
                                                    </p>
                                                </div>

                                                {/* Video Switcher */}
                                                {trailers.length > 1 && (
                                                    <div className="flex flex-wrap gap-2">
                                                        {trailers.map(
                                                            (t, idx) => (
                                                                <button
                                                                    key={
                                                                        t.video_id ||
                                                                        idx
                                                                    }
                                                                    onClick={(
                                                                        e,
                                                                    ) => {
                                                                        e.preventDefault();
                                                                        e.stopPropagation();
                                                                        setActiveVideoIndex(
                                                                            idx,
                                                                        );
                                                                    }}
                                                                    className={`rounded-md border px-2 py-1 text-[10px] font-bold tracking-wider uppercase transition-all ${
                                                                        idx ===
                                                                        activeVideoIndex
                                                                            ? 'border-blue-500 bg-blue-500/20 text-blue-300'
                                                                            : 'border-white/10 bg-black/40 text-white/60 hover:bg-white/10 hover:text-white'
                                                                    }`}
                                                                >
                                                                    {t.name ||
                                                                        `Video ${idx + 1}`}
                                                                </button>
                                                            ),
                                                        )}
                                                    </div>
                                                )}

                                                <div className="flex items-center gap-4">
                                                    {videoId && (
                                                        <div className="group flex items-center gap-3 rounded-full bg-white px-6 py-2.5 text-sm font-bold text-black transition-all hover:scale-105 active:scale-95">
                                                            <Maximize2 className="h-4 w-4" />
                                                            Expand Cinema
                                                        </div>
                                                    )}

                                                    <Link
                                                        href={`/dashboard/${activeGame?.id}`}
                                                        onClick={(e) =>
                                                            e.stopPropagation()
                                                        }
                                                        className="rounded-full border border-white/20 bg-white/10 px-6 py-2.5 text-sm font-bold text-white backdrop-blur-md transition-all hover:bg-white/20"
                                                    >
                                                        View Analysis
                                                    </Link>
                                                </div>
                                            </div>
                                        </div>
                                    </AppleTvCard>
                                </div>
                            </DialogTrigger>

                            {/* Fullscreen Video Modal (Highest Quality, Unmuted) */}
                            {videoId && (
                                <DialogContent className="aspect-video w-[98vw] max-w-[90rem] overflow-hidden border-white/10 bg-black/95 p-0 shadow-2xl sm:rounded-2xl">
                                    <iframe
                                        src={`https://www.youtube.com/embed/${videoId}?autoplay=1&mute=0&controls=1&modestbranding=1&rel=0&fs=1&vq=hd1080`}
                                        className="h-full w-full border-0"
                                        allow="autoplay; encrypted-media; fullscreen"
                                        allowFullScreen
                                        title="Trailer Fullscreen"
                                    />
                                </DialogContent>
                            )}
                        </Dialog>
                    </motion.div>
                </div>
            </div>

            {/* Scroll Instruction */}
            <div className="mt-12 lg:absolute lg:bottom-12 lg:left-1/2 lg:mt-0 lg:-translate-x-1/2">
                <NeonCta />
            </div>
        </section>
    );
}
