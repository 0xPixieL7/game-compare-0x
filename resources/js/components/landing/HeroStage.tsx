import { AppleTvCard } from '@/components/apple-tv-card';
import BoxReveal from '@/components/landing/BoxReveal';
import MetricBadge from '@/components/landing/MetricBadge';
import OrbitStat from '@/components/landing/OrbitStat';
import SignalPill from '@/components/landing/SignalPill';
import { Dialog, DialogContent, DialogTrigger } from '@/components/ui/dialog';
import { dashboard, register } from '@/routes';
import { type Game, type SharedData } from '@/types';
import { Link, usePage } from '@inertiajs/react';
import { motion } from 'framer-motion';
import { Compass, Maximize2, Sparkles } from 'lucide-react';
import { useEffect, useRef, useState } from 'react';

interface HeroStageProps {
    hero: Game | null;
}

export default function HeroStage({ hero }: HeroStageProps) {
    const { auth } = usePage<SharedData>().props;
    const heroTitle =
        hero?.canonical_name || hero?.name || 'Global game markets';
    const heroGenre = hero?.genres?.[0] ?? 'Market signal';
    const heroImage = hero?.media?.cover_url ?? hero?.media?.cover_url_thumb;
    const heroBackdrop = hero?.media?.screenshots?.[0]?.url ?? heroImage;

    // Trailer Logic
    const trailer = hero?.media?.trailers?.[0];
    const videoId = trailer?.video_id;
    const [isVideoOpen, setIsVideoOpen] = useState(false);

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

            <div className="relative z-10 grid gap-16 lg:grid-cols-[1.1fr_0.9fr]">
                <motion.div
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
                </motion.div>

                <div className="relative">
                    <div className="absolute top-12 -left-6 hidden h-40 w-40 rounded-full border border-white/10 bg-white/5 blur-2xl lg:block" />
                    <motion.div
                        initial={{ opacity: 0, scale: 0.95, y: 20 }}
                        animate={{ opacity: 1, scale: 1, y: 0 }}
                        transition={{
                            delay: 0.5,
                            duration: 0.8,
                            ease: 'easeOut',
                        }}
                    >
                        <Dialog
                            open={isVideoOpen}
                            onOpenChange={setIsVideoOpen}
                        >
                            <AppleTvCard
                                className="min-h-[420px] overflow-hidden border border-white/10 bg-black"
                                shineClassName="mix-blend-screen"
                            >
                                {/* Video Background (Muted Loop) */}
                                {videoId ? (
                                    <div className="absolute inset-0 z-0">
                                        <div className="absolute inset-0 z-10 bg-black/20" />{' '}
                                        {/* Dim overlay */}
                                        <iframe
                                            src={`https://www.youtube.com/embed/${videoId}?autoplay=1&mute=1&controls=0&loop=1&playlist=${videoId}&modestbranding=1&showinfo=0&rel=0&iv_load_policy=3&fs=0&disablekb=1`}
                                            className="pointer-events-none absolute inset-0 h-[150%] w-[150%] -translate-x-[16.67%] -translate-y-[16.67%]"
                                            allow="autoplay; encrypted-media"
                                            title="Background Trailer"
                                        />
                                    </div>
                                ) : (
                                    heroImage && (
                                        <img
                                            src={heroImage}
                                            alt=""
                                            className="absolute inset-0 -z-10 h-full w-full object-cover opacity-60"
                                            loading="lazy"
                                        />
                                    )
                                )}

                                <div className="relative z-10 flex h-full flex-col justify-between p-6">
                                    <div className="space-y-4">
                                        <div className="flex items-center gap-2 text-xs tracking-[0.2em] text-blue-200/80 uppercase">
                                            Spotlight
                                        </div>
                                        <div className="space-y-2">
                                            <h2 className="text-2xl font-semibold text-white drop-shadow-md">
                                                {heroTitle}
                                            </h2>
                                            <p className="max-w-xs text-sm text-slate-200/90 drop-shadow-sm">
                                                {hero?.pricing?.is_free
                                                    ? 'Free-to-play surge across multiple regions.'
                                                    : 'Cinematic experience. Watch the trailer now.'}
                                            </p>
                                        </div>
                                    </div>

                                    {/* Action Area (Replaces Chart) */}
                                    <div className="flex items-end justify-between">
                                        <div className="space-y-2">
                                            <div className="flex items-center gap-2 text-xs text-white/60">
                                                <span>Powered by IGDB</span>
                                            </div>
                                        </div>

                                        {videoId && (
                                            <DialogTrigger asChild>
                                                <button className="group flex items-center gap-3 rounded-xl border border-white/10 bg-black/60 px-4 py-3 text-sm font-medium text-white backdrop-blur transition hover:border-white/30 hover:bg-white/10">
                                                    <div className="relative flex h-8 w-8 items-center justify-center rounded-full bg-white text-black transition group-hover:scale-110">
                                                        <Maximize2 className="h-4 w-4" />
                                                    </div>
                                                    <span className="pr-1">
                                                        Expand Trailer
                                                    </span>
                                                </button>
                                            </DialogTrigger>
                                        )}
                                    </div>
                                </div>
                            </AppleTvCard>

                            {/* Fullscreen Video Modal */}
                            {videoId && (
                                <DialogContent className="aspect-video max-w-5xl overflow-hidden border-white/10 bg-black/95 p-0 sm:rounded-2xl">
                                    <iframe
                                        src={`https://www.youtube.com/embed/${videoId}?autoplay=1&controls=1&modestbranding=1&rel=0&fs=1`}
                                        className="h-full w-full"
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
        </section>
    );
}
