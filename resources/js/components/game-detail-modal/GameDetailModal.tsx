import { AnimatePresence, motion } from 'framer-motion';
import { useEffect, useMemo, useRef, useState } from 'react';
import { useGameCard } from './GameCardContext';

/**
 * Bespoke Game Transition - Uses ALL game media assets for a cinematic transition.
 *
 * Phases:
 * 1. LOADING - Fetching media data (show quick spinner)
 * 2. WARP - Screen distorts with game's primary color
 * 3. FLASH - Bright flash at peak distortion
 * 4. REVEAL - Montage of screenshots/artworks with cover art center
 * 5. NAVIGATING - Fade to the actual page
 * 6. EXIT - Clean up
 */
export function GameDetailModal() {
    const { selectedGame, phase, closeGameCard } = useGameCard();
    const containerRef = useRef<HTMLDivElement>(null);
    const [currentImageIndex, setCurrentImageIndex] = useState(0);
    const [distortion, setDistortion] = useState(0);

    // Combine all available images for the montage
    const allImages = useMemo(() => {
        if (!selectedGame) return [];
        const images: string[] = [];

        // Priority: Artworks first (more cinematic), then screenshots
        if (selectedGame.artworks?.length) {
            images.push(
                ...selectedGame.artworks.map((url) =>
                    url.replace('t_thumb', 't_1080p'),
                ),
            );
        }
        if (selectedGame.screenshots?.length) {
            images.push(
                ...selectedGame.screenshots.map((url) =>
                    url.replace('t_thumb', 't_1080p'),
                ),
            );
        }
        // Hero as backup
        if (selectedGame.heroUrl) {
            images.push(selectedGame.heroUrl);
        }
        // Cover as final fallback
        if (images.length === 0 && selectedGame.coverUrl) {
            images.push(selectedGame.coverUrl);
        }

        return images.slice(0, 6); // Max 6 images
    }, [selectedGame]);

    // Cycle through images during reveal phase
    useEffect(() => {
        if (phase === 'reveal' && allImages.length > 1) {
            const interval = setInterval(() => {
                setCurrentImageIndex((prev) => (prev + 1) % allImages.length);
            }, 250); // Fast cycling for dramatic effect
            return () => clearInterval(interval);
        }
        return undefined;
    }, [phase, allImages.length]);

    // Animate distortion during warp and loading phase
    useEffect(() => {
        if (phase === 'loading') {
            // Quick build-up during loading
            const start = Date.now();
            const duration = 200;
            const animate = () => {
                const elapsed = Date.now() - start;
                const progress = Math.min(elapsed / duration, 1);
                const eased = 1 - Math.pow(1 - progress, 2);
                setDistortion(eased * 40); // Lower intensity for loading
                if (progress < 1) {
                    requestAnimationFrame(animate);
                }
            };
            requestAnimationFrame(animate);
        } else if (phase === 'warp') {
            const start = Date.now();
            const duration = 350;
            const animate = () => {
                const elapsed = Date.now() - start;
                const progress = Math.min(elapsed / duration, 1);
                // Ease-out for smooth peak
                const eased = 1 - Math.pow(1 - progress, 3);
                setDistortion(40 + eased * 60); // Continue from loading level
                if (progress < 1) {
                    requestAnimationFrame(animate);
                }
            };
            requestAnimationFrame(animate);
        } else if (phase === 'idle') {
            setDistortion(0);
            setCurrentImageIndex(0);
        }
    }, [phase]);

    // Lock body scroll
    useEffect(() => {
        if (phase !== 'idle') {
            document.body.style.overflow = 'hidden';
        } else {
            document.body.style.overflow = '';
        }
        return () => {
            document.body.style.overflow = '';
        };
    }, [phase]);

    // ESC to abort
    useEffect(() => {
        const handleKeyDown = (e: KeyboardEvent) => {
            if (e.key === 'Escape' && phase !== 'idle') {
                closeGameCard();
            }
        };
        window.addEventListener('keydown', handleKeyDown);
        return () => window.removeEventListener('keydown', handleKeyDown);
    }, [phase, closeGameCard]);

    // Theme colors with fallbacks - ensure all values are defined
    const rawTheme = selectedGame?.theme;
    const theme = {
        primary: rawTheme?.primary || '#3b82f6',
        accent: rawTheme?.accent || '#60a5fa',
        background: rawTheme?.background || '#030712',
        surface: rawTheme?.surface || '#111827',
    };

    const toRgba = (hex: string, alpha: number): string => {
        const raw = hex.replace('#', '').trim();
        if (raw.length !== 3 && raw.length !== 6) {
            return `rgba(59, 130, 246, ${alpha})`;
        }
        const full =
            raw.length === 3
                ? raw
                      .split('')
                      .map((c) => c + c)
                      .join('')
                : raw;
        const r = parseInt(full.slice(0, 2), 16);
        const g = parseInt(full.slice(2, 4), 16);
        const b = parseInt(full.slice(4, 6), 16);
        return `rgba(${r}, ${g}, ${b}, ${alpha})`;
    };

    const isActive = phase !== 'idle';

    return (
        <>
            {/* SVG Filter for Distortion */}
            <svg
                style={{
                    position: 'absolute',
                    width: 0,
                    height: 0,
                    pointerEvents: 'none',
                }}
            >
                <defs>
                    <filter id="bespoke-warp">
                        <feTurbulence
                            type="fractalNoise"
                            baseFrequency="0.015"
                            numOctaves="3"
                            seed={Math.floor(distortion)}
                            result="noise"
                        />
                        <feDisplacementMap
                            in="SourceGraphic"
                            in2="noise"
                            scale={distortion * 1.5}
                            xChannelSelector="R"
                            yChannelSelector="G"
                            result="displaced"
                        />
                        <feGaussianBlur
                            in="displaced"
                            stdDeviation={distortion / 30}
                        />
                    </filter>
                </defs>
            </svg>

            {/* CSS Animations */}
            <style>
                {`
                    @keyframes bespoke-shake {
                        0% { transform: translate(0, 0) scale(${1 - distortion / 500}); }
                        25% { transform: translate(${distortion / 8}px, -${distortion / 10}px) scale(${1 - distortion / 500}); }
                        50% { transform: translate(-${distortion / 8}px, ${distortion / 10}px) scale(${1 - distortion / 500}); }
                        75% { transform: translate(${distortion / 8}px, ${distortion / 10}px) scale(${1 - distortion / 500}); }
                        100% { transform: translate(0, 0) scale(${1 - distortion / 500}); }
                    }
                    @keyframes image-cycle {
                        0% { opacity: 0; transform: scale(1.1); }
                        10% { opacity: 1; transform: scale(1.05); }
                        90% { opacity: 1; transform: scale(1); }
                        100% { opacity: 0; transform: scale(0.98); }
                    }
                    @keyframes cover-pulse {
                        0%, 100% { transform: scale(1); filter: drop-shadow(0 0 40px ${theme.primary}); }
                        50% { transform: scale(1.02); filter: drop-shadow(0 0 80px ${theme.primary}); }
                    }
                    @keyframes cover-tilt-3d {
                        0% { transform: rotateY(-14deg) rotateX(6deg) translateZ(0); }
                        50% { transform: rotateY(14deg) rotateX(-6deg) translateZ(0); }
                        100% { transform: rotateY(-14deg) rotateX(6deg) translateZ(0); }
                    }
                    @keyframes energy-ring {
                        0% { transform: scale(0.5); opacity: 1; }
                        100% { transform: scale(3); opacity: 0; }
                    }
                    @keyframes title-glow {
                        0%, 100% { text-shadow: 0 0 20px ${theme.primary}, 0 0 40px ${toRgba(theme.primary, 0.5)}; }
                        50% { text-shadow: 0 0 40px ${theme.primary}, 0 0 80px ${toRgba(theme.primary, 0.8)}, 0 0 120px ${toRgba(theme.accent || theme.primary, 0.4)}; }
                    }
                `}
            </style>

            <AnimatePresence>
                {isActive && selectedGame && (
                    <motion.div
                        ref={containerRef}
                        className="pointer-events-none fixed inset-0 z-[100000] overflow-hidden"
                        initial={{ opacity: 0 }}
                        animate={{ opacity: 1 }}
                        exit={{ opacity: 0 }}
                        transition={{ duration: 0.2 }}
                    >
                        {/* Background - Game's background color with radial gradient */}
                        <motion.div
                            className="absolute inset-0"
                            initial={{ opacity: 0 }}
                            animate={{
                                opacity:
                                    phase === 'reveal' || phase === 'navigating'
                                        ? 1
                                        : 0,
                            }}
                            transition={{ duration: 0.3 }}
                            style={{
                                backgroundColor: theme.background,
                                backgroundImage: `
                                    radial-gradient(ellipse at 30% 20%, ${toRgba(theme.primary, 0.4)} 0%, transparent 50%),
                                    radial-gradient(ellipse at 70% 80%, ${toRgba(theme.accent || theme.primary, 0.3)} 0%, transparent 50%),
                                    radial-gradient(circle at center, ${toRgba(theme.surface || theme.background, 0.8)} 0%, ${theme.background} 70%)
                                `,
                            }}
                        />

                        {/* Image Montage Layer - Fast cycling of screenshots/artworks */}
                        {phase === 'reveal' && allImages.length > 0 && (
                            <div className="absolute inset-0">
                                {allImages.map((url, index) => (
                                    <motion.div
                                        key={url}
                                        className="absolute inset-0"
                                        initial={{ opacity: 0, scale: 1.1 }}
                                        animate={{
                                            opacity:
                                                index === currentImageIndex
                                                    ? 0.4
                                                    : 0,
                                            scale:
                                                index === currentImageIndex
                                                    ? 1
                                                    : 1.1,
                                        }}
                                        transition={{ duration: 0.2 }}
                                    >
                                        <img
                                            src={url}
                                            alt=""
                                            className="h-full w-full object-cover"
                                            style={{
                                                filter: `blur(4px) saturate(1.2)`,
                                            }}
                                        />
                                        {/* Gradient overlay */}
                                        <div
                                            className="absolute inset-0"
                                            style={{
                                                background: `linear-gradient(to bottom, ${toRgba(theme.background, 0.6)}, ${toRgba(theme.background, 0.9)})`,
                                            }}
                                        />
                                    </motion.div>
                                ))}
                            </div>
                        )}

                        {/* Energy Rings */}
                        {(phase === 'reveal' || phase === 'navigating') && (
                            <div className="absolute inset-0 flex items-center justify-center">
                                {[...Array(3)].map((_, i) => (
                                    <div
                                        key={i}
                                        className="absolute h-64 w-64 rounded-full border-2"
                                        style={{
                                            borderColor: toRgba(
                                                theme.primary,
                                                0.6,
                                            ),
                                            animation: `energy-ring 1.5s ease-out infinite`,
                                            animationDelay: `${i * 0.3}s`,
                                        }}
                                    />
                                ))}
                            </div>
                        )}

                        {/* Cover Art - Central Focus */}
                        {(phase === 'reveal' || phase === 'navigating') && (
                            <motion.div
                                className="absolute inset-0 flex items-center justify-center"
                                initial={{ opacity: 0, scale: 0.5, y: 50 }}
                                animate={{
                                    opacity: 1,
                                    scale: 1,
                                    y: 0,
                                }}
                                transition={{
                                    type: 'spring',
                                    stiffness: 200,
                                    damping: 20,
                                    delay: 0.1,
                                }}
                            >
                                <div className="relative">
                                    {/* Glow behind cover */}
                                    <div
                                        className="absolute -inset-8 rounded-3xl blur-3xl"
                                        style={{
                                            backgroundColor: theme.primary,
                                            opacity: 0.5,
                                        }}
                                    />

                                    {/* 3D Cover build (transition-only, no card mutation) */}
                                    <div
                                        className="relative mx-auto h-80 w-[15rem] md:h-96 md:w-[18rem] lg:h-[28rem] lg:w-[20rem]"
                                        style={{ perspective: '1400px' }}
                                    >
                                        <div
                                            className="relative h-full w-full"
                                            style={{
                                                transformStyle: 'preserve-3d',
                                                animation:
                                                    'cover-tilt-3d 3.4s ease-in-out infinite',
                                            }}
                                        >
                                            {/* Front face */}
                                            <motion.img
                                                src={selectedGame.coverUrl}
                                                alt={selectedGame.name}
                                                className="absolute inset-0 z-10 h-full w-full rounded-2xl object-cover"
                                                style={{
                                                    transform: 'translateZ(50px)',
                                                    animation:
                                                        'cover-pulse 2s ease-in-out infinite',
                                                    boxShadow: `0 0 60px ${toRgba(theme.primary, 0.6)}, 0 25px 50px -12px rgba(0, 0, 0, 0.8)`,
                                                }}
                                            />

                                            {/* Back plate for depth */}
                                            <div
                                                className="absolute inset-0 rounded-2xl"
                                                style={{
                                                    transform: 'translateZ(-4px)',
                                                    background:
                                                        theme.surface || theme.background,
                                                    boxShadow:
                                                        'inset 0 0 40px rgba(0,0,0,0.55)',
                                                }}
                                            />

                                            {/* Left edge */}
                                            <div
                                                className="absolute left-0 top-0 h-full w-6 rounded-l-2xl"
                                                style={{
                                                    transform:
                                                        'rotateY(-90deg) translateZ(26px)',
                                                    transformOrigin: 'left',
                                                    background: `linear-gradient(180deg, ${toRgba(theme.primary, 0.35)}, ${toRgba(theme.background, 0.9)})`,
                                                }}
                                            />

                                            {/* Right edge */}
                                            <div
                                                className="absolute right-0 top-0 h-full w-6 rounded-r-2xl"
                                                style={{
                                                    transform:
                                                        'rotateY(90deg) translateZ(26px)',
                                                    transformOrigin: 'right',
                                                    background: `linear-gradient(180deg, ${toRgba(theme.accent || theme.primary, 0.35)}, ${toRgba(theme.background, 0.9)})`,
                                                }}
                                            />

                                            {/* Specular sweep */}
                                            <div
                                                className="absolute inset-0 rounded-2xl"
                                                style={{
                                                    transform: 'translateZ(58px)',
                                                    background:
                                                        'linear-gradient(120deg, rgba(255,255,255,0.0) 20%, rgba(255,255,255,0.25) 45%, rgba(255,255,255,0.0) 70%)',
                                                    opacity: 0.5,
                                                    mixBlendMode: 'screen',
                                                }}
                                            />
                                        </div>
                                    </div>

                                    {/* Game Title below cover */}
                                    <motion.h1
                                        className="relative z-10 mt-8 max-w-md text-center text-3xl font-black text-white md:text-4xl lg:text-5xl"
                                        initial={{ opacity: 0, y: 20 }}
                                        animate={{ opacity: 1, y: 0 }}
                                        transition={{ delay: 0.3 }}
                                        style={{
                                            animation: `title-glow 2s ease-in-out infinite`,
                                        }}
                                    >
                                        {selectedGame.name}
                                    </motion.h1>

                                    {/* Loading indicator */}
                                    <motion.div
                                        className="mt-6 flex justify-center"
                                        initial={{ opacity: 0 }}
                                        animate={{ opacity: 1 }}
                                        transition={{ delay: 0.5 }}
                                    >
                                        <div className="flex items-center gap-2">
                                            <div
                                                className="h-2 w-2 animate-pulse rounded-full"
                                                style={{
                                                    backgroundColor:
                                                        theme.accent ||
                                                        theme.primary,
                                                }}
                                            />
                                            <span
                                                className="text-sm font-medium tracking-widest uppercase"
                                                style={{
                                                    color:
                                                        theme.accent ||
                                                        theme.primary,
                                                }}
                                            >
                                                Loading
                                            </span>
                                            <div
                                                className="h-2 w-2 animate-pulse rounded-full"
                                                style={{
                                                    backgroundColor:
                                                        theme.accent ||
                                                        theme.primary,
                                                    animationDelay: '0.2s',
                                                }}
                                            />
                                        </div>
                                    </motion.div>
                                </div>
                            </motion.div>
                        )}

                        {/* Flash Effect */}
                        <motion.div
                            className="absolute inset-0 bg-white"
                            initial={{ opacity: 0 }}
                            animate={{
                                opacity: phase === 'flash' ? 1 : 0,
                            }}
                            transition={{ duration: 0.15 }}
                        />

                        {/* Warp Vignette - Colored edges during loading/warp */}
                        {(phase === 'loading' || phase === 'warp') && (
                            <motion.div
                                className="absolute inset-0"
                                initial={{ opacity: 0 }}
                                animate={{
                                    opacity: phase === 'warp' ? 1 : 0.6,
                                }}
                                exit={{ opacity: 0 }}
                                style={{
                                    background: `radial-gradient(ellipse at center, transparent 30%, ${toRgba(theme.primary, 0.8)} 100%)`,
                                }}
                            />
                        )}

                        {/* Particle burst during flash */}
                        {phase === 'flash' && (
                            <div className="absolute inset-0 flex items-center justify-center">
                                {[...Array(24)].map((_, i) => {
                                    const angle = (i / 24) * 360;
                                    const distance = 100 + Math.random() * 200;
                                    const x =
                                        Math.cos((angle * Math.PI) / 180) *
                                        distance;
                                    const y =
                                        Math.sin((angle * Math.PI) / 180) *
                                        distance;

                                    return (
                                        <motion.div
                                            key={i}
                                            className="absolute h-3 w-3 rounded-full"
                                            style={{
                                                backgroundColor: theme.primary,
                                                boxShadow: `0 0 10px ${theme.primary}`,
                                            }}
                                            initial={{
                                                x: 0,
                                                y: 0,
                                                scale: 1,
                                                opacity: 1,
                                            }}
                                            animate={{
                                                x,
                                                y,
                                                scale: 0,
                                                opacity: 0,
                                            }}
                                            transition={{
                                                duration: 0.4,
                                                ease: 'easeOut',
                                            }}
                                        />
                                    );
                                })}
                            </div>
                        )}
                    </motion.div>
                )}
            </AnimatePresence>
        </>
    );
}
