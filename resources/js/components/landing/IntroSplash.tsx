import { AnimatePresence, motion } from 'framer-motion';
import { useEffect, useState } from 'react';

interface IntroSplashProps {
    onComplete: () => void;
}

export default function IntroSplash({ onComplete }: IntroSplashProps) {
    const [phase, setPhase] = useState<'logo' | 'ripple' | 'morph'>('logo');

    // Reduced motion preference
    const prefersReducedMotion =
        typeof window !== 'undefined'
            ? window.matchMedia('(prefers-reduced-motion: reduce)').matches
            : false;

    useEffect(() => {
        // Adjust timing based on motion preference
        const rippleDelay = prefersReducedMotion ? 500 : 2000;
        const morphDelay = prefersReducedMotion ? 1000 : 3500;

        const rippleTimer = setTimeout(() => setPhase('ripple'), rippleDelay);
        const morphTimer = setTimeout(() => setPhase('morph'), morphDelay);

        return () => {
            clearTimeout(rippleTimer);
            clearTimeout(morphTimer);
        };
    }, [prefersReducedMotion]);

    const duration = prefersReducedMotion ? 0.1 : 1.2;
    const rippleDuration = prefersReducedMotion ? 0.3 : 1.5;

    return (
        <AnimatePresence onExitComplete={onComplete}>
            {phase !== 'morph' && (
                <motion.div
                    className="fixed inset-0 z-[100] flex items-center justify-center overflow-hidden bg-black"
                    initial={{ opacity: 1 }}
                    exit={{ opacity: 0 }}
                    transition={{ duration: 0.8, ease: 'easeInOut' }}
                >
                    {/* Cinematic Lighting */}
                    <div className="pointer-events-none absolute inset-0 bg-gradient-to-b from-neutral-900/20 via-black to-black" />

                    {/* Logo Container - Frosted Glass Effect */}
                    <motion.div
                        initial={{ opacity: 0, scale: 0.9, y: 10 }}
                        animate={{ opacity: 1, scale: 1, y: 0 }}
                        transition={{ duration: duration, ease: 'easeOut' }}
                        className="relative z-10 flex h-48 w-48 items-center justify-center rounded-3xl border border-white/10 bg-white/5 p-6 shadow-2xl backdrop-blur-md"
                    >
                        <img
                            src="/GC Landing Page Logo.png"
                            alt="Game Compare"
                            className="h-full w-full object-contain drop-shadow-[0_0_15px_rgba(255,255,255,0.3)]"
                        />
                    </motion.div>

                    {/* The Ripple - Expands from center */}
                    {phase === 'ripple' && (
                        <motion.div
                            className="absolute z-20 rounded-full bg-white mix-blend-overlay"
                            initial={{ width: 0, height: 0, opacity: 0.8 }}
                            animate={{
                                width: '250vmax',
                                height: '250vmax',
                                opacity: 0,
                            }}
                            transition={{
                                duration: rippleDuration,
                                ease: 'circOut',
                            }}
                            style={{
                                left: '50%',
                                top: '50%',
                                x: '-50%',
                                y: '-50%',
                            }}
                        />
                    )}
                </motion.div>
            )}
        </AnimatePresence>
    );
}
