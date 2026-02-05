import { motion, useMotionValue, useSpring, useTransform } from 'framer-motion';
import { Play } from 'lucide-react';
import React, { useEffect, useRef, useState } from 'react';

interface SpatialImageProps {
    src: string;
    alt: string;
    videoUrl?: string | null;
    className?: string;
    aspectRatio?: 'video' | 'portrait' | 'square';
    showGlare?: boolean;
}

export const SpatialImage: React.FC<SpatialImageProps> = ({
    src,
    alt,
    videoUrl,
    className = '',
    aspectRatio = 'video',
    showGlare = true,
}) => {
    const containerRef = useRef<HTMLDivElement>(null);
    const [hovered, setHovered] = useState(false);
    
    // Mouse tracking for parallax and glare
    const mouseX = useMotionValue(0.5);
    const mouseY = useMotionValue(0.5);

    // Smooth springs for high-end feel
    const springConfig = { damping: 30, stiffness: 200, mass: 0.5 };
    const rotateX = useSpring(useTransform(mouseY, [0, 1], [10, -10]), springConfig);
    const rotateY = useSpring(useTransform(mouseX, [0, 1], [-10, 10]), springConfig);
    
    const glareX = useSpring(useTransform(mouseX, [0, 1], [0, 100]), springConfig);
    const glareY = useSpring(useTransform(mouseY, [0, 1], [0, 100]), springConfig);
    
    const contentTranslateX = useSpring(useTransform(mouseX, [0, 1], [-5, 5]), springConfig);
    const contentTranslateY = useSpring(useTransform(mouseY, [0, 1], [-5, 5]), springConfig);

    const handleMouseMove = (e: React.MouseEvent<HTMLDivElement>) => {
        if (!containerRef.current) return;
        const rect = containerRef.current.getBoundingClientRect();
        mouseX.set((e.clientX - rect.left) / rect.width);
        mouseY.set((e.clientY - rect.top) / rect.height);
    };

    const handleMouseLeave = () => {
        setHovered(false);
        mouseX.set(0.5);
        mouseY.set(0.5);
    };

    const aspectClasses = {
        video: 'aspect-video',
        portrait: 'aspect-[3/4]',
        square: 'aspect-square',
    };

    return (
        <div
            ref={containerRef}
            className={`group relative perspective-1000 ${aspectClasses[aspectRatio]} ${className}`}
            onMouseMove={handleMouseMove}
            onMouseEnter={() => setHovered(true)}
            onMouseLeave={handleMouseLeave}
        >
            <motion.div
                className="relative h-full w-full overflow-hidden rounded-[2rem] border border-white/10 bg-zinc-900 shadow-2xl transition-all duration-500 will-change-transform group-hover:border-white/25 group-hover:shadow-blue-500/20"
                style={{
                    rotateX,
                    rotateY,
                    transformStyle: 'preserve-3d',
                }}
            >
                {/* Parallax Content Layer */}
                <motion.div
                    className="absolute -inset-4 z-0"
                    style={{
                        x: contentTranslateX,
                        y: contentTranslateY,
                    }}
                >
                    <img
                        src={src}
                        alt={alt}
                        className="h-full w-full object-cover transition-transform duration-700 group-hover:scale-105"
                    />
                </motion.div>

                {/* Video Overlay / Trigger */}
                {videoUrl && (
                    <div className="absolute inset-0 z-10 flex items-center justify-center bg-black/20 opacity-0 transition-opacity duration-500 group-hover:opacity-100">
                        <div className="flex h-16 w-16 items-center justify-center rounded-full bg-white/10 backdrop-blur-xl border border-white/20 text-white shadow-2xl">
                            <Play className="h-8 w-8 fill-current" />
                        </div>
                    </div>
                )}

                {/* Glass Distortion Overlay (Apple Style) */}
                <div className="absolute inset-0 z-20 pointer-events-none">
                    <div className="absolute inset-0 bg-gradient-to-tr from-white/5 via-transparent to-white/5" />
                    <div className="absolute inset-0 ring-1 ring-inset ring-white/10 group-hover:ring-white/20 transition-all" />
                </div>

                {/* Specular Glare */}
                {showGlare && (
                    <motion.div
                        className="pointer-events-none absolute inset-0 z-30 opacity-0 transition-opacity duration-500 group-hover:opacity-100"
                        style={{
                            background: useTransform(
                                [glareX, glareY],
                                ([x, y]) => `radial-gradient(circle at ${x}% ${y}%, rgba(255,255,255,0.3) 0%, transparent 60%)`
                            ),
                        }}
                    />
                )}

                {/* Bottom Shadow Depth */}
                <div className="absolute inset-x-0 bottom-0 z-10 h-1/3 bg-gradient-to-t from-black/60 to-transparent" />
            </motion.div>

            {/* Reflection Shadow (External) */}
            <motion.div
                className="absolute -bottom-8 left-1/2 -z-10 h-12 w-[90%] -translate-x-1/2 rounded-[100%] bg-black/40 blur-2xl transition-opacity duration-500"
                initial={{ opacity: 0 }}
                animate={{ opacity: hovered ? 0.6 : 0.2 }}
            />
        </div>
    );
};
