import { index as compareIndex } from '@/actions/App/Http/Controllers/CompareController';
import { type Game } from '@/types';
import { ChevronLeftIcon, ChevronRightIcon } from '@heroicons/react/24/outline';
import { Link } from '@inertiajs/react';
import { motion } from 'framer-motion';
import { useEffect, useRef, useState } from 'react';
import { GameCard } from './GameCard';

interface EndlessCarouselProps {
    title: string;
    games: Game[];
    className?: string;
}

export default function EndlessCarousel({
    title,
    games,
    className = '',
}: EndlessCarouselProps) {
    const [currentIndex, setCurrentIndex] = useState(0);
    const [isHovered, setIsHovered] = useState(false);
    const [visibleCount, setVisibleCount] = useState(6);
    const containerRef = useRef<HTMLDivElement>(null);
    const [isTransitioning, setIsTransitioning] = useState(true);
    const [containerWidth, setContainerWidth] = useState(0);

    // Reduced motion preference
    const prefersReducedMotion =
        typeof window !== 'undefined'
            ? window.matchMedia('(prefers-reduced-motion: reduce)').matches
            : false;

    // Responsive visible count
    useEffect(() => {
        const handleResize = () => {
            if (window.innerWidth < 768) setVisibleCount(2);
            else if (window.innerWidth < 1024) setVisibleCount(4);
            else setVisibleCount(6);

            // Update container width for transform calculation
            if (containerRef.current) {
                setContainerWidth(containerRef.current.offsetWidth);
            }
        };

        handleResize(); // Initial call
        window.addEventListener('resize', handleResize);
        return () => window.removeEventListener('resize', handleResize);
    }, []);

    // Triple the games to ensure smooth infinite looping
    // [games (buffer), games (active), games (buffer)]
    const displayGames = [...games, ...games, ...games];
    const totalItems = games.length;

    // Initial position: Start at the second set
    useEffect(() => {
        setCurrentIndex(totalItems);
    }, [totalItems]);

    // Auto-advance
    useEffect(() => {
        if (isHovered || prefersReducedMotion || games.length === 0) return;

        const interval = setInterval(() => {
            setCurrentIndex((prev) => prev + 1);
        }, 4000);

        return () => clearInterval(interval);
    }, [isHovered, prefersReducedMotion, games.length]);

    const handleTransitionEnd = () => {
        // If we've scrolled past the second set (to the right)
        if (currentIndex >= totalItems * 2) {
            setIsTransitioning(false);
            setCurrentIndex(totalItems + (currentIndex % totalItems));
        }
        // If we've scrolled past the first set (to the left)
        else if (currentIndex < totalItems) {
            setIsTransitioning(false);
            setCurrentIndex(totalItems * 2 - (totalItems - currentIndex));
        }
    };

    // Re-enable transition after instant jump
    useEffect(() => {
        if (!isTransitioning) {
            // Force reflow
            containerRef.current?.getBoundingClientRect();
            // Small timeout to ensure DOM update before re-enabling transition
            requestAnimationFrame(() => setIsTransitioning(true));
        }
    }, [isTransitioning]);

    const handleNext = () => {
        if (!isTransitioning) return;
        setCurrentIndex((prev) => prev + 1);
    };

    const handlePrev = () => {
        if (!isTransitioning) return;
        setCurrentIndex((prev) => prev - 1);
    };

    if (games.length === 0) return null;

    // Calculate pixel-based width for each item (accounts for gap properly)
    const itemWidth = containerWidth / visibleCount;
    const translateX = -(currentIndex * itemWidth);

    return (
        <div
            className={`group relative py-8 ${className}`}
            onMouseEnter={() => setIsHovered(true)}
            onMouseLeave={() => setIsHovered(false)}
        >
            {/* Section Title */}
            <div className="mx-auto mb-6 flex max-w-[90rem] items-end justify-between px-4 lg:px-12">
                <motion.h2
                    initial={{ opacity: 0, x: -20 }}
                    whileInView={{ opacity: 1, x: 0 }}
                    viewport={{ once: true }}
                    className="text-2xl font-black tracking-widest text-white uppercase transition-colors hover:text-cyan-300 lg:text-3xl"
                    style={{
                        textShadow:
                            '0 0 5px rgba(255,255,255,0.6), 0 0 10px rgba(6,182,212,0.5), 0 0 20px rgba(6,182,212,0.3)',
                    }}
                >
                    {title}
                </motion.h2>
                <Link
                    href={compareIndex().url}
                    className="text-[10px] font-bold tracking-[0.2em] text-white/50 uppercase transition-colors hover:text-white"
                >
                    See All
                </Link>
            </div>

            {/* Carousel Container */}
            <div className="relative overflow-hidden">
                {/* Left Arrow */}
                <button
                    onClick={handlePrev}
                    aria-label="Previous slide"
                    className={`absolute top-0 bottom-0 left-0 z-20 flex w-12 items-center justify-center bg-black/30 backdrop-blur-sm transition-all duration-300 hover:w-16 hover:bg-black/60 ${
                        isHovered || prefersReducedMotion
                            ? 'translate-x-0 opacity-100'
                            : '-translate-x-full opacity-0'
                    }`}
                >
                    <ChevronLeftIcon className="h-8 w-8 text-white drop-shadow-lg" />
                </button>

                {/* Right Arrow */}
                <button
                    onClick={handleNext}
                    aria-label="Next slide"
                    className={`absolute top-0 right-0 bottom-0 z-20 flex w-12 items-center justify-center bg-black/30 backdrop-blur-sm transition-all duration-300 hover:w-16 hover:bg-black/60 ${
                        isHovered || prefersReducedMotion
                            ? 'translate-x-0 opacity-100'
                            : 'translate-x-full opacity-0'
                    }`}
                >
                    <ChevronRightIcon className="h-8 w-8 text-white drop-shadow-lg" />
                </button>

                {/* Games Track */}
                <div className="px-4 lg:px-12" ref={containerRef}>
                    <div
                        className="flex will-change-transform"
                        style={{
                            transform: `translateX(${translateX}px)`,
                            transition: isTransitioning
                                ? 'transform 0.5s cubic-bezier(0.25, 1, 0.5, 1)'
                                : 'none',
                        }}
                        onTransitionEnd={handleTransitionEnd}
                    >
                        {displayGames.map((game, index) => (
                            <div
                                key={`${game.id}-${index}`}
                                className="flex-none px-2"
                                style={{ width: `${itemWidth}px` }}
                            >
                                <GameCard
                                    game={game}
                                    className="aspect-[2/3]"
                                />
                            </div>
                        ))}
                    </div>
                </div>
            </div>
        </div>
    );
}
