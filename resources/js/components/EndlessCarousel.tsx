import { index as compareIndex } from '@/actions/App/Http/Controllers/CompareController';
import { toUrl } from '@/lib/utils';
import { type Game } from '@/types';
import { ChevronLeftIcon, ChevronRightIcon } from '@heroicons/react/24/outline';
import { Link } from '@inertiajs/react';
import { motion, useInView } from 'framer-motion';
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
    const sectionRef = useRef<HTMLDivElement>(null);
    const isInView = useInView(sectionRef, { once: false, margin: "200px" });
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
            else if (window.innerWidth < 1280)
                setVisibleCount(3);
            else setVisibleCount(4);

            if (containerRef.current) {
                setContainerWidth(containerRef.current.offsetWidth);
            }
        };

        handleResize();
        window.addEventListener('resize', handleResize);
        return () => window.removeEventListener('resize', handleResize);
    }, []);

    const totalItems = games.length;
    // For large lists, we only double the set to save memory
    const displayGames = totalItems > 10 ? [...games, ...games] : [...games, ...games, ...games];
    const multiplier = totalItems > 10 ? 2 : 3;

    useEffect(() => {
        setCurrentIndex(totalItems);
    }, [totalItems]);

    // Auto-advance
    useEffect(() => {
        if (!isInView || isHovered || prefersReducedMotion || games.length === 0) return;

        const interval = setInterval(() => {
            setCurrentIndex((prev) => prev + 1);
        }, 4000);

        return () => clearInterval(interval);
    }, [isInView, isHovered, prefersReducedMotion, games.length]);

    const handleTransitionEnd = () => {
        if (currentIndex >= totalItems * (multiplier - 1)) {
            setIsTransitioning(false);
            setCurrentIndex(totalItems + (currentIndex % totalItems));
        }
        else if (currentIndex < totalItems && multiplier === 3) {
            setIsTransitioning(false);
            setCurrentIndex(totalItems * 2 - (totalItems - currentIndex));
        }
    };

    useEffect(() => {
        if (!isTransitioning) {
            containerRef.current?.getBoundingClientRect();
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

    const itemWidth = containerWidth / visibleCount;
    const translateX = -(currentIndex * itemWidth);

    return (
        <div
            ref={sectionRef}
            className={`group relative py-8 ${className}`}
            onMouseEnter={() => setIsHovered(true)}
            onMouseLeave={() => setIsHovered(false)}
        >
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
                    href={toUrl(compareIndex())}
                    className="text-[10px] font-bold tracking-[0.2em] text-white/50 uppercase transition-colors hover:text-white"
                >
                    See All
                </Link>
            </div>

            <div className="relative overflow-hidden">
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

                <button
                    onClick={handleNext}
                    aria-label="Next slide"
                    className={`absolute top-0 right-0 bottom-0 z-20 flex w-12 items-center justify-center bg-black/30 backdrop-blur-sm transition-all duration-300 hover:w-16 hover:bg-black/60 ${
                        isHovered || prefersReducedMotion
                            ? 'translate-x-0 opacity-100'
                            : '-translate-x-full opacity-0'
                    }`}
                >
                    <ChevronRightIcon className="h-8 w-8 text-white drop-shadow-lg" />
                </button>

                <div className="w-full" ref={containerRef}>
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
                        {isInView ? displayGames.map((game, index) => (
                            <div
                                key={`${game.id}-${index}`}
                                className="flex-none px-2"
                                style={{ width: `${itemWidth}px` }}
                            >
                                <GameCard
                                    game={game}
                                    className="aspect-[3/4]"
                                />
                            </div>
                        )) : (
                            <div className="h-[400px] w-full" />
                        )}
                    </div>
                </div>
            </div>
        </div>
    );
}
