import { type Game } from '@/types';
import { ChevronLeftIcon, ChevronRightIcon } from '@heroicons/react/24/outline';
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
    const carouselRef = useRef<HTMLDivElement>(null);

    // Duplicate games for endless scrolling effect
    // If games list is short, we might need more duplication to fill the screen
    const minGamesForLoop = 12;
    const duplicationFactor =
        Math.ceil(minGamesForLoop / Math.max(games.length, 1)) + 1;

    // Create a larger set for smooth infinite scrolling
    const duplicatedGames = Array(duplicationFactor).fill(games).flat();

    const visibleCount = 6; // Number of items visible at once (responsive?)

    useEffect(() => {
        // Start from a middle point to allow scrolling left immediately
        setCurrentIndex(games.length);
    }, [games.length]);

    const scrollTo = (index: number) => {
        if (carouselRef.current) {
            const containerWidth = carouselRef.current.clientWidth;
            const itemWidth = containerWidth / visibleCount;

            carouselRef.current.scrollTo({
                left: index * itemWidth,
                behavior: 'smooth',
            });
            setCurrentIndex(index);
        }
    };

    const nextSlide = () => {
        const newIndex = currentIndex + 1;
        scrollTo(newIndex);

        // Reset position silently if too far right
        if (newIndex >= duplicatedGames.length - visibleCount) {
            setTimeout(() => {
                if (carouselRef.current) {
                    const containerWidth = carouselRef.current.clientWidth;
                    const itemWidth = containerWidth / visibleCount;
                    const resetIndex = games.length + (newIndex % games.length);

                    carouselRef.current.scrollTo({
                        left: resetIndex * itemWidth,
                        behavior: 'auto', // Instant jump
                    });
                    setCurrentIndex(resetIndex);
                }
            }, 500); // Wait for smooth scroll to finish
        }
    };

    const prevSlide = () => {
        const newIndex = currentIndex - 1;
        scrollTo(newIndex);

        // Reset position silently if too far left
        if (newIndex <= visibleCount) {
            setTimeout(() => {
                if (carouselRef.current) {
                    const containerWidth = carouselRef.current.clientWidth;
                    const itemWidth = containerWidth / visibleCount;
                    const resetIndex =
                        games.length * (duplicationFactor - 2) +
                        (newIndex % games.length);

                    carouselRef.current.scrollTo({
                        left: resetIndex * itemWidth,
                        behavior: 'auto',
                    });
                    setCurrentIndex(resetIndex);
                }
            }, 500);
        }
    };

    if (games.length === 0) {
        return null;
    }

    return (
        <div
            className={`group relative py-8 ${className}`}
            onMouseEnter={() => setIsHovered(true)}
            onMouseLeave={() => setIsHovered(false)}
        >
            {/* Section Title */}
            <div className="mx-auto mb-4 flex max-w-[90rem] items-end justify-between px-4 lg:px-12">
                <h2 className="text-xl font-bold text-white transition-colors hover:text-blue-400 lg:text-2xl">
                    {title}
                </h2>
                <div className="text-xs font-medium tracking-wider text-gray-500 uppercase">
                    See All
                </div>
            </div>

            {/* Carousel Container */}
            <div className="relative">
                {/* Left Arrow */}
                <button
                    onClick={prevSlide}
                    className={`absolute top-0 bottom-0 left-0 z-20 flex w-12 items-center justify-center bg-black/30 backdrop-blur-sm transition-all duration-300 hover:w-16 hover:bg-black/60 ${
                        isHovered
                            ? 'translate-x-0 opacity-100'
                            : '-translate-x-full opacity-0'
                    }`}
                >
                    <ChevronLeftIcon className="h-8 w-8 text-white drop-shadow-lg" />
                </button>

                {/* Right Arrow */}
                <button
                    onClick={nextSlide}
                    className={`absolute top-0 right-0 bottom-0 z-20 flex w-12 items-center justify-center bg-black/30 backdrop-blur-sm transition-all duration-300 hover:w-16 hover:bg-black/60 ${
                        isHovered
                            ? 'translate-x-0 opacity-100'
                            : 'translate-x-full opacity-0'
                    }`}
                >
                    <ChevronRightIcon className="h-8 w-8 text-white drop-shadow-lg" />
                </button>

                {/* Games Container */}
                <div
                    ref={carouselRef}
                    className="flex gap-4 overflow-x-hidden px-4 pt-2 pb-4 lg:px-12"
                    style={{
                        scrollBehavior: 'smooth',
                    }}
                >
                    {duplicatedGames.map((game, index) => (
                        <div
                            key={`${game.id}-${index}`}
                            className="flex-none transition-all duration-300"
                            style={{
                                width: `calc((100% - ${(visibleCount - 1) * 16}px) / ${visibleCount})`,
                                minWidth: '160px',
                                maxWidth: '240px',
                            }}
                        >
                            <GameCard game={game} />
                        </div>
                    ))}
                </div>
            </div>
        </div>
    );
}
