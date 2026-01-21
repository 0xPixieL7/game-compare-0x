import { useState, useEffect, useRef, useMemo } from 'react';
import { Link } from '@inertiajs/react';
import { GameCard } from '@/components/GameCard';
import { AppleTvCard } from '@/components/apple-tv-card';

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
}

interface SpotlightCarouselProps {
    spotlight: SpotlightProduct[];
    hero?: SpotlightProduct;
}

export function SpotlightCarousel({ spotlight = [], hero }: SpotlightCarouselProps) {
    if (!spotlight || spotlight.length === 0) {
        return <div className="p-8 text-center text-gray-500">Spotlight warming up...</div>;
    }

    const [currentIndex, setCurrentIndex] = useState(0);
    const [isPaused, setIsPaused] = useState(false);
    const autoplayRef = useRef<NodeJS.Timeout | null>(null);

    // If a hero is provided, we can either prepend it or treat it as a special case
    // For now, if hero is passed, we might want to ensure it's the first visible item?
    // Or arguably, if "hero" is distinct from "spotlight" list, we might just default currentItem to hero if set?
    
    // Let's assume we want to cycle through the spotlight list, but if a hero is provided, it might override 
    // or be the initial state. For simpler logic, let's just stick to the carousel list. 
    // The user asked for a "Spotlight hero", which implies the carousel ITSELF is the hero section.
    // So the `hero` prop passed from controller is just a specific Item to feature.
    
    // Effective list: if hero is passed, ensure it is in the list or is the list?
    // The controller passes `spotlight` (array) AND `hero` (single object).
    // Let's prepend hero to spotlight if it's not already there?
    const effectiveList = hero ? [hero, ...spotlight.filter(s => s.id !== hero.id)] : spotlight;

    const currentItem = effectiveList[currentIndex] || {};
    const totalItems = effectiveList.length;

    // Autoplay logic
    useEffect(() => {
        if (!isPaused) {
            autoplayRef.current = setInterval(() => {
                nextSlide();
            }, 8000); // 8 seconds, matching legacy
        }
        return () => {
            if (autoplayRef.current) clearInterval(autoplayRef.current);
        };
    }, [currentIndex, isPaused]);

    const nextSlide = () => {
        setCurrentIndex((prev) => (prev + 1) % totalItems);
    };

    const prevSlide = () => {
        setCurrentIndex((prev) => (prev - 1 + totalItems) % totalItems);
    };

    const goToSlide = (index: number) => {
        setCurrentIndex(index);
    };

    // Helper to format score
    const formatScore = (score: any) => {
        return score && score.total ? parseFloat(score.total).toFixed(1) : '—';
    };

    // Helper to build subtitle
    const buildSubtitle = (item: SpotlightProduct) => {
        const platforms = item.platform_labels && item.platform_labels.length
            ? item.platform_labels
            : (item.platform ? [item.platform] : []);
        const release = item.release_date || '';
        const regions = item.region_codes ? item.region_codes.length : 0;

        const parts = [];
        if (platforms.length) parts.push(platforms.map(p => p.toUpperCase()).join(' · '));
        if (release) parts.push(release);
        if (regions > 0) parts.push(`${regions} region${regions === 1 ? '' : 's'}`);

        return parts.join(' · ') || 'Spotlight metrics warming up';
    };

    const currentScore = currentItem.spotlight_score || {};
    // @ts-ignore - 'context' might be dynamic in Score object
    const currentContext = currentScore.context || {};
    const metrics = currentScore.breakdown || []; // 'breakdown' maps to 'metrics' in legacy logic?

    // Resolve media
    const gallery = currentItem.spotlight_gallery || [];
    const primaryMedia = gallery.find(m => m && m.type === 'image') || gallery[0] || {};
    const coverImage = primaryMedia.url || currentItem.image || '/images/placeholders/game-cover.svg';

    // Map currentItem to GameCard format
    const gameCardData = useMemo(() => ({
        id: currentItem.id,
        name: currentItem.name,
        media: {
            cover_url: currentItem.image,
            cover_url_high_res: coverImage,
        },
        rating: currentItem.spotlight_score?.total || 0,
    }), [currentItem, coverImage]);

    return (
        <div id="compareBackdrop" className="relative text-white col-span-12" onMouseEnter={() => setIsPaused(true)} onMouseLeave={() => setIsPaused(false)}>
            <div className="glass-panel compare-spotlight-shell">
                <div className="spotlight-grid">
                    {/* Left Column: Prime Info */}
                    <div className="spotlight-prime">
                        <div className="flex flex-col gap-2">
                            <span className="prime-badge">Spotlight Selection</span>
                            <h1 className="text-4xl font-bold leading-tight">{currentItem.name}</h1>
                            <div className="prime-metric-header">{buildSubtitle(currentItem)}</div>
                        </div>

                        <div className="prime-cover group">
                            <img src={coverImage} alt={`${currentItem.name} cover art`} className="w-full h-full object-cover transition-transform duration-500 group-hover:scale-105" />
                            <div className="prime-cover-caption">
                                {primaryMedia.source || 'MEDIA FEED'}
                            </div>
                        </div>

                        <div className="prime-score">
                            <div className="flex items-baseline gap-3">
                                <span className="prime-score-value">{formatScore(currentScore)}</span>
                                <span className="prime-score-grade">{currentScore.grade || '—'}</span>
                            </div>
                            <span className="prime-score-meta">{currentScore.verdict || 'No Verdict'}</span>
                        </div>

                        {/* Metrics */}
                        <div className="prime-metrics mt-4">
                            {metrics.map((metric: any, idx: number) => (
                                <div key={idx} className="prime-metric">
                                    <div className="prime-metric-header">
                                        <span>{metric.label}</span>
                                        <span className="prime-metric-weight">{metric.weight_percentage || metric.weight}%</span>
                                    </div>
                                    <div className="prime-metric-bar">
                                        <div
                                            className="prime-metric-fill"
                                            style={{ width: `${metric.score}%` }}
                                        ></div>
                                    </div>
                                    <div className="prime-metric-summary">
                                        {metric.summary}
                                    </div>
                                </div>
                            ))}
                        </div>
                    </div>

                    {/* Right Column: Carousel / Apple TV Card */}
                    <div className="apple-tv-carousel">
                        <div className="carousel-header">
                            <span>Featured Comparison</span>
                            <div className="carousel-controls">
                                <button onClick={prevSlide} className="carousel-btn">
                                    <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" strokeWidth={1.5} stroke="currentColor" className="w-4 h-4">
                                        <path strokeLinecap="round" strokeLinejoin="round" d="M15.75 19.5L8.25 12l7.5-7.5" />
                                    </svg>
                                </button>
                                <button onClick={nextSlide} className="carousel-btn">
                                    <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" strokeWidth={1.5} stroke="currentColor" className="w-4 h-4">
                                        <path strokeLinecap="round" strokeLinejoin="round" d="M8.25 4.5l7.5 7.5-7.5 7.5" />
                                    </svg>
                                </button>
                            </div>
                        </div>

                        {/* The Large Card (Apple Card style) */}
                        <div className="flex-1 relative">
                            <div className="h-full min-h-[500px]">
                                <GameCard 
                                    game={gameCardData as any} 
                                    className="!max-w-full h-full"
                                />
                            </div>
                        </div>

                        {/* Dots */}
                        <div className="carousel-dots">
                            {effectiveList.map((_, idx) => (
                                <div
                                    key={idx}
                                    onClick={() => goToSlide(idx)}
                                    className={`carousel-dot ${idx === currentIndex ? 'active' : ''}`}
                                ></div>
                            ))}
                        </div>
                    </div>
                </div>
            </div>
        </div>
    );
}
