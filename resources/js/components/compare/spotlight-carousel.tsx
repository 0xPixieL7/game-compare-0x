import { useState, useEffect, useRef } from 'react';
import { Link } from '@inertiajs/react';

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
}

export function SpotlightCarousel({ spotlight = [] }: SpotlightCarouselProps) {
    if (!spotlight || spotlight.length === 0) {
        return <div className="p-8 text-center text-gray-500">Spotlight warming up...</div>;
    }

    const [currentIndex, setCurrentIndex] = useState(0);
    const [isPaused, setIsPaused] = useState(false);
    const autoplayRef = useRef<NodeJS.Timeout | null>(null);

    const currentItem = spotlight[currentIndex] || {};
    const totalItems = spotlight.length;

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

    // Resolve cover image
    const gallery = currentItem.spotlight_gallery || [];
    const primaryMedia = gallery.find(m => m && m.type === 'image') || gallery[0] || {};
    const coverImage = primaryMedia.url || currentItem.image || '/images/placeholders/game-cover.svg';

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

                        {/* The Large Card */}
                        <div className="flex-1 relative perspective-1000">
                            <div className="apple-tv-card group">
                                <div className="apple-tv-media">
                                    {/* Placeholder for video/large image - reusing cover for now or random gallery image */}
                                    <img
                                        src={coverImage}
                                        alt="Presentation"
                                        className="w-full h-full object-cover"
                                    />
                                    <div className="video-overlay">
                                        <div className="play-pause-btn">
                                            <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" strokeWidth={1.5} stroke="currentColor" className="w-8 h-8">
                                                <path strokeLinecap="round" strokeLinejoin="round" d="M5.25 5.653c0-.856.917-1.398 1.667-.986l11.54 6.348a1.125 1.125 0 010 1.971l-11.54 6.347a1.125 1.125 0 01-1.667-.985V5.653z" />
                                            </svg>
                                        </div>
                                    </div>
                                </div>
                                <div className="apple-tv-overlay">
                                    <div className="apple-tv-content">
                                        <div className="apple-tv-badges">
                                            {currentContext.media_count > 0 && (
                                                <span className="apple-tv-badge">{currentContext.media_count} Assets</span>
                                            )}
                                            {currentContext.retailer_count > 0 && (
                                                <span className="apple-tv-badge">{currentContext.retailer_count} Retailers</span>
                                            )}
                                        </div>
                                        <h2 className="apple-tv-title">{currentItem.name}</h2>
                                        <p className="apple-tv-subtitle">
                                            Compare prices across {currentItem.region_codes?.length || 0} regions and {currentContext.retailer_count || 0} stores.
                                        </p>
                                        <Link href={`/games/${currentItem.id}`} className="apple-tv-cta">
                                            View Analysis
                                            <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" strokeWidth={1.5} stroke="currentColor" className="w-4 h-4">
                                                <path strokeLinecap="round" strokeLinejoin="round" d="M13.5 4.5L21 12m0 0l-7.5 7.5M21 12H3" />
                                            </svg>
                                        </Link>
                                    </div>
                                    <div className="apple-tv-source">
                                        {primaryMedia.source || 'LIVE'}
                                    </div>
                                </div>
                            </div>
                        </div>

                        {/* Dots */}
                        <div className="carousel-dots">
                            {spotlight.map((_, idx) => (
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
