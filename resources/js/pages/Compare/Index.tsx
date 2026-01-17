import { Head } from '@inertiajs/react';
import AppLayout from '@/layouts/app-layout';
import { compare } from '@/routes';
import { type BreadcrumbItem } from '@/types';
import { useState } from 'react';
import { SpotlightCarousel } from '@/components/compare/spotlight-carousel';

// Types for compare page data
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

interface SpotlightProduct {
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
}

interface CrossReferenceStats {
    total: number;
    digital: number;
    physical: number;
    both: number;
    generated_at: string | null;
    displayed: number;
    display_limit: number | null;
}

interface DigitalOffer {
    region: string;
    currency: string;
    amount: number;
    btc_value: number;
    retailer: string;
    url?: string | null;
}

interface CurrencyPrice {
    code: string;
    symbol: string;
    amount: number;
    formatted: string;
}

interface Video {
    url: string;
    provider: string;
    type: string;
    title?: string | null;
    thumbnail_url?: string | null;
}

interface PrioritizedMatch {
    product_id: number;
    product_slug: string;
    title_id?: number;
    name: string;
    normalized_title?: string;
    image: string;
    videos?: Video[];
    has_videos?: boolean;
    has_digital: boolean;
    has_physical: boolean;
    platforms: string[];
    currencies: string[];
    digital: {
        best: DigitalOffer | null;
        offers: DigitalOffer[];
        currencies: CurrencyPrice[];
    };
    physical: any[];
    best_digital: DigitalOffer | null;
    best_physical: any | null;
    rating?: number | null;
    normalized_key: string;
    updated_at: string;
}

interface ComparePageProps {
    spotlight: SpotlightProduct[];
    crossReferenceStats: CrossReferenceStats;
    prioritizedMatches: PrioritizedMatch[];
    crossReferencePlatforms: string[];
    crossReferenceCurrencies: string[];
    regionOptions: string[];
    apiEndpoints: {
        stats: string;
        entries: string;
        spotlight: string;
    };
}

const breadcrumbs: BreadcrumbItem[] = [
    {
        title: 'Compare',
        href: compare().url,
    },
];

export default function Compare({
    spotlight,
    crossReferenceStats,
    prioritizedMatches,
    crossReferencePlatforms,
    crossReferenceCurrencies,
    regionOptions,
}: ComparePageProps) {
    const [currentSlideIndex, setCurrentSlideIndex] = useState(0);
    const [showVideo, setShowVideo] = useState(true);
    const [searchQuery, setSearchQuery] = useState('');
    const [selectedPlatform, setSelectedPlatform] = useState('');
    const [selectedCurrency, setSelectedCurrency] = useState('');
    const [availabilityFilter, setAvailabilityFilter] = useState('all');

    // Filter matches based on search and filters
    const filteredMatches = prioritizedMatches.filter((match) => {
        // Search filter
        if (searchQuery) {
            const query = searchQuery.toLowerCase();
            const matchesSearch =
                match.name.toLowerCase().includes(query) ||
                match.product_slug.toLowerCase().includes(query) ||
                match.platforms.some(p => p.toLowerCase().includes(query));

            if (!matchesSearch) return false;
        }

        // Platform filter
        if (selectedPlatform && !match.platforms.includes(selectedPlatform)) {
            return false;
        }

        // Currency filter
        if (selectedCurrency && !match.currencies.includes(selectedCurrency)) {
            return false;
        }

        // Availability filter
        if (availabilityFilter !== 'all') {
            if (availabilityFilter === 'digital' && !match.has_digital) return false;
            if (availabilityFilter === 'physical' && !match.has_physical) return false;
            if (availabilityFilter === 'both' && !(match.has_digital && match.has_physical)) return false;
        }

        return true;
    });

    const currentSpotlight = spotlight[currentSlideIndex];

    const nextSlide = () => {
        setCurrentSlideIndex((prev) => (prev + 1) % spotlight.length);
    };

    const prevSlide = () => {
        setCurrentSlideIndex((prev) => (prev - 1 + spotlight.length) % spotlight.length);
    };

    const goToSlide = (index: number) => {
        setCurrentSlideIndex(index);
    };

    return (
        <AppLayout breadcrumbs={breadcrumbs}>
            <Head title="Compare Games & Prices" />

            {/* Custom background for compare page */}
            <style>{`
                html {
                    background: radial-gradient(circle at 12% 18%, rgba(88, 134, 255, 0.26), transparent 56%),
                        radial-gradient(circle at 82% 22%, rgba(234, 84, 255, 0.22), transparent 62%),
                        linear-gradient(180deg, rgb(6, 8, 14) 0%, rgb(3, 6, 12) 60%, rgb(2, 6, 23) 100%);
                    background-attachment: fixed;
                }
                html.dark {
                    background: radial-gradient(circle at 12% 18%, rgba(88, 134, 255, 0.26), transparent 56%),
                        radial-gradient(circle at 82% 22%, rgba(234, 84, 255, 0.22), transparent 62%),
                        linear-gradient(180deg, rgb(6, 8, 14) 0%, rgb(3, 6, 12) 60%, rgb(2, 6, 23) 100%);
                    background-attachment: fixed;
                }
            `}</style>

            <div className="mx-auto w-full max-w-[1600px] px-4 py-10 sm:px-6 lg:px-8">
                {/* Glass panel container */}
                <div className="rounded-3xl border border-white/10 bg-white/5 p-8 shadow-[0_24px_80px_rgba(2,6,23,0.55)] backdrop-blur-[22px] sm:p-10">

                    {/* Header */}
                    <div className="mb-6 flex items-end justify-between">
                        <div className="space-y-1">
                            <h1 className="sr-only">Compare</h1>
                            <div className="text-xs uppercase tracking-[0.34em] text-white/60">
                                Catalogue cross-reference
                            </div>
                            <div className="text-sm text-white/80">
                                Giant Bomb ↔ Nexarda ↔ Price Guide
                            </div>
                        </div>
                        {crossReferenceStats && (
                            <div className="text-right">
                                <div className="text-xs uppercase tracking-[0.34em] text-white/60">
                                    Matched titles
                                </div>
                                <div className="text-2xl font-semibold tracking-tight text-white">
                                    {crossReferenceStats.total.toLocaleString()}
                                </div>
                            </div>
                        )}
                    </div>

                    {/* Main content grid */}
                    <div className="grid grid-cols-12 gap-8">
                        {/* Spotlight carousel */}
                        <SpotlightCarousel spotlight={spotlight} />

                        {/* Stats section */}
                        <div className="col-span-12">
                            <div className="grid gap-6 sm:grid-cols-2 lg:grid-cols-4">
                                <div className="rounded-lg border border-white/10 bg-white/5 p-6">
                                    <div className="text-2xl text-white">{crossReferenceStats.total.toLocaleString()}</div>
                                    <div className="text-sm text-white/60">Matched titles</div>
                                </div>
                                <div className="rounded-lg border border-white/10 bg-white/5 p-6">
                                    <div className="text-2xl text-white">{crossReferenceStats.digital.toLocaleString()}</div>
                                    <div className="text-sm text-white/60">Digital ready</div>
                                </div>
                                <div className="rounded-lg border border-white/10 bg-white/5 p-6">
                                    <div className="text-2xl text-white">{crossReferenceStats.physical.toLocaleString()}</div>
                                    <div className="text-sm text-white/60">Physical ready</div>
                                </div>
                                <div className="rounded-lg border border-white/10 bg-white/5 p-6">
                                    <div className="text-2xl text-white">{crossReferenceStats.both.toLocaleString()}</div>
                                    <div className="text-sm text-white/60">Dual coverage</div>
                                </div>
                            </div>
                        </div>

                        {/* Explorer section - table with filters */}
                        <div className="col-span-12 space-y-8">
                            {/* Search and filters */}
                            <div className="space-y-6 rounded-lg border border-white/10 bg-white/5 p-6">
                                <div className="flex flex-col gap-6 xl:flex-row xl:items-end xl:justify-between">
                                    <div className="w-full xl:max-w-2xl">
                                        <input
                                            type="search"
                                            value={searchQuery}
                                            onChange={(e) => setSearchQuery(e.target.value)}
                                            className="w-full rounded-lg border border-white/20 bg-white/10 px-4 py-3 pl-10 text-white placeholder-white/40 focus:border-blue-400 focus:outline-none focus:ring-2 focus:ring-blue-400/50"
                                            placeholder="Search by title, slug, console, or retailer"
                                        />
                                    </div>
                                    <div className="flex flex-wrap items-start gap-4">
                                        <select
                                            value={selectedPlatform}
                                            onChange={(e) => setSelectedPlatform(e.target.value)}
                                            className="rounded-lg border border-white/20 bg-white/10 px-4 py-2 text-white focus:border-blue-400 focus:outline-none focus:ring-2 focus:ring-blue-400/50"
                                        >
                                            <option value="">All Platforms</option>
                                            {crossReferencePlatforms.map((platform) => (
                                                <option key={platform} value={platform}>
                                                    {platform}
                                                </option>
                                            ))}
                                        </select>
                                        <select
                                            value={selectedCurrency}
                                            onChange={(e) => setSelectedCurrency(e.target.value)}
                                            className="rounded-lg border border-white/20 bg-white/10 px-4 py-2 text-white focus:border-blue-400 focus:outline-none focus:ring-2 focus:ring-blue-400/50"
                                        >
                                            <option value="">All Currencies</option>
                                            {crossReferenceCurrencies.map((currency) => (
                                                <option key={currency} value={currency}>
                                                    {currency}
                                                </option>
                                            ))}
                                        </select>
                                        <div className="flex gap-2">
                                            {['all', 'digital', 'physical', 'both'].map((filter) => (
                                                <button
                                                    key={filter}
                                                    onClick={() => setAvailabilityFilter(filter)}
                                                    className={`rounded-lg px-4 py-2 text-sm font-medium transition-colors ${
                                                        availabilityFilter === filter
                                                            ? 'bg-blue-600 text-white'
                                                            : 'border border-white/20 bg-white/5 text-white/80 hover:bg-white/10'
                                                    }`}
                                                >
                                                    {filter.charAt(0).toUpperCase() + filter.slice(1)}
                                                </button>
                                            ))}
                                        </div>
                                    </div>
                                </div>
                            </div>

                            {/* Results table */}
                            <div className="overflow-hidden rounded-lg border border-white/10 bg-white/5">
                                <div className="overflow-x-auto">
                                    <table className="w-full">
                                        <thead className="border-b border-white/10 bg-white/5">
                                            <tr>
                                                <th className="px-6 py-4 text-left text-xs font-semibold uppercase tracking-wide text-white/80">
                                                    Title
                                                </th>
                                                <th className="px-6 py-4 text-left text-xs font-semibold uppercase tracking-wide text-white/80">
                                                    Digital offers
                                                </th>
                                                <th className="px-6 py-4 text-left text-xs font-semibold uppercase tracking-wide text-white/80">
                                                    Physical guide
                                                </th>
                                            </tr>
                                        </thead>
                                        <tbody className="divide-y divide-white/5">
                                            {filteredMatches.length === 0 ? (
                                                <tr>
                                                    <td colSpan={3} className="px-6 py-8 text-center text-white/60">
                                                        No results match the current filters. Try widening your search.
                                                    </td>
                                                </tr>
                                            ) : (
                                                filteredMatches.map((match) => (
                                                    <tr key={match.normalized_key} className="hover:bg-white/5">
                                                        <td className="px-6 py-4">
                                                            <div className="flex items-start gap-4">
                                                                <div className="hidden sm:block">
                                                                    <img
                                                                        src={match.image}
                                                                        alt={match.name}
                                                                        className="h-20 w-20 rounded-lg object-cover shadow-md"
                                                                    />
                                                                </div>
                                                                <div className="space-y-1">
                                                                    <div className="text-lg font-semibold text-white">
                                                                        {match.name}
                                                                    </div>
                                                                    <div className="flex flex-wrap gap-2 text-xs">
                                                                        {match.platforms.map((platform) => (
                                                                            <span
                                                                                key={platform}
                                                                                className="rounded bg-white/10 px-2 py-1 text-white/70"
                                                                            >
                                                                                {platform}
                                                                            </span>
                                                                        ))}
                                                                    </div>
                                                                    <div className="flex flex-wrap gap-2 text-xs text-white/60">
                                                                        {match.has_digital && (
                                                                            <span className="rounded border border-green-400/50 px-2 py-0.5 text-green-300">
                                                                                Digital
                                                                            </span>
                                                                        )}
                                                                        {match.has_physical && (
                                                                            <span className="rounded border border-blue-400/50 px-2 py-0.5 text-blue-300">
                                                                                Physical
                                                                            </span>
                                                                        )}
                                                                    </div>
                                                                </div>
                                                            </div>
                                                        </td>
                                                        <td className="px-6 py-4">
                                                            {match.has_digital && match.digital ? (
                                                                <div className="space-y-2">
                                                                    <div className="flex flex-wrap gap-2">
                                                                        {match.digital.currencies.map((currency, idx) => (
                                                                            <span
                                                                                key={idx}
                                                                                className="rounded border border-white/20 bg-white/5 px-3 py-1 text-sm text-white"
                                                                            >
                                                                                {currency.code} · {currency.formatted}
                                                                            </span>
                                                                        ))}
                                                                    </div>
                                                                </div>
                                                            ) : (
                                                                <span className="text-sm text-white/40">No digital pricing yet</span>
                                                            )}
                                                        </td>
                                                        <td className="px-6 py-4">
                                                            {match.has_physical && match.physical.length > 0 ? (
                                                                <div className="flex flex-wrap gap-2">
                                                                    {match.physical.map((physical: any, idx: number) => (
                                                                        <span
                                                                            key={idx}
                                                                            className="rounded border border-white/20 bg-white/5 px-3 py-1 text-sm text-white"
                                                                        >
                                                                            {physical.console} · {physical.formatted_price}
                                                                        </span>
                                                                    ))}
                                                                </div>
                                                            ) : (
                                                                <span className="text-sm text-white/40">No physical pricing yet</span>
                                                            )}
                                                        </td>
                                                    </tr>
                                                ))
                                            )}
                                        </tbody>
                                    </table>
                                </div>
                                <div className="border-t border-white/10 px-6 py-4 text-sm text-white/70">
                                    Showing {filteredMatches.length} of {prioritizedMatches.length} matched titles
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        </AppLayout>
    );
}
