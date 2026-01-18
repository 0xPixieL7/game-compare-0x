import PriceHistoryChart from '@/components/PriceHistoryChart';
import PriceTable from '@/components/PriceTable';
import AppLayout from '@/layouts/app-layout';
import { GameMediaSummary, GameModel, GamePrice } from '@/types';
import { Head } from '@inertiajs/react';

export default function Show({
    game,
    prices,
    media,
}: {
    game: GameModel;
    prices: GamePrice[];
    media: GameMediaSummary;
}) {
    return (
        <AppLayout
            breadcrumbs={[
                { title: 'Library', href: '/games' },
                { title: game.name, href: `/games/${game.id}` },
            ]}
        >
            <Head title={game.name} />

            <div className="relative min-h-screen overflow-hidden">
                {/* Fixed Cinematic Background */}
                {media.images.hero_url && (
                    <div 
                        className="fixed inset-0 z-0 bg-cover bg-center bg-no-repeat"
                        style={{ 
                            backgroundImage: `url(${media.images.hero_url})`,
                            backgroundAttachment: 'fixed'
                        }}
                    >
                        {/* Dark overlay for readability without blurring */}
                        <div className="absolute inset-0 bg-black/50" />
                        <div className="absolute inset-0 bg-gradient-to-t from-black via-transparent to-black/30" />
                    </div>
                )}

                <div className="relative z-10 py-12">
                    <div className="mx-auto max-w-7xl px-4 sm:px-6 lg:px-8">
                        {/* Main Content Card - Semi-transparent glassmorphism */}
                        <div className="overflow-hidden rounded-2xl bg-black/60 shadow-2xl border border-white/10">
                            <div className="grid grid-cols-1 gap-8 p-6 md:grid-cols-3 lg:p-10">
                                {/* Left Column: Cover & Key Info */}
                                <div className="col-span-1">
                                    <div className="group relative mb-6 aspect-[3/4] transform overflow-hidden rounded-xl shadow-2xl transition-transform duration-500 hover:scale-[1.02]">
                                        {media.images.cover_url ? (
                                            <img
                                                src={media.images.cover_url}
                                                alt={game.name}
                                                className="h-full w-full object-cover"
                                            />
                                        ) : (
                                            <div className="flex h-full w-full items-center justify-center bg-gray-800">
                                                <span className="text-gray-400">Cover Image</span>
                                            </div>
                                        )}
                                        <div className="absolute inset-0 bg-gradient-to-t from-black/60 to-transparent opacity-0 transition-opacity duration-300 group-hover:opacity-100" />
                                    </div>

                                    <div className="space-y-4">
                                        <div className="rounded-xl bg-white/5 p-5 border border-white/5">
                                            <h3 className="mb-3 text-sm font-bold uppercase tracking-wider text-blue-400">
                                                Ratings
                                            </h3>
                                            <div className="flex items-center justify-between">
                                                <span className="text-gray-300 font-medium">Hype Score</span>
                                                <div className="flex items-baseline gap-1">
                                                    <span className="text-2xl font-black text-white">
                                                        {game.hypes ? game.hypes.toLocaleString() : 'N/A'}
                                                    </span>
                                                </div>
                                            </div>
                                            
                                            {game.rating && (
                                                <div className="mt-4 flex items-center justify-between">
                                                    <span className="text-gray-300 font-medium">IGDB Score</span>
                                                    <span className="rounded bg-green-500/20 px-2 py-1 text-sm font-bold text-green-400 border border-green-500/30">
                                                        {Math.round(game.rating)}%
                                                    </span>
                                                </div>
                                            )}
                                        </div>
                                    </div>
                                </div>

                                {/* Middle/Right: Details & Prices */}
                                <div className="col-span-1 md:col-span-2">
                                    <div className="mb-6">
                                        <h1 className="text-5xl font-black tracking-tight text-white md:text-6xl">
                                            {game.name}
                                        </h1>
                                        {game.release_date && (
                                            <p className="mt-2 text-lg font-medium text-gray-400">
                                                Released: {new Date(game.release_date).toLocaleDateString(undefined, { year: 'numeric', month: 'long', day: 'numeric' })}
                                            </p>
                                        )}
                                    </div>

                                    <div className="mb-10 max-w-2xl">
                                        <div className="prose prose-invert prose-lg">
                                            <p className="leading-relaxed text-gray-200">
                                                {game.attributes?.summary || 'No summary available.'}
                                            </p>
                                        </div>
                                    </div>

                                    <div className="space-y-6">
                                        <h2 className="flex items-center gap-2 text-2xl font-bold text-white">
                                            <span className="h-8 w-1 bg-blue-500 rounded-full" />
                                            Retailer Prices
                                        </h2>

                                        <div className="rounded-xl bg-black/20 p-2 border border-white/5">
                                            <PriceTable prices={prices} />
                                        </div>
                                        
                                        <div className="mt-8 rounded-xl bg-black/20 p-6 border border-white/5">
                                            <h3 className="mb-4 text-xl font-bold text-white">Price History</h3>
                                            <PriceHistoryChart />
                                        </div>
                                    </div>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        </AppLayout>
    );
}
