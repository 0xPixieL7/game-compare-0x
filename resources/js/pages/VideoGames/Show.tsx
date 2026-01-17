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

            <div className="py-12">
                <div className="mx-auto max-w-7xl sm:px-6 lg:px-8">
                    <div className="overflow-hidden bg-white shadow-sm sm:rounded-lg dark:bg-gray-800">
                        <div className="grid grid-cols-1 gap-8 p-6 md:grid-cols-3">
                            {/* Left Column: Cover & Key Info */}
                            <div className="col-span-1">
                                <div className="mb-4 flex aspect-[3/4] items-center justify-center rounded-lg bg-gray-200 dark:bg-gray-700 overflow-hidden">
                                    {media.images.cover_url ? (
                                        <img
                                            src={media.images.cover_url}
                                            alt={game.name}
                                            className="h-full w-full object-cover"
                                        />
                                    ) : (
                                        <span className="text-gray-500">
                                            Cover Image
                                        </span>
                                    )}
                                </div>

                                <div className="space-y-4">
                                    <div className="rounded-lg bg-gray-100 p-4 dark:bg-gray-700">
                                        <h3 className="mb-2 font-bold">
                                            Ratings
                                        </h3>
                                        <div className="flex justify-between">
                                            <span>Hype Score</span>
                                            <span className="font-bold">
                                                {game.hypes
                                                    ? game.hypes.toLocaleString()
                                                    : 'N/A'}
                                            </span>
                                        </div>
                                    </div>
                                </div>
                            </div>

                            {/* Middle/Right: Details & Prices */}
                            <div className="col-span-1 md:col-span-2">
                                <h1 className="mb-4 text-4xl font-bold text-gray-900 dark:text-white">
                                    {game.name}
                                </h1>

                                <div className="prose dark:prose-invert mb-8">
                                    <p>
                                        {game.attributes?.summary ||
                                            'No summary available.'}
                                    </p>
                                </div>

                                <h2 className="mb-4 text-2xl font-bold">
                                    Retailer Prices
                                </h2>

                                <PriceTable prices={prices} />
                                <PriceHistoryChart />
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        </AppLayout>
    );
}
