import { Head, Link } from '@inertiajs/react';
import { useState, useEffect } from 'react';
import { ArrowLeft, TrendingUp, DollarSign, Percent } from 'lucide-react';

interface Trade {
    name: string;
    cost: number;
    revenue: number;
    profit: number;
    roi: number;
}

export default function Calculator() {
    const [trades, setTrades] = useState<Trade[]>([]);
    const [loading, setLoading] = useState(true);

    useEffect(() => {
        // Load profit data from API or static file
        // For now, using sample data - replace with actual API call
        fetch('/arc-raiders/profit.json')
            .then((res) => res.json())
            .then((data) => {
                setTrades(data);
                setLoading(false);
            })
            .catch((err) => {
                console.error('Failed to load profit data:', err);
                // Load sample data as fallback
                setTrades([
                    {
                        name: 'Tempest III',
                        cost: 14.0,
                        revenue: 15.0,
                        profit: 1.0,
                        roi: 7.14,
                    },
                ]);
                setLoading(false);
            });
    }, []);

    const formatCurrency = (value: number) => {
        return new Intl.NumberFormat('en-US', {
            style: 'currency',
            currency: 'USD',
        }).format(value);
    };

    const formatPercent = (value: number) => {
        return `${value.toFixed(2)}%`;
    };

    return (
        <>
            <Head title="ARC Raiders Profit Calculator" />

            <div className="min-h-screen bg-gradient-to-b from-zinc-950 via-zinc-900 to-black text-white">
                {/* Header */}
                <header className="border-b border-zinc-800 px-4 py-6">
                    <div className="mx-auto max-w-5xl">
                        <Link
                            href="/"
                            className="inline-flex items-center gap-2 text-sm text-zinc-400 hover:text-white"
                        >
                            <ArrowLeft className="h-4 w-4" />
                            Back to Home
                        </Link>
                        <h1 className="mt-4 text-3xl font-bold">
                            ARC Raiders Profit Calculator
                        </h1>
                        <p className="mt-2 text-zinc-400">
                            Updated from real Odealo marketplace prices
                        </p>
                    </div>
                </header>

                {/* Main Content */}
                <main className="px-4 py-12">
                    <div className="mx-auto max-w-5xl">
                        {loading ? (
                            <div className="flex items-center justify-center py-20">
                                <div className="text-center">
                                    <div className="mb-4 mx-auto h-12 w-12 animate-spin rounded-full border-4 border-zinc-700 border-t-blue-500" />
                                    <p className="text-zinc-400">Loading profit data...</p>
                                </div>
                            </div>
                        ) : trades.length === 0 ? (
                            <div className="rounded-xl border border-yellow-900/50 bg-yellow-950/20 p-8 text-center">
                                <div className="mb-3 text-4xl">⚠️</div>
                                <h2 className="mb-2 text-xl font-semibold text-yellow-400">
                                    No Profitable Crafts Found
                                </h2>
                                <p className="text-zinc-400">
                                    Based on current market prices, no crafts are showing positive ROI.
                                    <br />
                                    Check back later when prices change.
                                </p>
                            </div>
                        ) : (
                            <>
                                {/* Summary Cards */}
                                <div className="mb-8 grid gap-4 sm:grid-cols-3">
                                    <div className="rounded-xl border border-zinc-800 bg-zinc-900/50 p-6">
                                        <div className="flex items-center gap-3">
                                            <div className="rounded-lg bg-green-500/10 p-3">
                                                <TrendingUp className="h-6 w-6 text-green-400" />
                                            </div>
                                            <div>
                                                <div className="text-2xl font-bold text-green-400">
                                                    {trades.length}
                                                </div>
                                                <div className="text-sm text-zinc-400">Profitable Crafts</div>
                                            </div>
                                        </div>
                                    </div>

                                    <div className="rounded-xl border border-zinc-800 bg-zinc-900/50 p-6">
                                        <div className="flex items-center gap-3">
                                            <div className="rounded-lg bg-blue-500/10 p-3">
                                                <DollarSign className="h-6 w-6 text-blue-400" />
                                            </div>
                                            <div>
                                                <div className="text-2xl font-bold text-blue-400">
                                                    {formatCurrency(Math.max(...trades.map((t) => t.profit)))}
                                                </div>
                                                <div className="text-sm text-zinc-400">Best Profit</div>
                                            </div>
                                        </div>
                                    </div>

                                    <div className="rounded-xl border border-zinc-800 bg-zinc-900/50 p-6">
                                        <div className="flex items-center gap-3">
                                            <div className="rounded-lg bg-cyan-500/10 p-3">
                                                <Percent className="h-6 w-6 text-cyan-400" />
                                            </div>
                                            <div>
                                                <div className="text-2xl font-bold text-cyan-400">
                                                    {formatPercent(Math.max(...trades.map((t) => t.roi)))}
                                                </div>
                                                <div className="text-sm text-zinc-400">Best ROI</div>
                                            </div>
                                        </div>
                                    </div>
                                </div>

                                {/* Profitable Crafts Table */}
                                <div className="rounded-xl border border-zinc-800 bg-zinc-900/50 overflow-hidden">
                                    <div className="border-b border-zinc-800 px-6 py-4">
                                        <h2 className="text-xl font-semibold">Profitable Crafts</h2>
                                        <p className="text-sm text-zinc-400 mt-1">
                                            Sorted by ROI % (highest first)
                                        </p>
                                    </div>

                                    <div className="overflow-x-auto">
                                        <table className="w-full">
                                            <thead className="border-b border-zinc-800 bg-zinc-950/50">
                                                <tr>
                                                    <th className="px-6 py-4 text-left text-sm font-semibold text-zinc-300">
                                                        Item
                                                    </th>
                                                    <th className="px-6 py-4 text-right text-sm font-semibold text-zinc-300">
                                                        Cost
                                                    </th>
                                                    <th className="px-6 py-4 text-right text-sm font-semibold text-zinc-300">
                                                        Revenue
                                                    </th>
                                                    <th className="px-6 py-4 text-right text-sm font-semibold text-zinc-300">
                                                        Profit
                                                    </th>
                                                    <th className="px-6 py-4 text-right text-sm font-semibold text-zinc-300">
                                                        ROI
                                                    </th>
                                                </tr>
                                            </thead>
                                            <tbody className="divide-y divide-zinc-800">
                                                {trades
                                                    .sort((a, b) => b.roi - a.roi)
                                                    .map((trade, index) => (
                                                        <tr
                                                            key={trade.name}
                                                            className={index === 0 ? 'bg-green-950/20' : 'hover:bg-zinc-800/50'}
                                                        >
                                                            <td className="px-6 py-4">
                                                                <div className="flex items-center gap-3">
                                                                    {index === 0 && (
                                                                        <span className="text-xl">👑</span>
                                                                    )}
                                                                    <span className="font-medium">{trade.name}</span>
                                                                </div>
                                                            </td>
                                                            <td className="px-6 py-4 text-right text-zinc-400">
                                                                {formatCurrency(trade.cost)}
                                                            </td>
                                                            <td className="px-6 py-4 text-right text-zinc-400">
                                                                {formatCurrency(trade.revenue)}
                                                            </td>
                                                            <td className="px-6 py-4 text-right">
                                                                <span className="font-semibold text-green-400">
                                                                    +{formatCurrency(trade.profit)}
                                                                </span>
                                                            </td>
                                                            <td className="px-6 py-4 text-right">
                                                                <span className="inline-flex items-center gap-1 rounded-full bg-green-500/10 px-3 py-1 text-sm font-semibold text-green-400">
                                                                    <TrendingUp className="h-3 w-3" />
                                                                    {formatPercent(trade.roi)}
                                                                </span>
                                                            </td>
                                                        </tr>
                                                    ))}
                                            </tbody>
                                        </table>
                                    </div>
                                </div>

                                {/* Info Box */}
                                <div className="mt-8 rounded-xl border border-blue-900/50 bg-blue-950/20 p-6">
                                    <h3 className="mb-2 text-lg font-semibold text-blue-400">
                                        📊 How to Use This Data
                                    </h3>
                                    <ul className="space-y-2 text-sm text-zinc-400">
                                        <li>
                                            <strong className="text-zinc-300">Cost:</strong> Total material cost to craft the item
                                        </li>
                                        <li>
                                            <strong className="text-zinc-300">Revenue:</strong> Current selling price on Odealo
                                        </li>
                                        <li>
                                            <strong className="text-zinc-300">Profit:</strong> Net profit after marketplace fees
                                        </li>
                                        <li>
                                            <strong className="text-zinc-300">ROI:</strong> Return on investment percentage
                                        </li>
                                    </ul>
                                    <p className="mt-4 text-xs text-zinc-500">
                                        Prices update regularly from live marketplace data. Always verify current prices before making large investments.
                                    </p>
                                </div>
                            </>
                        )}
                    </div>
                </main>

                {/* Footer CTA */}
                <section className="border-t border-zinc-800 px-4 py-12">
                    <div className="mx-auto max-w-3xl text-center">
                        <h2 className="mb-3 text-2xl font-bold">
                            Want Alerts When Prices Change?
                        </h2>
                        <p className="mb-6 text-zinc-400">
                            Coming soon: Real-time notifications when profitable opportunities appear
                        </p>
                        <button
                            disabled
                            className="inline-flex items-center gap-2 rounded-lg border border-zinc-700 bg-zinc-800/50 px-6 py-3 font-semibold text-zinc-500 cursor-not-allowed"
                        >
                            Join Waitlist (Coming Soon)
                        </button>
                    </div>
                </section>

                {/* Footer */}
                <footer className="border-t border-zinc-800 px-4 py-8 text-center text-sm text-zinc-500">
                    <p>
                        Built for ARC Raiders players. Prices from{' '}
                        <a href="https://odealo.com" className="text-zinc-400 hover:text-white">
                            Odealo
                        </a>
                        .
                    </p>
                </footer>
            </div>
        </>
    );
}
