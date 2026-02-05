import Header from '@/components/Header';
import { Head, Link } from '@inertiajs/react';
import { Calculator, Clock, DollarSign, Shield, TrendingUp, Zap } from 'lucide-react';

export default function ArcRaiders() {
    return (
        <>
            <Head title="ARC Raiders Profit Calculator - Stop Wasting Materials">
                <meta name="description" content="Instantly calculate profit and ROI for every ARC Raiders craft. Know which items make money before you craft. Updated from real Odealo marketplace prices." />
            </Head>

            <div className="min-h-screen bg-gradient-to-b from-zinc-950 via-zinc-900 to-black text-white">
                <Header />

                {/* Hero Section */}
                <section className="relative overflow-hidden px-4 pt-32 pb-20 sm:px-6 lg:px-8">
                    <div className="absolute inset-0 bg-[radial-gradient(ellipse_at_top,_var(--tw-gradient-stops))] from-blue-900/20 via-transparent to-transparent" />
                    
                    <div className="relative mx-auto max-w-4xl text-center">
                        {/* Pill Badge */}
                        <div className="mb-6 inline-flex items-center gap-2 rounded-full bg-green-500/10 px-4 py-2 text-sm font-medium text-green-400 ring-1 ring-green-500/20">
                            <TrendingUp className="h-4 w-4" />
                            Updated from real Odealo prices
                        </div>

                        {/* Main Headline */}
                        <h1 className="mb-6 text-4xl font-bold tracking-tight sm:text-5xl lg:text-6xl">
                            Stop Wasting Materials on{' '}
                            <span className="bg-gradient-to-r from-blue-400 to-cyan-400 bg-clip-text text-transparent">
                                Unprofitable Crafts
                            </span>
                        </h1>

                        {/* Subheadline */}
                        <p className="mb-10 text-xl leading-relaxed text-zinc-400 sm:text-2xl">
                            Instantly calculate profit and ROI for every ARC Raiders craft.
                            <br className="hidden sm:block" />
                            Know which items make money <strong>before</strong> you craft.
                        </p>

                        {/* CTA Buttons */}
                        <div className="flex flex-col gap-4 sm:flex-row sm:justify-center">
                            <Link
                                href="/calculator"
                                className="group inline-flex items-center justify-center gap-2 rounded-lg bg-gradient-to-r from-blue-600 to-cyan-600 px-8 py-4 text-lg font-semibold text-white shadow-lg shadow-blue-500/25 transition-all hover:scale-105 hover:shadow-blue-500/40"
                            >
                                <Calculator className="h-5 w-5" />
                                Try Calculator Now
                                <span className="transition-transform group-hover:translate-x-1">→</span>
                            </Link>
                            
                            <a
                                href="#how-it-works"
                                className="inline-flex items-center justify-center gap-2 rounded-lg border border-zinc-700 px-8 py-4 text-lg font-semibold text-zinc-300 transition-colors hover:border-zinc-600 hover:bg-zinc-800/50"
                            >
                                How It Works
                            </a>
                        </div>

                        {/* Trust Signals */}
                        <div className="mt-10 flex flex-wrap items-center justify-center gap-6 text-sm text-zinc-500">
                            <div className="flex items-center gap-2">
                                <Shield className="h-4 w-4 text-green-500" />
                                No login required
                            </div>
                            <div className="flex items-center gap-2">
                                <Zap className="h-4 w-4 text-yellow-500" />
                                Instant results
                            </div>
                            <div className="flex items-center gap-2">
                                <Clock className="h-4 w-4 text-blue-500" />
                                Real-time prices
                            </div>
                        </div>
                    </div>
                </section>

                {/* Payment / Support Section (Task 2.0 Placeholder) */}
                <section className="bg-blue-600/5 px-4 py-16 border-y border-white/5">
                    <div className="mx-auto max-w-4xl text-center">
                        <h2 className="text-2xl font-bold mb-4">Support the Project</h2>
                        <p className="text-zinc-400 mb-8">Unlock premium features or buy our developers a coffee via Card or Crypto.</p>
                        <div className="flex flex-wrap justify-center gap-4">
                            <button className="px-6 py-3 rounded-xl bg-white text-black font-bold hover:bg-zinc-200 transition-all">Pay with Card</button>
                            <button className="px-6 py-3 rounded-xl bg-[#14F195]/10 border border-[#14F195]/20 text-[#14F195] font-bold hover:bg-[#14F195]/20 transition-all">Solana</button>
                            <button className="px-6 py-3 rounded-xl bg-blue-500/10 border border-blue-500/20 text-blue-400 font-bold hover:bg-blue-500/20 transition-all">EVM (Base/ETH)</button>
                        </div>
                    </div>
                </section>

                {/* Stats Bar */}
                <section className="border-b border-zinc-800 bg-zinc-900/50 px-4 py-8">
                    <div className="mx-auto grid max-w-5xl grid-cols-1 gap-8 sm:grid-cols-3">
                        <div className="text-center">
                            <div className="mb-2 text-4xl font-bold text-blue-400">26</div>
                            <div className="text-sm text-zinc-400">Recipes Tracked</div>
                        </div>
                        <div className="text-center">
                            <div className="mb-2 text-4xl font-bold text-green-400">60+</div>
                            <div className="text-sm text-zinc-400">Market Listings</div>
                        </div>
                        <div className="text-center">
                            <div className="mb-2 text-4xl font-bold text-cyan-400">100%</div>
                            <div className="text-sm text-zinc-400">Free Forever</div>
                        </div>
                    </div>
                </section>

                {/* The Problem Section */}
                <section className="px-4 py-20 sm:px-6 lg:px-8">
                    <div className="mx-auto max-w-4xl">
                        <div className="mb-12 text-center">
                            <h2 className="mb-4 text-3xl font-bold sm:text-4xl">
                                Most Players Are Losing Money
                            </h2>
                            <p className="text-lg text-zinc-400">
                                Without knowing the exact costs and market prices, you're gambling every time you craft.
                            </p>
                        </div>

                        <div className="grid gap-6 md:grid-cols-2">
                            <div className="rounded-xl border border-red-900/50 bg-red-950/20 p-6">
                                <div className="mb-3 text-3xl">❌</div>
                                <h3 className="mb-2 text-xl font-semibold text-red-400">Without Calculator</h3>
                                <ul className="space-y-2 text-zinc-400">
                                    <li>• Guess which crafts are profitable</li>
                                    <li>• Waste materials on bad trades</li>
                                    <li>• Lose money to marketplace fees</li>
                                    <li>• Miss hidden gem opportunities</li>
                                </ul>
                            </div>

                            <div className="rounded-xl border border-green-900/50 bg-green-950/20 p-6">
                                <div className="mb-3 text-3xl">✅</div>
                                <h3 className="mb-2 text-xl font-semibold text-green-400">With Calculator</h3>
                                <ul className="space-y-2 text-zinc-400">
                                    <li>• Know exact profit before crafting</li>
                                    <li>• See ROI % for every recipe</li>
                                    <li>• Avoid money-losing crafts</li>
                                    <li>• Maximize your material value</li>
                                </ul>
                            </div>
                        </div>
                    </div>
                </section>

                {/* How It Works Section */}
                <section id="how-it-works" className="bg-zinc-900/30 px-4 py-20 sm:px-6 lg:px-8">
                    <div className="mx-auto max-w-4xl">
                        <div className="mb-12 text-center">
                            <h2 className="mb-4 text-3xl font-bold sm:text-4xl">
                                How It Works
                            </h2>
                            <p className="text-lg text-zinc-400">
                                Three simple steps to never lose money on crafting again
                            </p>
                        </div>

                        <div className="space-y-8">
                            <div className="flex gap-6">
                                <div className="flex h-12 w-12 flex-shrink-0 items-center justify-center rounded-full bg-blue-600 text-xl font-bold">
                                    1
                                </div>
                                <div>
                                    <h3 className="mb-2 text-xl font-semibold">Real Market Prices</h3>
                                    <p className="text-zinc-400">
                                        We scrape live prices from Odealo marketplace for all crafting materials and finished items.
                                    </p>
                                </div>
                            </div>

                            <div className="flex gap-6">
                                <div className="flex h-12 w-12 flex-shrink-0 items-center justify-center rounded-full bg-cyan-600 text-xl font-bold">
                                    2
                                </div>
                                <div>
                                    <h3 className="mb-2 text-xl font-semibold">Calculate Everything</h3>
                                    <p className="text-zinc-400">
                                        Our solver analyzes all 26 recipes, computing material costs, marketplace fees, and final profit margins.
                                    </p>
                                </div>
                            </div>

                            <div className="flex gap-6">
                                <div className="flex h-12 w-12 flex-shrink-0 items-center justify-center rounded-full bg-green-600 text-xl font-bold">
                                    3
                                </div>
                                <div>
                                    <h3 className="mb-2 text-xl font-semibold">See Profitable Crafts</h3>
                                    <p className="text-zinc-400">
                                        Get instant results showing exact profit, ROI %, and which crafts are worth your materials.
                                    </p>
                                </div>
                            </div>
                        </div>

                        <div className="mt-12 text-center">
                            <Link
                                href="/calculator"
                                className="inline-flex items-center gap-2 rounded-lg bg-gradient-to-r from-blue-600 to-cyan-600 px-8 py-4 text-lg font-semibold text-white shadow-lg shadow-blue-500/25 transition-all hover:scale-105"
                            >
                                <DollarSign className="h-5 w-5" />
                                Start Calculating Profits
                            </Link>
                        </div>
                    </div>
                </section>

                {/* Social Proof Section */}
                <section className="px-4 py-20 sm:px-6 lg:px-8">
                    <div className="mx-auto max-w-4xl">
                        <div className="mb-12 text-center">
                            <h2 className="mb-4 text-3xl font-bold sm:text-4xl">
                                Built for ARC Raiders Players
                            </h2>
                            <p className="text-lg text-zinc-400">
                                No ads. No spam. Just math that saves you money.
                            </p>
                        </div>

                        <div className="grid gap-6 md:grid-cols-3">
                            <div className="rounded-xl border border-zinc-800 bg-zinc-900/50 p-6">
                                <div className="mb-3 text-3xl">⚡</div>
                                <h3 className="mb-2 text-lg font-semibold">Fast</h3>
                                <p className="text-sm text-zinc-400">
                                    Instant calculations. No loading screens, no waiting for server responses.
                                </p>
                            </div>

                            <div className="rounded-xl border border-zinc-800 bg-zinc-900/50 p-6">
                                <div className="mb-3 text-3xl">📱</div>
                                <h3 className="mb-2 text-lg font-semibold">Mobile-First</h3>
                                <p className="text-sm text-zinc-400">
                                    Perfect for checking on your phone while you're in-game.
                                </p>
                            </div>

                            <div className="rounded-xl border border-zinc-800 bg-zinc-900/50 p-6">
                                <div className="mb-3 text-3xl">🔒</div>
                                <h3 className="mb-2 text-lg font-semibold">No Login</h3>
                                <p className="text-sm text-zinc-400">
                                    Use it immediately. No accounts, no tracking, no bullshit.
                                </p>
                            </div>
                        </div>
                    </div>
                </section>

                {/* Footer */}
                <footer className="border-t border-zinc-800 px-4 py-8 text-center text-sm text-zinc-500">
                    <p>
                        Built for ARC Raiders players by players. Prices from{' '}
                        <a href="https://odealo.com" className="text-zinc-400 hover:text-white">
                            Odealo
                        </a>
                        .
                    </p>
                    <p className="mt-2">
                        Not affiliated with Embark Studios or ARC Raiders.
                    </p>
                </footer>
            </div>
        </>
    );
}