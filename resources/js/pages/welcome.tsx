import { SpotlightCarousel } from '@/components/compare/spotlight-carousel';
import EndlessCarousel from '@/components/EndlessCarousel';
import Header from '@/components/Header';
import IgdbAttribution from '@/components/igdb-attribution';
import IntroSplash from '@/components/landing/IntroSplash';
import { type GameRowData, type SharedData } from '@/types';
import { Head, Link, usePage } from '@inertiajs/react';
import { useState } from 'react';

interface WelcomeProps {
    canRegister?: boolean;
    hero: any;
    spotlightGames?: any[];
    rows: GameRowData[];
    landingStats?: any;
    cta: { pricing: string };
}

export default function Welcome({
    hero,
    spotlightGames = [],
    rows,
    landingStats,
}: WelcomeProps) {
    const { props } = usePage<SharedData>();
    const [introComplete, setIntroComplete] = useState(true);

    return (
        <>
            <Head title="Game Compare – Premium Discovery">
                <link rel="preconnect" href="https://fonts.bunny.net" />
                <link
                    href="https://fonts.bunny.net/css?family=inter:400,500,600,700,800&display=swap"
                    rel="stylesheet"
                />
                {hero?.image && (
                    <link
                        rel="preload"
                        as="image"
                        href={hero.image}
                        type="image/webp"
                    />
                )}
            </Head>

            {!introComplete && (
                <IntroSplash onComplete={() => setIntroComplete(true)} />
            )}

            <div className="landing-shell relative min-h-screen text-white selection:bg-blue-500 selection:text-white">
                <div className="landing-scanlines pointer-events-none absolute inset-0 z-0" />
                <div className="relative z-10 flex min-h-screen flex-col">
                    <Header />

                    <SpotlightCarousel spotlight={spotlightGames} hero={hero} />

                    <section
                        id="rows"
                        className="relative z-20 mt-0 flex flex-col gap-8 border-t border-white/5 bg-[#050505] pt-12 pb-24 shadow-[0_-50px_100px_rgba(0,0,0,0.5)]"
                    >
                        {rows && rows.map((row) => (
                            <EndlessCarousel
                                key={row.id}
                                title={row.title}
                                games={row.games}
                                className="pl-6 lg:pl-12"
                            />
                        ))}
                    </section>

                    {/* Footer */}
                    <footer className="mt-auto border-t border-white/10 bg-black/90 py-12 backdrop-blur">
                        <div className="mx-auto max-w-7xl px-6 lg:px-8">
                            
                            {landingStats && (
                                <div className="mb-12 grid grid-cols-2 gap-8 border-b border-white/5 pb-12 md:grid-cols-3">
                                    <div className="flex flex-col items-center gap-1 text-center">
                                        <span className="text-2xl font-black text-blue-400">{(landingStats.active_prices / 1000).toFixed(0)}K+</span>
                                        <span className="text-[10px] font-bold tracking-[0.2em] text-white/40 uppercase">Active Signals</span>
                                    </div>
                                    <div className="flex flex-col items-center gap-1 text-center">
                                        <span className="text-2xl font-black text-cyan-400">{landingStats.priced_games}</span>
                                        <span className="text-[10px] font-bold tracking-[0.2em] text-white/40 uppercase">Tracked Games</span>
                                    </div>
                                    <div className="hidden flex-col items-center gap-1 text-center md:flex">
                                        <span className="text-2xl font-black text-indigo-400">{landingStats.markets}</span>
                                        <span className="text-[10px] font-bold tracking-[0.2em] text-white/40 uppercase">Global Markets</span>
                                    </div>
                                </div>
                            )}

                            <div className="flex flex-col items-center justify-between gap-6 md:flex-row">
                                <div className="flex items-center gap-3">
                                    <IgdbAttribution />
                                </div>
                                <p className="text-sm text-gray-500">
                                    © {new Date().getFullYear()} Game Compare.
                                    All rights reserved.
                                </p>
                                <div className="flex space-x-6 text-sm text-gray-500">
                                    <Link
                                        href="/privacy-policy"
                                        className="hover:text-white"
                                    >
                                        Privacy
                                    </Link>
                                    <Link
                                        href="/terms-of-service"
                                        className="hover:text-white"
                                    >
                                        Terms
                                    </Link>
                                </div>
                            </div>
                        </div>
                    </footer>
                </div>
            </div>
        </>
    );
}
