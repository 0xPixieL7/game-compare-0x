import EndlessCarousel from '@/components/EndlessCarousel';
import IgdbAttribution from '@/components/igdb-attribution';
import { dashboard, login, register } from '@/routes';
import { type Game, type GameRowData, type SharedData } from '@/types';
import { AppleTvCard } from '@/components/apple-tv-card';
import { Head, Link, usePage } from '@inertiajs/react';

interface WelcomeProps {
    canRegister?: boolean;
    hero: Game | null;
    rows: GameRowData[];
    cta: { pricing: string };
}

export default function Welcome({
    canRegister = true,
    hero,
    rows,
}: WelcomeProps) {
    const { auth } = usePage<SharedData>().props;

    // Cyberpunk 2077 high-res background for landing page
    const heroImage = 'https://images.igdb.com/igdb/image/upload/t_1080p/co2mdf.jpg';

    const stats = [
        { label: 'Live prices', value: '250K+' },
        { label: 'Markets tracked', value: '120+' },
        { label: 'Platforms', value: '15+' },
    ];

    return (
        <>
            <Head title="Game Compare – IGDB Powered"> 
                <link rel="preconnect" href="https://fonts.bunny.net" />
                <link
                    href="https://fonts.bunny.net/css?family=inter:400,500,600,700,800&display=swap"
                    rel="stylesheet"
                />
            </Head>
            <div className="relative min-h-screen bg-black text-white selection:bg-blue-500 selection:text-white">
                {/* Hero Background Image */}
                <div className="fixed inset-0 z-0 overflow-hidden">
                    <div className="absolute inset-0 z-10 bg-gradient-to-b from-black via-black/60 to-black" />
                    <div className="absolute inset-0 z-10 bg-gradient-to-r from-black via-black/40 to-transparent" />
                    <img
                        src={heroImage}
                        alt="The Witcher backdrop"
                        className="h-full w-full object-cover object-center opacity-65"
                        loading="lazy"
                    />
                </div>

                {/* Content */}
                <div className="relative z-10 flex min-h-screen flex-col">
                    {/* Navigation */}
                    <header className="w-full bg-gradient-to-b from-black/80 to-transparent px-6 py-6 transition-all duration-500 lg:px-12">
                        <nav className="mx-auto flex w-full items-center justify-between">
                            <div className="flex items-center gap-2">
                                <img
                                    src="/gc.svg"
                                    alt="Game Compare"
                                    className="h-8 w-auto lg:h-10"
                                />
                                <span className="text-sm font-semibold text-gray-300">
                                    Markets • Charts • Alerts
                                </span>
                            </div>
                            <div className="flex items-center gap-4">
                                {auth.user ? (
                                    <Link
                                        href={dashboard()}
                                        className="rounded bg-blue-500 px-4 py-1.5 text-sm font-semibold text-white transition-all hover:bg-blue-600"
                                    >
                                        Dashboard
                                    </Link>
                                ) : (
                                    <>
                                        <Link
                                            href={login()}
                                            className="rounded px-4 py-1.5 text-sm font-semibold text-white transition-colors hover:text-gray-300"
                                        >
                                            Log in
                                        </Link>
                                        {canRegister && (
                                            <Link
                                                href={register()}
                                                className="rounded bg-blue-500 px-4 py-1.5 text-sm font-semibold text-white transition-all hover:bg-blue-600"
                                            >
                                                Sign Up
                                            </Link>
                                        )}
                                    </>
                                )}
                            </div>
                        </nav>
                    </header>

                    {/* Hero Section */}
                    <main className="flex flex-col px-4 pt-14 pb-12 sm:px-6 lg:px-12 lg:pt-24">
                        <div className="grid gap-10 lg:grid-cols-2 lg:items-center">
                            <div className="space-y-6">
                                {hero && (
                                    <div className="flex items-center gap-2 text-xs font-bold uppercase tracking-wide text-blue-300/90">
                                        <span className="rounded-full bg-blue-500/20 px-3 py-1 text-blue-200">
                                            #{hero.id}
                                        </span>
                                        <span>{hero.genres?.[0] || 'Featured'}</span>
                                    </div>
                                )}

                                <h1 className="text-4xl font-black leading-tight tracking-tight drop-shadow-2xl sm:text-5xl lg:text-6xl">
                                    {hero ? (
                                        <>
                                            {hero.canonical_name || hero.name}
                                            <span className="block text-blue-400">Live market view</span>
                                        </>
                                    ) : (
                                        <>
                                            Track every game price
                                            <span className="block text-blue-400">IGDB-powered intelligence</span>
                                        </>
                                    )}
                                </h1>

                                <p className="max-w-2xl text-lg text-gray-200 sm:text-xl">
                                    {hero
                                        ? `Real-time prices and media for ${hero.name}, spanning Steam, Switch, PlayStation, Xbox, and PC.`
                                        : 'Streaming prices, media, and metadata for every title across platforms and regions—rebased to BTC and ready for alerts.'}
                                </p>

                                <div className="flex flex-col gap-3 sm:flex-row sm:items-center">
                                    {auth.user ? (
                                        <Link
                                            href={dashboard()}
                                            className="flex items-center justify-center gap-2 rounded bg-white px-6 py-3 text-base font-bold text-black transition-all hover:bg-gray-200"
                                        >
                                            <span>Open dashboard</span>
                                        </Link>
                                    ) : (
                                        <Link
                                            href={register()}
                                            className="flex items-center justify-center gap-2 rounded bg-blue-500 px-6 py-3 text-base font-bold text-white transition-all hover:bg-blue-600"
                                        >
                                            Start free
                                        </Link>
                                    )}
                                    <Link
                                        href="#rows"
                                        className="flex items-center justify-center gap-2 rounded border border-white/20 bg-white/5 px-6 py-3 text-base font-semibold text-white backdrop-blur transition-all hover:border-white/40"
                                    >
                                        Browse catalog
                                    </Link>
                                </div>

                                <div className="grid grid-cols-1 gap-4 rounded-lg border border-white/10 bg-white/5 p-4 backdrop-blur sm:grid-cols-3">
                                    {stats.map((stat) => (
                                        <div key={stat.label} className="space-y-1 text-center sm:text-left">
                                            <div className="text-xl font-black text-white sm:text-2xl">{stat.value}</div>
                                            <div className="text-xs font-semibold uppercase tracking-wide text-gray-300">
                                                {stat.label}
                                            </div>
                                        </div>
                                    ))}
                                </div>
                            </div>

                            <AppleTvCard className="lg:ml-auto min-h-[400px]">
                                <div className="p-6 h-full flex flex-col">
                                    <div className="mb-3 text-xs font-semibold uppercase tracking-wide text-blue-200">Spotlight</div>
                                    <div className="space-y-3 flex-1">
                                        <div className="text-2xl font-black text-white">
                                            {hero?.canonical_name || hero?.name || 'Global markets'}
                                        </div>
                                        <div className="flex flex-wrap gap-2 text-sm text-gray-200">
                                            <span className="rounded-full bg-white/10 px-3 py-1">Real-time prices</span>
                                            <span className="rounded-full bg-white/10 px-3 py-1">Charts</span>
                                            <span className="rounded-full bg-white/10 px-3 py-1">Media</span>
                                            <span className="rounded-full bg-white/10 px-3 py-1">Alerts</span>
                                        </div>
                                        <div className="rounded-lg bg-black/40 p-4">
                                            <div className="flex items-center justify-between text-sm text-gray-200">
                                                <span>Price heatmap</span>
                                                <span className="text-blue-300">Live</span>
                                            </div>
                                            <div className="mt-3 h-24 rounded bg-gradient-to-r from-blue-500/40 via-purple-500/30 to-blue-500/40" />
                                        </div>
                                    </div>
                                    <div className="mt-6 grid grid-cols-2 gap-3 text-sm text-gray-200">
                                        <div>
                                            <div className="text-xs font-semibold uppercase text-blue-200">Coverage</div>
                                            <div className="text-white">Global markets</div>
                                        </div>
                                        <div>
                                            <div className="text-xs font-semibold uppercase text-blue-200">Data partner</div>
                                            <div className="text-white">IGDB</div>
                                        </div>
                                        <div>
                                            <div className="text-xs font-semibold uppercase text-blue-200">Realtime</div>
                                            <div className="text-white">Every refresh</div>
                                        </div>
                                        <div>
                                            <div className="text-xs font-semibold uppercase text-blue-200">Platforms</div>
                                            <div className="text-white">PC • Console • Switch</div>
                                        </div>
                                    </div>
                                </div>
                            </AppleTvCard>
                        </div>
                    </main>

                    {/* Content Rows */}
                    <div
                        id="rows"
                        className="relative z-20 mt-10 flex flex-col gap-4 pb-20"
                    >
                        {rows.map((row) => (
                            <EndlessCarousel
                                key={row.id}
                                title={row.title}
                                games={row.games}
                                className="pl-0"
                            />
                        ))}
                    </div>

                    {/* Footer */}
                    <footer className="mt-auto border-t border-white/10 bg-black/90 py-12 backdrop-blur">
                        <div className="mx-auto max-w-7xl px-6 lg:px-8">
                            <div className="flex flex-col items-center justify-between gap-6 md:flex-row">
                                <div className="flex items-center gap-3">
                                    <IgdbAttribution />
                                </div>
                                <p className="text-sm text-gray-500">
                                    © {new Date().getFullYear()} Game Compare. All rights reserved.
                                </p>
                                <div className="flex space-x-6 text-sm text-gray-500">
                                    <Link href="/privacy-policy" className="hover:text-white">
                                        Privacy
                                    </Link>
                                    <Link href="/terms-of-service" className="hover:text-white">
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
