import EndlessCarousel from '@/components/EndlessCarousel';
import IgdbAttribution from '@/components/igdb-attribution';
import HeroStage from '@/components/landing/HeroStage';
import IntroSplash from '@/components/landing/IntroSplash';
import NeonCta from '@/components/landing/NeonCta';
import { dashboard, login, register } from '@/routes';
import { type Game, type GameRowData, type SharedData } from '@/types';
import { Head, Link, usePage } from '@inertiajs/react';
import { useState } from 'react';

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
    const [introComplete, setIntroComplete] = useState(false);

    return (
        <>
            <Head title="Game Compare – IGDB Powered">
                <link rel="preconnect" href="https://fonts.bunny.net" />
                <link
                    href="https://fonts.bunny.net/css?family=inter:400,500,600,700,800&display=swap"
                    rel="stylesheet"
                />
            </Head>

            {!introComplete && (
                <IntroSplash onComplete={() => setIntroComplete(true)} />
            )}

            <div className="landing-shell relative min-h-screen text-white selection:bg-blue-500 selection:text-white">
                <div className="landing-scanlines pointer-events-none absolute inset-0 z-0" />
                <div className="relative z-10 flex min-h-screen flex-col">
                    <header className="w-full px-6 py-6 lg:px-12">
                        <nav className="mx-auto flex w-full items-center justify-between rounded-full border border-white/10 bg-black/60 px-5 py-3 backdrop-blur">
                            <div className="flex items-center gap-2">
                                <img
                                    src="/gc.svg"
                                    alt="Game Compare"
                                    className="h-8 w-auto lg:h-10"
                                />
                                <span className="text-xs font-semibold tracking-[0.3em] text-gray-300 uppercase">
                                    Signal Lab
                                </span>
                            </div>
                            <div className="flex items-center gap-4">
                                {auth.user ? (
                                    <Link
                                        href={dashboard()}
                                        className="rounded-full bg-blue-500 px-4 py-1.5 text-xs font-semibold tracking-[0.2em] text-white uppercase transition-all hover:bg-blue-400"
                                    >
                                        Dashboard
                                    </Link>
                                ) : (
                                    <>
                                        <Link
                                            href={login()}
                                            className="rounded-full px-4 py-1.5 text-xs font-semibold tracking-[0.2em] text-white uppercase transition-colors hover:text-gray-300"
                                        >
                                            Log in
                                        </Link>
                                        {canRegister && (
                                            <Link
                                                href={register()}
                                                className="rounded-full bg-blue-500 px-4 py-1.5 text-xs font-semibold tracking-[0.2em] text-white uppercase transition-all hover:bg-blue-400"
                                            >
                                                Sign up
                                            </Link>
                                        )}
                                    </>
                                )}
                            </div>
                        </nav>
                    </header>

                    <HeroStage hero={hero} />

                    {/* Neon Open Sign CTA */}
                    <NeonCta />

                    <section
                        id="rows"
                        className="relative z-20 mt-6 flex flex-col gap-8 pb-24"
                    >
                        {Array.isArray(rows) && rows.map((row) => (
                            <EndlessCarousel
                                key={row.id}
                                title={row.title}
                                games={row.games}
                                className="pl-0"
                            />
                        ))}
                    </section>

                    {/* Footer */}
                    <footer className="mt-auto border-t border-white/10 bg-black/90 py-12 backdrop-blur">
                        <div className="mx-auto max-w-7xl px-6 lg:px-8">
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
