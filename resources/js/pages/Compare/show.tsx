import { AppleTvCard } from '@/components/apple-tv-card';
import { Head, Link } from '@inertiajs/react';
import { ChevronLeft, Sparkles, Star } from 'lucide-react';

interface ShowProps {
    game: any; // Ideally typed with SpotlightProduct or similar
}

export default function Show({ game }: ShowProps) {
    // Ensure transition name matches the card
    const vtName = `game-cover-${game.id}`;

    // Helper for cover URL
    const coverUrl =
        game.cover_url_high_res ||
        game.cover_url ||
        game.media?.cover_url_high_res ||
        game.media?.cover_url ||
        '/placeholder-game.jpg';

    return (
        <div className="min-h-screen bg-black text-white selection:bg-blue-500 selection:text-white">
            <Head title={`${game.name} - Game Compare`} />

            {/* Navbar / Back Button */}
            <header className="fixed top-0 left-0 z-50 w-full p-6 lg:p-12">
                <Link
                    href="/"
                    className="group inline-flex items-center gap-2 rounded-full border border-white/10 bg-black/50 px-4 py-2 text-sm font-medium backdrop-blur-md transition-all hover:bg-white/10"
                >
                    <ChevronLeft className="h-4 w-4 text-white/70 transition-transform group-hover:-translate-x-1" />
                    Back
                </Link>
            </header>

            <main className="relative pt-32 pb-24 lg:pt-48">
                {/* Hero Section */}
                <div className="mx-auto max-w-7xl px-6 lg:px-12">
                    <div className="grid gap-12 lg:grid-cols-[1fr_1.5fr]">
                        {/* Cover Image with Transition */}
                        <div className="relative mx-auto w-full max-w-md lg:mx-0 lg:max-w-none">
                            <AppleTvCard
                                className="aspect-[3/4] w-full overflow-hidden rounded-3xl border border-white/10 shadow-2xl lg:aspect-square"
                                enableTilt
                            >
                                <img
                                    src={coverUrl}
                                    alt={game.name}
                                    className="h-full w-full object-cover"
                                    style={{ viewTransitionName: vtName }}
                                />
                                <div className="absolute inset-0 bg-gradient-to-t from-black/60 via-transparent to-transparent" />
                            </AppleTvCard>
                        </div>

                        {/* Details */}
                        <div className="flex flex-col justify-end space-y-8">
                            <div className="space-y-4">
                                <div className="flex flex-wrap gap-3">
                                    {game.rating && (
                                        <div className="flex items-center gap-2 rounded-full border border-yellow-500/20 bg-yellow-500/10 px-3 py-1 text-xs font-bold tracking-wider text-yellow-400 uppercase backdrop-blur-md">
                                            <Star className="h-3.5 w-3.5 fill-current" />
                                            Score: {Math.round(game.rating)}
                                        </div>
                                    )}
                                    <div className="flex items-center gap-2 rounded-full border border-blue-500/20 bg-blue-500/10 px-3 py-1 text-xs font-bold tracking-wider text-blue-400 uppercase backdrop-blur-md">
                                        <Sparkles className="h-3.5 w-3.5" />
                                        Verified
                                    </div>
                                </div>

                                <h1 className="text-5xl font-black tracking-tighter text-white sm:text-6xl lg:text-7xl">
                                    {game.name}
                                </h1>

                                <p className="max-w-xl text-lg leading-relaxed text-slate-400">
                                    {game.summary ||
                                        'No summary available for this title yet. Check back soon for detailed pricing analysis and cross-region comparisons.'}
                                </p>
                            </div>

                            {/* Stats Grid */}
                            <div className="grid grid-cols-2 gap-4 lg:grid-cols-3">
                                <div className="rounded-2xl border border-white/10 bg-white/5 p-4 backdrop-blur-sm">
                                    <div className="text-xs font-bold tracking-widest text-white/40 uppercase">
                                        Release Date
                                    </div>
                                    <div className="mt-1 font-mono text-lg font-medium">
                                        {game.first_release_date
                                            ? new Date(
                                                  game.first_release_date *
                                                      1000,
                                              ).toLocaleDateString()
                                            : 'TBA'}
                                    </div>
                                </div>
                                <div className="rounded-2xl border border-white/10 bg-white/5 p-4 backdrop-blur-sm">
                                    <div className="text-xs font-bold tracking-widest text-white/40 uppercase">
                                        Platforms
                                    </div>
                                    <div className="mt-1 font-mono text-lg font-medium">
                                        {game.platforms
                                            ?.map((p: any) => p.abbreviation)
                                            .slice(0, 3)
                                            .join(', ') || 'N/A'}
                                    </div>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
            </main>
        </div>
    );
}
