import PrismCard from '@/components/landing/PrismCard';
import { show as dashboardShow } from '@/routes/dashboard';
import { type Game } from '@/types';
import { Link } from '@inertiajs/react';
import { motion } from 'framer-motion';
import { ChevronLeft, ChevronRight } from 'lucide-react';
import { useRef } from 'react';

interface DataRowProps {
    title: string;
    games: Game[];
    href?: string;
}

export default function DataRow({
    title,
    games,
    href = '/compare',
}: DataRowProps) {
    const scrollerRef = useRef<HTMLDivElement>(null);

    const handleScroll = (direction: 'next' | 'prev') => {
        if (!scrollerRef.current) {
            return;
        }

        const width = scrollerRef.current.clientWidth;
        const offset = direction === 'next' ? width * 0.9 : -width * 0.9;

        scrollerRef.current.scrollBy({ left: offset, behavior: 'smooth' });
    };

    if (games.length === 0) {
        return null;
    }

    const handleKeyDown = (event: React.KeyboardEvent<HTMLDivElement>) => {
        if (event.key === 'ArrowRight') {
            event.preventDefault();
            handleScroll('next');
        }

        if (event.key === 'ArrowLeft') {
            event.preventDefault();
            handleScroll('prev');
        }
    };

    return (
        <section className="relative py-10" aria-label={`${title} row`}>
            <div className="mx-auto flex max-w-[92rem] items-center justify-between px-6 lg:px-16">
                <motion.div
                    initial={{ opacity: 0, x: -20 }}
                    whileInView={{ opacity: 1, x: 0 }}
                    viewport={{ once: true }}
                    transition={{ duration: 0.5 }}
                    className="space-y-2"
                >
                    <h2 className="text-xl font-semibold tracking-[0.25em] text-white uppercase">
                        {title}
                    </h2>
                    <p className="text-sm text-white/50">
                        Precision-ranked by live pricing and media signals.
                    </p>
                </motion.div>
                <div className="hidden items-center gap-2 md:flex">
                    <button
                        type="button"
                        onClick={() => handleScroll('prev')}
                        className="rounded-full border border-white/10 bg-black/50 p-2 text-white/70 transition hover:border-white/30 hover:text-white"
                        aria-label={`Scroll ${title} left`}
                    >
                        <ChevronLeft className="h-4 w-4" />
                    </button>
                    <button
                        type="button"
                        onClick={() => handleScroll('next')}
                        className="rounded-full border border-white/10 bg-black/50 p-2 text-white/70 transition hover:border-white/30 hover:text-white"
                        aria-label={`Scroll ${title} right`}
                    >
                        <ChevronRight className="h-4 w-4" />
                    </button>
                </div>
                <Link
                    href={href}
                    className="hidden text-xs tracking-[0.3em] text-white/50 uppercase transition hover:text-white md:block"
                >
                    See all
                </Link>
            </div>

            <div
                ref={scrollerRef}
                role="list"
                tabIndex={0}
                onKeyDown={handleKeyDown}
                className="scrollbar-hidden mt-6 flex snap-x snap-mandatory gap-6 overflow-x-auto px-6 pb-4 outline-none focus-visible:ring-2 focus-visible:ring-blue-400 lg:px-16"
            >
                {games.map((game, index) => (
                    <motion.div
                        key={`${title}-${game.id}`}
                        role="listitem"
                        className="max-w-[240px] min-w-[220px] snap-start"
                        initial={{ opacity: 0, scale: 0.9 }}
                        whileInView={{ opacity: 1, scale: 1 }}
                        viewport={{ once: true, margin: '-5%' }}
                        transition={{
                            duration: 0.4,
                            delay: Math.min(index * 0.05, 0.3), // Cap delay so scrolling fast doesn't hide items
                            ease: 'easeOut',
                        }}
                    >
                        <PrismCard
                            game={game}
                            href={dashboardShow.url(game.id)}
                        />
                    </motion.div>
                ))}
            </div>
        </section>
    );
}
