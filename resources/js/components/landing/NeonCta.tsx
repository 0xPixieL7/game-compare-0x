import { dashboard, login } from '@/routes';
import { type SharedData } from '@/types';
import { Link, usePage } from '@inertiajs/react';
import { useEffect, useState } from 'react';

export default function NeonCta() {
    const { auth } = usePage<SharedData>().props;
    const [phase, setPhase] = useState<'off' | 'flicker' | 'partial' | 'surge' | 'stable'>('off');

    // Animation orchestration
    useEffect(() => {
        // 0-0.4s: Off
        const t1 = setTimeout(() => setPhase('flicker'), 400);
        // 0.4-1.2s: Flicker
        const t2 = setTimeout(() => setPhase('partial'), 1200);
        // 1.2-2.3s: Partial
        const t3 = setTimeout(() => setPhase('surge'), 2300);
        // 2.3-3.2s: Surge -> Stable
        const t4 = setTimeout(() => setPhase('stable'), 3200);

        return () => {
            clearTimeout(t1);
            clearTimeout(t2);
            clearTimeout(t3);
            clearTimeout(t4);
        };
    }, []);

    const href = auth.user ? dashboard() : login();

    // Visual styles based on phase
    const getContainerStyle = () => {
        switch (phase) {
            case 'off': return 'opacity-10 border-white/10';
            case 'flicker': return 'animate-neon-flicker border-white/50';
            case 'partial': return 'opacity-80 border-white/70 shadow-[0_0_10px_rgba(255,255,255,0.2)]';
            case 'surge': return 'opacity-100 border-white shadow-[0_0_30px_rgba(255,255,255,0.6),inset_0_0_20px_rgba(255,255,255,0.4)]';
            case 'stable': return 'opacity-100 border-white shadow-[0_0_15px_rgba(255,255,255,0.4),inset_0_0_10px_rgba(255,255,255,0.2)]';
        }
    };

    const getTextStyle = (letterIndex: number) => {
        const base = "transition-all duration-100";
        if (phase === 'off') return `${base} opacity-10 blur-[1px]`;
        
        if (phase === 'flicker') {
            // Random flickering is hard with pure CSS classes without keyframes, 
            // but we can simulate mostly-on with rapid changes if we had a loop.
            // Simplified: alternating opacity
            return `${base} animate-neon-flicker-text`;
        }

        if (phase === 'partial') {
            // "O" and "N" dim
            if (letterIndex === 0 || letterIndex === 3) return `${base} opacity-50 blur-[0.5px]`;
            return `${base} opacity-100 drop-shadow-[0_0_5px_rgba(255,255,255,0.8)]`;
        }

        if (phase === 'surge') {
            return `${base} opacity-100 text-white drop-shadow-[0_0_15px_rgba(255,255,255,1)] scale-105`;
        }

        // Stable
        return `${base} opacity-100 text-white drop-shadow-[0_0_8px_rgba(255,255,255,0.8)]`;
    };

    return (
        <div className="w-full flex justify-center py-12 relative z-30">
            <style>{`
                @keyframes neon-flicker {
                    0%, 19%, 21%, 23%, 25%, 54%, 56%, 100% { opacity: 0.99; }
                    20%, 24%, 55% { opacity: 0.4; }
                }
                @keyframes neon-flicker-text {
                    0%, 19%, 21%, 23%, 25%, 54%, 56%, 100% { opacity: 1; text-shadow: 0 0 10px white; }
                    20%, 24%, 55% { opacity: 0.5; text-shadow: none; }
                }
                .animate-neon-flicker { animation: neon-flicker 1.5s infinite; }
                .animate-neon-flicker-text { animation: neon-flicker-text 1.5s infinite; }
            `}</style>

            <Link href={href} className="group relative">
                {/* Border Container */}
                <div 
                    className={`
                        relative px-12 py-4 rounded-xl border-4 
                        transition-all duration-300 ease-out
                        ${getContainerStyle()}
                    `}
                >
                    {/* Text */}
                    <div className="flex gap-2 text-4xl md:text-6xl font-black tracking-[0.2em] font-mono text-white">
                        {['O', 'P', 'E', 'N'].map((char, i) => (
                            <span key={i} className={getTextStyle(i)}>
                                {char}
                            </span>
                        ))}
                    </div>
                </div>

                {/* Reflection/Ground Glow */}
                <div className={`
                    absolute -bottom-8 left-1/2 -translate-x-1/2 w-3/4 h-4 
                    bg-white/20 blur-xl rounded-[100%] transition-opacity duration-500
                    ${phase === 'stable' || phase === 'surge' ? 'opacity-100' : 'opacity-0'}
                `} />
            </Link>
        </div>
    );
}
