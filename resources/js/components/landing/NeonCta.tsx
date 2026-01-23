import { useEffect, useState } from 'react';

export default function NeonCta() {
    const [phase, setPhase] = useState<
        'off' | 'flicker' | 'partial' | 'surge' | 'stable'
    >('off');

    // Animation orchestration
    useEffect(() => {
        const t1 = setTimeout(() => setPhase('flicker'), 400);
        const t2 = setTimeout(() => setPhase('partial'), 1200);
        const t3 = setTimeout(() => setPhase('surge'), 2300);
        const t4 = setTimeout(() => setPhase('stable'), 3200);

        return () => {
            clearTimeout(t1);
            clearTimeout(t2);
            clearTimeout(t3);
            clearTimeout(t4);
        };
    }, []);

    // Visual styles based on phase
    const getContainerStyle = () => {
        switch (phase) {
            case 'off':
                return 'opacity-10 border-white/10';
            case 'flicker':
                return 'animate-neon-flicker border-white/50';
            case 'partial':
                return 'opacity-80 border-white/70 shadow-[0_0_10px_rgba(255,255,255,0.2)]';
            case 'surge':
                return 'opacity-100 border-white shadow-[0_0_30px_rgba(255,255,255,0.6),inset_0_0_20px_rgba(255,255,255,0.4)]';
            case 'stable':
                return 'opacity-100 border-white shadow-[0_0_15px_rgba(255,255,255,0.4),inset_0_0_10px_rgba(255,255,255,0.2)]';
        }
    };

    const getTextStyle = (index: number) => {
        const base = 'transition-all duration-100';
        if (phase === 'off') return `${base} opacity-10 blur-[1px]`;
        if (phase === 'flicker') return `${base} animate-neon-flicker-text`;
        if (phase === 'partial') {
            if (index % 3 === 0) return `${base} opacity-50 blur-[0.5px]`;
            return `${base} opacity-100 drop-shadow-[0_0_5px_rgba(255,255,255,0.8)]`;
        }
        if (phase === 'surge')
            return `${base} opacity-100 text-white drop-shadow-[0_0_15px_rgba(255,255,255,1)] scale-105`;
        return `${base} opacity-100 text-white drop-shadow-[0_0_8px_rgba(255,255,255,0.8)]`;
    };

    return (
        <div className="pointer-events-none relative z-30 flex w-full justify-center py-8 select-none">
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

            <div className="group relative">
                <div
                    className={`relative rounded-xl border-2 px-8 py-3 transition-all duration-300 ease-out ${getContainerStyle()} `}
                >
                    <div className="flex gap-1 font-mono text-2xl font-black tracking-[0.1em] whitespace-nowrap text-white md:text-3xl">
                        {'SCROLL DOWN'.split('').map((char, i) => (
                            <span key={i} className={getTextStyle(i)}>
                                {char === ' ' ? '\u00A0' : char}
                            </span>
                        ))}
                    </div>
                </div>

                <div
                    className={`absolute -bottom-6 left-1/2 h-3 w-3/4 -translate-x-1/2 rounded-[100%] bg-white/20 blur-xl transition-opacity duration-500 ${phase === 'stable' || phase === 'surge' ? 'opacity-100' : 'opacity-0'} `}
                />
            </div>
        </div>
    );
}
