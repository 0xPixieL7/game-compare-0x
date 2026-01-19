interface SignalPillProps {
    label: string;
    value: string;
    tone?: 'cool' | 'warm' | 'glow';
}

const tones = {
    cool: 'from-cyan-500/15 via-slate-900/50 to-indigo-500/15 border-white/10 text-cyan-200',
    warm: 'from-amber-500/15 via-slate-900/50 to-rose-500/15 border-white/10 text-amber-200',
    glow: 'from-blue-500/15 via-slate-900/50 to-violet-500/15 border-white/10 text-blue-200',
};

export default function SignalPill({
    label,
    value,
    tone = 'glow',
}: SignalPillProps) {
    return (
        <div
            className={`flex items-center gap-2 rounded-full border bg-gradient-to-r px-3 py-1 text-xs tracking-[0.28em] uppercase ${tones[tone]}`}
        >
            <span className="text-white/70">{label}</span>
            <span className="text-white">{value}</span>
        </div>
    );
}
