interface OrbitStatProps {
    label: string;
    value: string;
}

export default function OrbitStat({ label, value }: OrbitStatProps) {
    return (
        <div className="flex flex-col items-start gap-2 rounded-2xl border border-white/10 bg-black/60 px-4 py-3 text-xs tracking-[0.28em] text-white/70 uppercase">
            <span>{label}</span>
            <span className="text-lg font-semibold text-white">{value}</span>
        </div>
    );
}
