import { type ReactNode } from 'react';

interface MetricBadgeProps {
    label: string;
    value: string;
    accent?: 'default' | 'positive' | 'alert';
    icon?: ReactNode;
}

const accents = {
    default: 'text-slate-200/80 bg-white/5 border-white/10',
    positive: 'text-emerald-200 bg-emerald-500/10 border-emerald-400/20',
    alert: 'text-rose-200 bg-rose-500/10 border-rose-400/20',
};

export default function MetricBadge({
    label,
    value,
    accent = 'default',
    icon,
}: MetricBadgeProps) {
    return (
        <div
            className={`flex items-center gap-3 rounded-full border px-4 py-2 text-sm backdrop-blur ${accents[accent]}`}
        >
            {icon ? <span className="text-base">{icon}</span> : null}
            <div className="flex flex-col leading-tight">
                <span className="text-xs tracking-[0.2em] text-white/50 uppercase">
                    {label}
                </span>
                <span className="text-sm font-semibold text-white">
                    {value}
                </span>
            </div>
        </div>
    );
}
