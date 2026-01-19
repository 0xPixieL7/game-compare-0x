import { type ReactNode, useEffect, useRef } from 'react';

interface AppleTvCardProps {
    children: ReactNode;
    className?: string;
    style?: React.CSSProperties;
    shineClassName?: string;
    enableTilt?: boolean;
}

export const AppleTvCard: React.FC<AppleTvCardProps> = ({
    children,
    className = '',
    style = {},
    shineClassName = '',
    enableTilt = true,
}) => {
    const cardRef = useRef<HTMLDivElement>(null);
    const shineRef = useRef<HTMLDivElement>(null);
    const rafRef = useRef<number | null>(null);
    const prefersReducedMotion = useRef(false);

    useEffect(() => {
        if (typeof window === 'undefined') {
            return;
        }

        prefersReducedMotion.current = window.matchMedia(
            '(prefers-reduced-motion: reduce)',
        ).matches;
    }, []);

    const updateCard = (event: React.PointerEvent<HTMLDivElement>) => {
        if (!enableTilt || prefersReducedMotion.current) {
            return;
        }

        const card = cardRef.current;
        const shine = shineRef.current;
        if (!card || !shine) {
            return;
        }

        const rect = card.getBoundingClientRect();
        const x = event.clientX - rect.left;
        const y = event.clientY - rect.top;
        const centerX = rect.width / 2;
        const centerY = rect.height / 2;
        const rotateX = ((centerY - y) / centerY) * 10;
        const rotateY = ((x - centerX) / centerX) * 10;
        const shineX = (x / rect.width) * 100;
        const shineY = (y / rect.height) * 100;

        if (rafRef.current) {
            cancelAnimationFrame(rafRef.current);
        }

        rafRef.current = requestAnimationFrame(() => {
            card.style.transform = `perspective(1000px) scale(1.04) rotateX(${rotateX}deg) rotateY(${rotateY}deg)`;
            card.style.transition =
                'transform 150ms ease, box-shadow 150ms ease';
            shine.style.background = `radial-gradient(circle at ${shineX}% ${shineY}%, rgba(255, 255, 255, 0.45), transparent 80%)`;
            shine.style.opacity = '1';
        });
    };

    const resetCard = () => {
        const card = cardRef.current;
        const shine = shineRef.current;
        if (!card || !shine) {
            return;
        }

        if (rafRef.current) {
            cancelAnimationFrame(rafRef.current);
        }

        card.style.transform =
            'perspective(1000px) scale(1) rotateX(0deg) rotateY(0deg)';
        card.style.transition = 'transform 400ms ease, box-shadow 400ms ease';
        shine.style.opacity = '0';
    };

    return (
        <div
            ref={cardRef}
            className={`apple-tv-card group/atv ${className}`}
            onPointerMove={updateCard}
            onPointerLeave={resetCard}
            onPointerDown={resetCard}
            style={style}
        >
            <div
                ref={shineRef}
                className={`pointer-events-none absolute inset-0 z-20 transition-opacity duration-300 ${shineClassName}`}
                aria-hidden="true"
            />

            <div className="transform-style-3d relative z-10 h-full w-full">
                {children}
            </div>
        </div>
    );
};
