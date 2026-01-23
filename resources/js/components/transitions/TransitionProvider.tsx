import { router } from '@inertiajs/react';
import React, {
    createContext,
    useCallback,
    useContext,
    useMemo,
    useRef,
    useState,
} from 'react';

type TransitionContextValue = {
    navigateCardToDetail: (href: string) => Promise<void>;
    isRunning: boolean;
};

const TransitionContext = createContext<TransitionContextValue | null>(null);

export function useTransitionNav() {
    const ctx = useContext(TransitionContext);
    if (!ctx) {
        throw new Error(
            'useTransitionNav must be used inside <TransitionProvider />',
        );
    }
    return ctx;
}

export function TransitionProvider({
    children,
}: {
    children: React.ReactNode;
}) {
    const videoRef = useRef<HTMLVideoElement | null>(null);

    const [isRunning, setIsRunning] = useState(false);
    const [src, setSrc] = useState<string | null>(null);
    const [visible, setVisible] = useState(false);

    const play = useCallback(async (videoSrc: string) => {
        const v = videoRef.current;
        if (!v) return;

        setSrc(videoSrc);
        setVisible(true);

        // Allow src to apply
        await new Promise((r) => setTimeout(r, 0));

        v.currentTime = 0;
        try {
            await v.play();
        } catch (e) {
            console.error('Video play failed', e);
        }
    }, []);

    const hide = useCallback(() => {
        const v = videoRef.current;
        if (v) v.pause();
        setVisible(false);
    }, []);

    /**
     * Card -> Detail transition:
     * - box-out (setup)
     * - route push near “impact”
     * - box-in (reveal)
     */
    const navigateCardToDetail = useCallback(
        async (href: string) => {
            if (isRunning) return;
            setIsRunning(true);

            try {
                // Box appears / lid setup
                await play('/transitions/box-out.mp4');

                // Navigate around the “impact” moment (tune this)
                await new Promise((r) => setTimeout(r, 420));

                // Using Inertia router
                router.visit(href);

                // Explosion/smoke/reveal
                await play('/transitions/box-in.mp4');
                await new Promise((r) => setTimeout(r, 650));
            } finally {
                hide();
                setIsRunning(false);
            }
        },
        [hide, isRunning, play],
    );

    const value = useMemo(
        () => ({ navigateCardToDetail, isRunning }),
        [navigateCardToDetail, isRunning],
    );

    return (
        <TransitionContext.Provider value={value}>
            {children}

            {/* Fullscreen overlay */}
            <div
                style={{
                    position: 'fixed',
                    inset: 0,
                    pointerEvents: 'none',
                    opacity: visible ? 1 : 0,
                    transition: 'opacity 140ms ease',
                    zIndex: 999999,
                }}
            >
                <video
                    ref={videoRef}
                    src={src ?? undefined}
                    playsInline
                    muted
                    preload="auto"
                    style={{
                        width: '100%',
                        height: '100%',
                        objectFit: 'cover',
                        display: 'block',
                    }}
                />
            </div>
        </TransitionContext.Provider>
    );
}
