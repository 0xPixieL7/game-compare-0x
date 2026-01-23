import { router } from '@inertiajs/react';
import React, {
    createContext,
    useCallback,
    useContext,
    useEffect,
    useMemo,
    useRef,
    useState,
} from 'react';

type TransitionContextValue = {
    navigateCardToDetail: (href: string) => void;
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

type Particle = {
    x: number;
    y: number;
    vx: number;
    vy: number;
    r: number;
    life: number;
    maxLife: number;
};

function clamp01(v: number) {
    return Math.max(0, Math.min(1, v));
}

function easeOutCubic(t: number) {
    return 1 - Math.pow(1 - t, 3);
}

function easeInOutCubic(t: number) {
    return t < 0.5 ? 4 * t * t * t : 1 - Math.pow(-2 * t + 2, 3) / 2;
}

/** Tiny shaping function */
function lifeS(v: number) {
    return v * v;
}

/** Canvas helper */
function roundRect(
    ctx: CanvasRenderingContext2D,
    x: number,
    y: number,
    w: number,
    h: number,
    r: number,
) {
    const rr = Math.min(r, w / 2, h / 2);
    ctx.beginPath();
    ctx.moveTo(x + rr, y);
    ctx.arcTo(x + w, y, x + w, y + h, rr);
    ctx.arcTo(x + w, y + h, x, y + h, rr);
    ctx.arcTo(x, y + h, x, y, rr);
    ctx.arcTo(x, y, x + w, y, rr);
    ctx.closePath();
}

/**
 * Fullscreen canvas “box” transition:
 * - t 0.00 -> 0.35 : box appears / settles
 * - t 0.35 -> 0.55 : lid opens
 * - t 0.42 : impact (navigate here)
 * - t 0.42 -> 1.00 : explosion particles + smoke fade
 */
export function TransitionProvider({
    children,
}: {
    children: React.ReactNode;
}) {
    const canvasRef = useRef<HTMLCanvasElement | null>(null);
    const rafRef = useRef<number | null>(null);

    const [isRunning, setIsRunning] = useState(false);
    const [visible, setVisible] = useState(false);

    const startTimeRef = useRef<number>(0);
    const navigatedRef = useRef<boolean>(false);
    const targetHrefRef = useRef<string>('');

    const particlesRef = useRef<Particle[]>([]);
    const explosionSpawnedRef = useRef<boolean>(false);

    const durationMs = 1050; // total animation length
    const impactMs = 420; // when navigation happens

    const resizeCanvas = useCallback(() => {
        const c = canvasRef.current;
        if (!c) return;

        const dpr = Math.max(1, Math.min(2, window.devicePixelRatio || 1));
        const { innerWidth: w, innerHeight: h } = window;

        c.width = Math.floor(w * dpr);
        c.height = Math.floor(h * dpr);
        c.style.width = `${w}px`;
        c.style.height = `${h}px`;

        const ctx = c.getContext('2d');
        if (ctx) ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    }, []);

    const stop = useCallback(() => {
        if (rafRef.current) cancelAnimationFrame(rafRef.current);
        rafRef.current = null;

        setVisible(false);
        setIsRunning(false);

        particlesRef.current = [];
        explosionSpawnedRef.current = false;
        navigatedRef.current = false;
    }, []);

    const spawnExplosion = useCallback((cx: number, cy: number) => {
        const parts: Particle[] = [];
        const count = 90;

        for (let i = 0; i < count; i++) {
            const a = Math.random() * Math.PI * 2;
            const sp = 2 + Math.random() * 5.5;
            parts.push({
                x: cx,
                y: cy,
                vx: Math.cos(a) * sp,
                vy: Math.sin(a) * sp - (1 + Math.random() * 2),
                r: 2 + Math.random() * 8,
                life: 0,
                maxLife: 34 + Math.random() * 24,
            });
        }

        particlesRef.current = parts;
    }, []);

    const drawFrame = useCallback(
        (now: number) => {
            const c = canvasRef.current;
            if (!c) return;

            const ctx = c.getContext('2d');
            if (!ctx) return;

            const w = window.innerWidth;
            const h = window.innerHeight;

            const elapsed = now - startTimeRef.current;
            const t = clamp01(elapsed / durationMs);

            // Clear (transparent overlay)
            ctx.clearRect(0, 0, w, h);

            // Fade in/out overlay alpha
            const alphaIn = easeOutCubic(clamp01(elapsed / 160));
            const alphaOut =
                1 - easeOutCubic(clamp01((elapsed - (durationMs - 220)) / 220));
            const overlayAlpha = clamp01(alphaIn * alphaOut);

            // Scene center (box position)
            const cx = w * 0.5;
            const cy = h * 0.62;

            // Box settle animation
            const settleT = easeOutCubic(clamp01(elapsed / 320));
            const boxY = cy + (1 - settleT) * 34;

            // Box dimensions
            const bw = Math.min(420, w * 0.45);
            const bh = bw * 0.62;

            // Lid open animation
            const lidT = easeInOutCubic(clamp01((elapsed - 340) / 240));
            const lidAngle = lidT * (-Math.PI / 2.35);

            // Draw a subtle vignette / focus
            ctx.save();
            ctx.globalAlpha = overlayAlpha * 0.9;
            const grd = ctx.createRadialGradient(
                cx,
                boxY,
                40,
                cx,
                boxY,
                Math.max(w, h) * 0.75,
            );
            grd.addColorStop(0, 'rgba(0,0,0,0.25)');
            grd.addColorStop(1, 'rgba(0,0,0,0.00)');
            ctx.fillStyle = grd;
            ctx.fillRect(0, 0, w, h);
            ctx.restore();

            // Draw box shadow
            ctx.save();
            ctx.globalAlpha = overlayAlpha * 0.55;
            ctx.beginPath();
            ctx.ellipse(
                cx,
                boxY + bh * 0.58,
                bw * 0.42,
                bh * 0.14,
                0,
                0,
                Math.PI * 2,
            );
            ctx.fillStyle = 'rgba(0,0,0,0.45)';
            ctx.fill();
            ctx.restore();

            // Draw box base
            ctx.save();
            ctx.globalAlpha = overlayAlpha;

            const baseX = cx - bw / 2;
            const baseY = boxY - bh / 2;

            const baseGrad = ctx.createLinearGradient(
                baseX,
                baseY,
                baseX + bw,
                baseY + bh,
            );
            baseGrad.addColorStop(0, 'rgba(40,140,255,0.95)');
            baseGrad.addColorStop(1, 'rgba(20,70,200,0.95)');

            ctx.fillStyle = baseGrad;
            ctx.strokeStyle = 'rgba(255,255,255,0.25)';
            ctx.lineWidth = 2;

            roundRect(ctx, baseX, baseY, bw, bh, 20);
            ctx.fill();
            ctx.stroke();

            // Front decal strip
            ctx.globalAlpha = overlayAlpha * 0.85;
            ctx.fillStyle = 'rgba(255,255,255,0.14)';
            roundRect(
                ctx,
                baseX + bw * 0.08,
                baseY + bh * 0.58,
                bw * 0.84,
                bh * 0.22,
                14,
            );
            ctx.fill();

            ctx.restore();

            // Lid (rotating rectangle/polygon)
            ctx.save();
            ctx.globalAlpha = overlayAlpha;
            ctx.translate(cx, baseY + 18);
            ctx.rotate(lidAngle);

            const lidW = bw * 1.02;
            const lidH = 36;

            const lidGrad = ctx.createLinearGradient(
                -lidW / 2,
                -lidH,
                lidW / 2,
                lidH,
            );
            lidGrad.addColorStop(0, 'rgba(15,50,150,0.95)');
            lidGrad.addColorStop(1, 'rgba(60,170,255,0.95)');

            ctx.fillStyle = lidGrad;
            ctx.strokeStyle = 'rgba(255,255,255,0.20)';
            ctx.lineWidth = 2;

            roundRect(ctx, -lidW / 2, -lidH, lidW, lidH, 16);
            ctx.fill();
            ctx.stroke();

            ctx.restore();

            // Spawn explosion once around impact
            if (!explosionSpawnedRef.current && elapsed >= impactMs) {
                explosionSpawnedRef.current = true;
                spawnExplosion(cx, boxY - bh * 0.05);
            }

            // Draw explosion particles
            if (explosionSpawnedRef.current) {
                const parts = particlesRef.current;
                ctx.save();
                ctx.globalCompositeOperation = 'lighter';

                for (const p of parts) {
                    p.life += 1;

                    // Update physics
                    p.x += p.vx;
                    p.y += p.vy;
                    p.vy += 0.12; // gravity
                    p.vx *= 0.985;
                    p.vy *= 0.985;

                    const lifeT = clamp01(p.life / p.maxLife);
                    const a = (1 - lifeT) * overlayAlpha;

                    // Fire core
                    ctx.globalAlpha = a * 0.9;
                    ctx.beginPath();
                    ctx.arc(
                        p.x,
                        p.y,
                        p.r * (0.9 + (1 - lifeT) * 0.6),
                        0,
                        Math.PI * 2,
                    );
                    ctx.fillStyle = 'rgba(255,180,40,1)';
                    ctx.fill();

                    // Smoke halo
                    ctx.globalAlpha = a * 0.45;
                    ctx.beginPath();
                    ctx.arc(
                        p.x,
                        p.y,
                        p.r * (1.7 + lifeS(1 - lifeT) * 1.6),
                        0,
                        Math.PI * 2,
                    );
                    ctx.fillStyle = 'rgba(180,180,180,1)';
                    ctx.fill();
                }

                // Remove dead particles
                particlesRef.current = parts.filter((p) => p.life < p.maxLife);

                ctx.restore();
            }

            // Navigate exactly once at impact frame
            if (!navigatedRef.current && elapsed >= impactMs) {
                navigatedRef.current = true;
                router.visit(targetHrefRef.current);
            }

            if (t < 1) {
                rafRef.current = requestAnimationFrame(drawFrame);
            } else {
                stop();
            }
        },
        [spawnExplosion, stop, resizeCanvas],
    );

    const navigateCardToDetail = useCallback(
        (href: string) => {
            if (isRunning) return;

            targetHrefRef.current = href;

            setIsRunning(true);
            setVisible(true);

            // Setup canvas
            resizeCanvas();

            startTimeRef.current = performance.now();
            navigatedRef.current = false;
            explosionSpawnedRef.current = false;
            particlesRef.current = [];

            rafRef.current = requestAnimationFrame(drawFrame);
        },
        [drawFrame, isRunning, resizeCanvas],
    );

    useEffect(() => {
        if (!visible) return;

        const onResize = () => resizeCanvas();
        window.addEventListener('resize', onResize);
        return () => window.removeEventListener('resize', onResize);
    }, [resizeCanvas, visible]);

    const value = useMemo(
        () => ({ navigateCardToDetail, isRunning }),
        [navigateCardToDetail, isRunning],
    );

    return (
        <TransitionContext.Provider value={value}>
            {children}

            {/* Fullscreen Canvas Overlay */}
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
                <canvas ref={canvasRef} />
            </div>
        </TransitionContext.Provider>
    );
}
