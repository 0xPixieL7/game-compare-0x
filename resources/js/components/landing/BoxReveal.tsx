import { motion } from 'framer-motion';

interface BoxRevealProps {
    children: React.ReactNode;
    rows?: number;
    cols?: number;
    className?: string;
    revealColor?: string;
    delay?: number;
}

export default function BoxReveal({
    children,
    rows = 3,
    cols = 6,
    className = '',
    revealColor = 'bg-black',
    delay = 0,
}: BoxRevealProps) {
    // Generate grid items
    const gridItems = Array.from({ length: rows * cols });

    return (
        <div className={`relative overflow-hidden ${className}`}>
            {/* The Content */}
            <div className="h-full w-full">{children}</div>

            {/* The Grid Overlay */}
            <div
                className="pointer-events-none absolute inset-0 z-20 grid h-full w-full"
                style={{
                    gridTemplateColumns: `repeat(${cols}, 1fr)`,
                    gridTemplateRows: `repeat(${rows}, 1fr)`,
                }}
            >
                {gridItems.map((_, i) => (
                    <motion.div
                        key={i}
                        className={`h-full w-full ${revealColor}`}
                        initial={{ scale: 1.01 }} // 1.01 prevents subpixel gaps
                        animate={{ scale: 0 }}
                        transition={{
                            duration: 0.5,
                            ease: [0.33, 1, 0.68, 1], // Cubic bezier
                            delay:
                                delay +
                                (i % cols) * 0.05 +
                                Math.floor(i / cols) * 0.05, // Diagonal stagger
                        }}
                        style={{ originX: 0.5, originY: 0.5 }}
                    />
                ))}
            </div>
        </div>
    );
}
