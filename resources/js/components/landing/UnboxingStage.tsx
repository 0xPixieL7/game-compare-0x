import { Environment, PerspectiveCamera, Stars } from '@react-three/drei';
import { Canvas } from '@react-three/fiber';
import { AnimatePresence, motion } from 'framer-motion';
import { Suspense, useEffect, useState } from 'react';
import { DigitalBox } from './DigitalBox';

interface UnboxingStageProps {
    onComplete: () => void;
    gameTitle?: string;
}

export default function UnboxingStage({
    onComplete,
    gameTitle,
}: UnboxingStageProps) {
    const [isOpening, setIsOpening] = useState(false);
    const [showUI, setShowUI] = useState(false);

    useEffect(() => {
        // Initial delay before auto-opening
        const timer = setTimeout(() => {
            setIsOpening(true);
        }, 1500);

        // Fade out 3D scene and callback to parent
        const completeTimer = setTimeout(() => {
            setShowUI(true);
            setTimeout(onComplete, 800); // Small overlap
        }, 3500);

        return () => {
            clearTimeout(timer);
            clearTimeout(completeTimer);
        };
    }, [onComplete]);

    return (
        <div className="fixed inset-0 z-[100] bg-black">
            <Canvas shadows>
                <PerspectiveCamera makeDefault position={[0, 0, 5]} fov={50} />
                <Stars
                    radius={100}
                    depth={50}
                    count={5000}
                    factor={4}
                    saturation={0}
                    fade
                    speed={1}
                />
                <ambientLight intensity={0.2} />
                <spotLight
                    position={[10, 10, 10]}
                    angle={0.15}
                    penumbra={1}
                    intensity={10}
                    castShadow
                />

                <Suspense fallback={null}>
                    <DigitalBox isOpening={isOpening} />
                    <Environment preset="city" />
                </Suspense>
            </Canvas>

            <div className="pointer-events-none absolute inset-0 flex flex-col items-center justify-center space-y-8 text-center">
                <AnimatePresence>
                    {!isOpening && (
                        <motion.div
                            initial={{ opacity: 0, y: 20 }}
                            animate={{ opacity: 1, y: 0 }}
                            exit={{ opacity: 0, scale: 1.5 }}
                            className="space-y-2"
                        >
                            <h2 className="text-sm font-bold tracking-[0.5em] text-blue-400 uppercase">
                                Initializing Signal
                            </h2>
                            <h3 className="text-4xl font-black text-white lg:text-6xl">
                                {gameTitle || 'Market Discovery'}
                            </h3>
                        </motion.div>
                    )}
                </AnimatePresence>

                <AnimatePresence>
                    {isOpening && !showUI && (
                        <motion.div
                            initial={{ opacity: 0, scale: 0.8 }}
                            animate={{ opacity: 1, scale: 1 }}
                            exit={{ opacity: 0 }}
                            className="absolute h-64 w-64 rounded-full bg-blue-500/20 blur-3xl"
                        />
                    )}
                </AnimatePresence>
            </div>

            {/* Global overlay mask for smooth exit */}
            <motion.div
                animate={{ opacity: showUI ? 1 : 0 }}
                className="pointer-events-none absolute inset-0 z-10 bg-black/50 backdrop-blur-sm"
            />
        </div>
    );
}
