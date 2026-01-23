import { Float, MeshWobbleMaterial } from '@react-three/drei';
import { useFrame } from '@react-three/fiber';
import gsap from 'gsap';
import { useEffect, useRef } from 'react';
import * as THREE from 'three';

export function DigitalBox({ isOpening }: { isOpening: boolean }) {
    const groupRef = useRef<THREE.Group>(null);
    const coreRef = useRef<THREE.Mesh>(null);
    const shellRef = useRef<THREE.Mesh>(null);

    // Discrete animation triggers with GSAP
    useEffect(() => {
        if (isOpening && groupRef.current) {
            // Unboxing sequence
            const tl = gsap.timeline();

            tl.to(groupRef.current.scale, {
                x: 1.5,
                y: 1.5,
                z: 1.5,
                duration: 0.5,
                ease: 'back.out(2)',
            })
                .to(
                    groupRef.current.rotation,
                    {
                        y: Math.PI * 2,
                        duration: 1.5,
                        ease: 'power2.inOut',
                    },
                    0,
                )
                .to(
                    groupRef.current.scale,
                    {
                        x: 0,
                        y: 0,
                        z: 0,
                        duration: 1,
                        ease: 'expo.in',
                    },
                    '+=0.2',
                );
        }
    }, [isOpening]);

    // Gentle idle rotation
    useFrame((state) => {
        if (!groupRef.current || isOpening) return;
        groupRef.current.rotation.y += 0.005;
        groupRef.current.rotation.x += 0.002;
    });

    return (
        <group ref={groupRef}>
            <Float speed={2} rotationIntensity={0.5} floatIntensity={0.5}>
                {/* Main core cube */}
                <mesh ref={coreRef}>
                    <boxGeometry args={[1, 1, 1]} />
                    <MeshWobbleMaterial
                        color="#3b82f6"
                        factor={0.4}
                        speed={2}
                        emissive="#1d4ed8"
                        emissiveIntensity={2}
                    />
                </mesh>

                {/* Wireframe shell */}
                <mesh ref={shellRef} scale={1.2}>
                    <boxGeometry args={[1, 1, 1]} />
                    <meshBasicMaterial
                        color="#60a5fa"
                        wireframe
                        transparent
                        opacity={0.3}
                    />
                </mesh>
            </Float>

            {/* Glow aura */}
            <pointLight
                intensity={isOpening ? 20 : 5}
                color="#3b82f6"
                distance={10}
            />
        </group>
    );
}
