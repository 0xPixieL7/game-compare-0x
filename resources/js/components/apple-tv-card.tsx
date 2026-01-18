import React, { useState, useRef, useEffect, ReactNode } from 'react';

interface AppleTvCardProps {
    children: ReactNode;
    className?: string;
    style?: React.CSSProperties;
    shineClassName?: string;
}

export const AppleTvCard: React.FC<AppleTvCardProps> = ({ 
    children, 
    className = '', 
    style = {},
    shineClassName = ''
}) => {
    const cardRef = useRef<HTMLDivElement>(null);
    const [rotation, setRotation] = useState({ x: 0, y: 0 });
    const [shine, setShine] = useState({ x: 50, y: 50, opacity: 0 });
    const [isHovered, setIsHovered] = useState(false);

    const handleMouseMove = (e: React.MouseEvent<HTMLDivElement>) => {
        if (!cardRef.current) return;

        const rect = cardRef.current.getBoundingClientRect();
        const x = e.clientX - rect.left; // x position within the element.
        const y = e.clientY - rect.top;  // y position within the element.

        const centerX = rect.width / 2;
        const centerY = rect.height / 2;

        // Rotation: max 10 degrees
        const rotateX = ((centerY - y) / centerY) * 10;
        const rotateY = ((x - centerX) / centerX) * 10;

        setRotation({ x: rotateX, y: rotateY });

        // Shine position (percentage)
        const shineX = (x / rect.width) * 100;
        const shineY = (y / rect.height) * 100;

        setShine({ x: shineX, y: shineY, opacity: 0.4 });
    };

    const handleMouseEnter = () => {
        setIsHovered(true);
    };

    const handleMouseLeave = () => {
        setIsHovered(false);
        setRotation({ x: 0, y: 0 });
        setShine(prev => ({ ...prev, opacity: 0 }));
    };

    return (
        <div
            ref={cardRef}
            className={`apple-tv-card group/atv ${className}`}
            onMouseMove={handleMouseMove}
            onMouseEnter={handleMouseEnter}
            onMouseLeave={handleMouseLeave}
            style={{
                ...style,
                transform: isHovered 
                    ? `perspective(1000px) scale(1.05) rotateX(${rotation.x}deg) rotateY(${rotation.y}deg)` 
                    : 'perspective(1000px) scale(1) rotateX(0deg) rotateY(0deg)',
                transition: isHovered ? 'none' : 'transform 0.5s ease, box-shadow 0.5s ease',
            }}
        >
            {/* Shine/Reflection Layer */}
            <div 
                className={`absolute inset-0 z-20 pointer-events-none transition-opacity duration-300 ${shineClassName}`}
                style={{
                    background: `radial-gradient(circle at ${shine.x}% ${shine.y}%, rgba(255, 255, 255, ${shine.opacity}), transparent 80%)`,
                    opacity: isHovered ? shine.opacity : 0,
                }}
            />
            
            {/* Parallax Content Root */}
            <div className="relative z-10 h-full w-full transform-style-3d">
                {children}
            </div>
        </div>
    );
};
