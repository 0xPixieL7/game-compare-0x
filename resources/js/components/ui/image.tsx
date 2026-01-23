import { type ImgHTMLAttributes, type CSSProperties } from 'react';

interface ImageProps extends ImgHTMLAttributes<HTMLImageElement> {
    fill?: boolean;
    priority?: boolean;
    style?: CSSProperties;
}

export default function Image({
    src,
    alt,
    className = '',
    style,
    fill,
    priority, // consumed but not used for now
    ...props
}: ImageProps) {
    const fillClasses = fill
        ? 'absolute inset-0 h-full w-full object-cover'
        : '';

    return (
        <img
            src={src}
            alt={alt}
            className={`${fillClasses} ${className}`}
            style={style}
            loading={priority ? 'eager' : 'lazy'}
            {...props}
        />
    );
}
