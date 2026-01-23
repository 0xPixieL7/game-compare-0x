import { Play } from 'lucide-react';
import { useState } from 'react';

interface MediaPlayerProps {
    url: string;
    thumbnail?: string;
    title?: string;
    className?: string;
    autoPlay?: boolean;
}

const getYoutubeId = (url: string) => {
    const regExp =
        /^.*(youtu.be\/|v\/|u\/\w\/|embed\/|watch\?v=|&v=)([^#&?]*).*/;
    const match = url.match(regExp);
    return match && match[2].length === 11 ? match[2] : null;
};

export default function MediaPlayer({
    url,
    thumbnail,
    title = 'Video player',
    className = '',
    autoPlay = false,
}: MediaPlayerProps) {
    const [isPlaying, setIsPlaying] = useState(autoPlay);
    const youtubeId = getYoutubeId(url);

    if (youtubeId) {
        return (
            <div
                className={`relative overflow-hidden rounded-2xl bg-black shadow-2xl ${className}`}
            >
                {!isPlaying ? (
                    <div
                        className="group relative h-full w-full cursor-pointer"
                        onClick={() => setIsPlaying(true)}
                    >
                        {/* Thumbnail */}
                        <img
                            src={
                                thumbnail ||
                                `https://img.youtube.com/vi/${youtubeId}/maxresdefault.jpg`
                            }
                            alt={title}
                            className="h-full w-full object-cover transition-transform duration-700 group-hover:scale-105"
                        />

                        {/* Overlay Gradient */}
                        <div className="absolute inset-0 bg-gradient-to-t from-black/80 via-transparent to-transparent opacity-60 transition-opacity group-hover:opacity-40" />

                        {/* Play Button */}
                        <div className="absolute inset-0 flex items-center justify-center">
                            <div className="flex h-16 w-16 items-center justify-center rounded-full bg-indigo-600/90 text-white shadow-xl backdrop-blur-sm transition-all duration-300 group-hover:scale-110 group-hover:bg-indigo-500">
                                <Play className="ml-1 h-8 w-8 fill-current" />
                            </div>
                        </div>

                        {/* Title Badge */}
                        <div className="absolute right-4 bottom-4 left-4">
                            <h3 className="line-clamp-1 text-sm font-bold text-white drop-shadow-lg">
                                {title}
                            </h3>
                        </div>
                    </div>
                ) : (
                    <iframe
                        src={`https://www.youtube-nocookie.com/embed/${youtubeId}?autoplay=1&modestbranding=1&rel=0`}
                        title={title}
                        className="h-full w-full"
                        allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture"
                        allowFullScreen
                    />
                )}
            </div>
        );
    }

    // Fallback for native/other video types
    return (
        <div
            className={`relative overflow-hidden rounded-2xl bg-black shadow-2xl ${className}`}
        >
            <video
                controls
                className="h-full w-full object-cover"
                poster={thumbnail}
            >
                <source src={url} type="video/mp4" />
                Your browser does not support the video tag.
            </video>
        </div>
    );
}
