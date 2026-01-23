import { InertiaLinkProps } from '@inertiajs/react';
import { LucideIcon } from 'lucide-react';

export type PageProps<
    T extends Record<string, unknown> = Record<string, unknown>,
> = T & {
    auth: Auth;
    [key: string]: unknown;
};

export interface Auth {
    user: User;
}

export interface BreadcrumbItem {
    title: string;
    href: string;
}

export interface NavGroup {
    title: string;
    items: NavItem[];
}

export interface NavItem {
    title: string;
    href: NonNullable<InertiaLinkProps['href']>;
    icon?: LucideIcon | null;
    isActive?: boolean;
}

export interface SharedData {
    name: string;
    quote: { message: string; author: string };
    auth: Auth;
    sidebarOpen: boolean;
    [key: string]: unknown;
}

export interface User {
    id: number;
    name: string;
    email: string;
    avatar?: string;
    email_verified_at: string | null;
    two_factor_enabled?: boolean;
    created_at: string;
    updated_at: string;
    [key: string]: unknown;
}

export interface GameMedia {
    cover: { url: string; width: number; height: number } | null;
    cover_url: string | null;
    cover_url_thumb: string | null;
    screenshots: Array<{ url: string; width: number; height: number }>;
    trailers: Array<{
        url?: string;
        thumbnail?: string;
        name?: string;
        video_id?: string;
    }>;
    cover_url_high_res?: string | null;
}

export interface GameMediaSummary {
    images: {
        has_cover: boolean;
        has_screenshots: boolean;
        has_artworks: boolean;
        cover_url: string | null;
        hero_url: string | null;
        total_count: number;
        [key: string]: unknown;
    };
    videos: {
        has_trailers: boolean;
        provider: string | null;
        total_count: number;
        [key: string]: unknown;
    };
}

export interface GameShowMedia {
    hero: string | null;
    logo: string | null;
    poster: string | null;
    background: string | null;
    cover: string | null;
    screenshots: string[];
    trailers: string[];
}

export interface GameShowPrice {
    id: number;
    retailer: string;
    country_code: string;
    currency: string;
    amount: number;
    url: string | null;
    discount_percent: number;
    initial_amount: number | null;
}

export interface GamePricing {
    amount_minor: number;
    amount_major: number;
    currency: string;
    local_currency: string;
    btc_price: number | null;
    is_free: boolean;
    retailer?: string;
}

export interface GamePrice {
    id: number;
    retailer: string;
    amount_minor: number;
    amount_major: number;
    currency: string;
    url: string | null;
    [key: string]: unknown;
}

// The "Rich" Game object (used in Welcome/GameCard)
export interface Game {
    id: number;
    name: string;
    canonical_name: string | null;
    rating: number | null;
    release_date: string | null;
    genres?: string[];
    media: GameMedia;
    pricing?: GamePricing | null;
    description?: string | null;
    synopsis?: string | null;
    backdrop_url?: string | null;
}

export interface GameRowData {
    id: string;
    title: string;
    games: Game[];
}

export interface GameTheme {
    primary: string;
    accent: string;
    background: string;
    surface: string;
    generated_at: string;
}

// The raw model structure used in Show.tsx
export interface GameModel {
    id: number;
    name: string;
    hypes: number | null;
    rating: number | null;
    release_date: string | null;
    summary?: string;
    developer?: string | string[];
    publisher?: string | string[];
    theme?: GameTheme | null;
    attributes?: {
        summary?: string;
        [key: string]: unknown;
    };
    [key: string]: unknown;
}

// The transformed item used in Index.tsx
export interface GameListItem {
    id: number;
    name: string;
    rating: number;
    release_date: string;
    cover_url: string;
    latest_price: number | string | null;
    currency: string | null;
}

export interface FeaturedGame {
    id: number;
    name: string;
    rating: number;
    cover_url: string;
    trailer_url: string;
    description: string;
}

export interface PaginationLink {
    url: string | null;
    label: string;
    active: boolean;
}

export interface PaginatedCollection<T> {
    data: T[];
    links: PaginationLink[];
    meta?: {
        current_page: number;
        last_page: number;
        from: number;
        to: number;
        total: number;
        per_page: number;
    };
}
