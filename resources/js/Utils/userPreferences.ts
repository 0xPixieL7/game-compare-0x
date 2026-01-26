import Cookies from 'js-cookie';
import { useEffect, useState } from 'react';

export interface UserGameList {
    id: string;
    name: string;
    games: number[];
    createdAt: string;
    updatedAt: string;
    type: 'user' | 'guest';
}

export interface GamePreferences {
    lists: UserGameList[];
    favoriteGenres: string[];
    recentlyViewed: number[];
    lastActivity: string;
}

const PREFERENCES_COOKIE_KEY = 'game_preferences';
const GUEST_SESSION_DURATION = 30; // minutes
const PERMANENT_DURATION = 365; // days

class UserPreferencesManager {
    private preferences: GamePreferences;
    private isGuest: boolean;

    constructor(isAuthenticated: boolean = false) {
        this.isGuest = !isAuthenticated;
        this.preferences = this.loadPreferences();
    }

    private loadPreferences(): GamePreferences {
        const defaultPreferences: GamePreferences = {
            lists: [
                {
                    id: 'favorites',
                    name: 'My Favorites',
                    games: [],
                    createdAt: new Date().toISOString(),
                    updatedAt: new Date().toISOString(),
                    type: this.isGuest ? 'guest' : 'user',
                },
                {
                    id: 'wishlist',
                    name: 'Wishlist',
                    games: [],
                    createdAt: new Date().toISOString(),
                    updatedAt: new Date().toISOString(),
                    type: this.isGuest ? 'guest' : 'user',
                },
            ],
            favoriteGenres: [],
            recentlyViewed: [],
            lastActivity: new Date().toISOString(),
        };

        try {
            const stored = Cookies.get(PREFERENCES_COOKIE_KEY);
            if (stored) {
                const parsed = JSON.parse(stored);

                if (this.isGuest && parsed.lastActivity) {
                    const lastActivity = new Date(parsed.lastActivity);
                    const now = new Date();
                    const diffMinutes =
                        (now.getTime() - lastActivity.getTime()) / (1000 * 60);

                    if (diffMinutes > GUEST_SESSION_DURATION) {
                        return defaultPreferences;
                    }
                }

                return { ...defaultPreferences, ...parsed };
            }
        } catch (error) {
            console.warn('Failed to load user preferences:', error);
        }

        return defaultPreferences;
    }

    private savePreferences(): void {
        this.preferences.lastActivity = new Date().toISOString();

        try {
            const cookieOptions: Cookies.CookieAttributes = {
                expires: this.isGuest
                    ? GUEST_SESSION_DURATION / (60 * 24)
                    : PERMANENT_DURATION,
                secure: window.location.protocol === 'https:',
                sameSite: 'lax',
            };

            Cookies.set(
                PREFERENCES_COOKIE_KEY,
                JSON.stringify(this.preferences),
                cookieOptions,
            );

            // Dispatch custom event for cross-component reactivity
            window.dispatchEvent(new CustomEvent('preferences-updated'));
        } catch (error) {
            console.error('Failed to save user preferences:', error);
        }
    }

    getPreferences(): GamePreferences {
        return { ...this.preferences };
    }

    createList(name: string): UserGameList {
        const newList: UserGameList = {
            id: `list_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`,
            name,
            games: [],
            createdAt: new Date().toISOString(),
            updatedAt: new Date().toISOString(),
            type: this.isGuest ? 'guest' : 'user',
        };

        this.preferences.lists.push(newList);
        this.savePreferences();
        return newList;
    }

    getLists(): UserGameList[] {
        return this.preferences.lists;
    }

    getList(listId: string): UserGameList | undefined {
        return this.preferences.lists.find((list) => list.id === listId);
    }

    addGameToList(listId: string, gameId: number): boolean {
        const list = this.getList(listId);
        if (!list) return false;

        if (!list.games.includes(gameId)) {
            list.games.push(gameId);
            list.updatedAt = new Date().toISOString();
            this.savePreferences();
            return true;
        }
        return false;
    }

    removeGameFromList(listId: string, gameId: number): boolean {
        const list = this.getList(listId);
        if (!list) return false;

        const index = list.games.indexOf(gameId);
        if (index > -1) {
            list.games.splice(index, 1);
            list.updatedAt = new Date().toISOString();
            this.savePreferences();
            return true;
        }
        return false;
    }

    isGameInList(listId: string, gameId: number): boolean {
        const list = this.getList(listId);
        return list ? list.games.includes(gameId) : false;
    }

    deleteList(listId: string): boolean {
        if (listId === 'favorites' || listId === 'wishlist') return false;

        const index = this.preferences.lists.findIndex(
            (list) => list.id === listId,
        );
        if (index > -1) {
            this.preferences.lists.splice(index, 1);
            this.savePreferences();
            return true;
        }
        return false;
    }

    renameList(listId: string, newName: string): boolean {
        const list = this.getList(listId);
        if (!list) return false;

        list.name = newName;
        list.updatedAt = new Date().toISOString();
        this.savePreferences();
        return true;
    }

    addToRecentlyViewed(gameId: number): void {
        this.preferences.recentlyViewed =
            this.preferences.recentlyViewed.filter((id) => id !== gameId);

        this.preferences.recentlyViewed.unshift(gameId);
        this.preferences.recentlyViewed = this.preferences.recentlyViewed.slice(
            0,
            50,
        );

        this.savePreferences();
    }

    getRecentlyViewed(): number[] {
        return this.preferences.recentlyViewed;
    }

    addFavoriteGenre(genre: string): void {
        if (!this.preferences.favoriteGenres.includes(genre)) {
            this.preferences.favoriteGenres.push(genre);
            this.savePreferences();
        }
    }

    removeFavoriteGenre(genre: string): void {
        const index = this.preferences.favoriteGenres.indexOf(genre);
        if (index > -1) {
            this.preferences.favoriteGenres.splice(index, 1);
            this.savePreferences();
        }
    }

    getFavoriteGenres(): string[] {
        return this.preferences.favoriteGenres;
    }

    clearAllData(): void {
        Cookies.remove(PREFERENCES_COOKIE_KEY);
        this.preferences = this.loadPreferences();
    }

    exportPreferences(): string {
        return JSON.stringify(this.preferences, null, 2);
    }

    importPreferences(data: string): boolean {
        try {
            const imported = JSON.parse(data);
            this.preferences = { ...this.preferences, ...imported };
            this.savePreferences();
            return true;
        } catch (error) {
            console.error('Failed to import preferences:', error);
            return false;
        }
    }

    getSessionInfo(): {
        isGuest: boolean;
        expiresAt?: string;
        duration: string;
    } {
        const info = {
            isGuest: this.isGuest,
            duration: this.isGuest
                ? `${GUEST_SESSION_DURATION} minutes`
                : 'Permanent',
        };

        if (this.isGuest && this.preferences.lastActivity) {
            const lastActivity = new Date(this.preferences.lastActivity);
            const expiresAt = new Date(
                lastActivity.getTime() + GUEST_SESSION_DURATION * 60 * 1000,
            );
            return { ...info, expiresAt: expiresAt.toISOString() };
        }

        return info;
    }
}

let guestPreferences: UserPreferencesManager | null = null;
let userPreferences: UserPreferencesManager | null = null;

export const getPreferencesManager = (
    isAuthenticated: boolean = false,
): UserPreferencesManager => {
    if (isAuthenticated) {
        if (!userPreferences) {
            userPreferences = new UserPreferencesManager(true);
        }
        return userPreferences;
    } else {
        if (!guestPreferences) {
            guestPreferences = new UserPreferencesManager(false);
        }
        return guestPreferences;
    }
};

export const useUserPreferences = (isAuthenticated: boolean = false) => {
    const manager = getPreferencesManager(isAuthenticated);
    const [prefs, setPrefs] = useState<GamePreferences>(
        manager.getPreferences(),
    );

    useEffect(() => {
        const handleUpdate = () => {
            setPrefs(manager.getPreferences());
        };

        window.addEventListener('preferences-updated', handleUpdate);
        return () =>
            window.removeEventListener('preferences-updated', handleUpdate);
    }, [manager]);

    return {
        lists: prefs.lists,
        recentlyViewed: prefs.recentlyViewed,
        favoriteGenres: prefs.favoriteGenres,

        createList: (name: string) => manager.createList(name),
        getLists: () => prefs.lists,
        getList: (listId: string) => prefs.lists.find((l) => l.id === listId),
        addGameToList: (listId: string, gameId: number) =>
            manager.addGameToList(listId, gameId),
        removeGameFromList: (listId: string, gameId: number) =>
            manager.removeGameFromList(listId, gameId),
        isGameInList: (listId: string, gameId: number) => {
            const list = prefs.lists.find((l) => l.id === listId);
            return list ? list.games.includes(gameId) : false;
        },
        deleteList: (listId: string) => manager.deleteList(listId),
        renameList: (listId: string, newName: string) =>
            manager.renameList(listId, newName),

        addToRecentlyViewed: (gameId: number) =>
            manager.addToRecentlyViewed(gameId),
        getRecentlyViewed: () => prefs.recentlyViewed,

        addFavoriteGenre: (genre: string) => manager.addFavoriteGenre(genre),
        removeFavoriteGenre: (genre: string) =>
            manager.removeFavoriteGenre(genre),
        getFavoriteGenres: () => prefs.favoriteGenres,

        clearAllData: () => manager.clearAllData(),
        exportPreferences: () => manager.exportPreferences(),
        importPreferences: (data: string) => manager.importPreferences(data),
        getSessionInfo: () => manager.getSessionInfo(),
    };
};
