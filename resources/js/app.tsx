import '../css/app.css';

import { createInertiaApp } from '@inertiajs/react';
import { resolvePageComponent } from 'laravel-vite-plugin/inertia-helpers';
import { StrictMode } from 'react';
import { createRoot } from 'react-dom/client';
import { TransitionProvider } from './components/transition/TransitionProvider';
import { initializeTheme } from './hooks/use-appearance';

// Extended type for View Transitions API - patching for TS
declare global {
    interface Document {
        // @ts-ignore
        startViewTransition?: (callback: () => Promise<void> | void) => {
            ready: Promise<void>;
            finished: Promise<void>;
            updateCallbackDone: Promise<void>;
        };
    }
}

// Extend CSSProperties to include viewTransitionName
import 'react';
declare module 'react' {
    interface CSSProperties {
        viewTransitionName?: string;
    }
}

const appName = import.meta.env.VITE_APP_NAME || 'Laravel';

createInertiaApp({
    title: (title) => (title ? `${title} - ${appName}` : appName),
    resolve: (name) =>
        resolvePageComponent(
            `./pages/${name}.tsx`,
            import.meta.glob('./pages/**/*.tsx'),
        ),
    setup({ el, App, props }) {
        const root = createRoot(el);

        // Initial render
        root.render(
            <StrictMode>
                <TransitionProvider>
                    <App {...props} />
                </TransitionProvider>
            </StrictMode>,
        );
    },
    progress: {
        color: '#4B5563',
    },
});

// This will set light / dark mode on load...
initializeTheme();
