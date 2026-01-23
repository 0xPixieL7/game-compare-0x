import { wayfinder } from '@laravel/vite-plugin-wayfinder';
import tailwindcss from '@tailwindcss/vite';
import react from '@vitejs/plugin-react';
import laravel from 'laravel-vite-plugin';
import { defineConfig } from 'vite';

const isProduction = process.env.NODE_ENV === 'production';

export default defineConfig({
    server: {
        host: '127.0.0.1',
    },
    build: {
        chunkSizeWarningLimit: 650,
        manifest: 'manifest.json',
        outDir: 'public/build',
        rollupOptions: {
            input: ['resources/css/app.css', 'resources/js/app.tsx'],
            output: {
                manualChunks(id) {
                    if (!id.includes('node_modules')) {
                        return;
                    }

                    if (id.includes('apexcharts')) {
                        return 'charts';
                    }

                    if (id.includes('@radix-ui')) {
                        return 'radix';
                    }

                    if (
                        id.includes('lucide-react') ||
                        id.includes('class-variance-authority') ||
                        id.includes('clsx') ||
                        id.includes('tailwind-merge')
                    ) {
                        return 'ui';
                    }

                    return 'vendor';
                },
            },
        },
    },
    plugins: [
        laravel({
            input: ['resources/css/app.css', 'resources/js/app.tsx'],
            ssr: 'resources/js/ssr.tsx',
            refresh: true,
        }),
        react({
            babel: {
                plugins: ['babel-plugin-react-compiler'],
            },
        }),
        tailwindcss(),
        ...(isProduction
            ? []
            : [
                wayfinder({
                    formVariants: true,
                }),
            ]),
    ],
    esbuild: {
        jsx: 'automatic',
    },
});
