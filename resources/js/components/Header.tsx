import { compare, dashboard, home } from '@/routes';
import { Link, usePage } from '@inertiajs/react';

interface NavLink {
    name: string;
    href: string;
    icon?: string;
}

export default function Header() {
    const { url } = usePage();

    const navigation: NavLink[] = [
        { name: 'Home', href: home.url() },
        { name: 'Dashboard', href: dashboard.url() },
        { name: 'Games', href: '/games' },
        { name: 'Compare', href: compare.url() },
    ];

    const isActive = (href: string) => {
        if (href === '/' && url === '/') return true;
        if (href !== '/' && url.startsWith(href)) return true;
        return false;
    };

    return (
        <header className="sticky top-0 z-50 border-b border-white/10 bg-black/90 backdrop-blur-sm">
            <div className="mx-auto max-w-7xl px-4 sm:px-6 lg:px-8">
                <div className="flex h-16 items-center justify-between">
                    {/* Logo */}
                    <Link
                        href={home.url()}
                        className="flex items-center space-x-2"
                    >
                        <div className="flex h-8 w-8 items-center justify-center rounded-lg bg-gradient-to-r from-blue-500 to-purple-600">
                            <span className="text-sm font-bold text-white">
                                GC
                            </span>
                        </div>
                        <span className="hidden text-lg font-semibold text-white sm:block">
                            Game Compare
                        </span>
                    </Link>

                    {/* Navigation */}
                    <nav className="flex space-x-1">
                        {navigation.map((item) => (
                            <Link
                                key={item.name}
                                href={item.href}
                                className={`rounded-md px-3 py-2 text-sm font-medium transition-colors ${
                                    isActive(item.href)
                                        ? 'bg-white/10 text-white'
                                        : 'text-white/70 hover:bg-white/5 hover:text-white'
                                }`}
                            >
                                {item.name}
                            </Link>
                        ))}
                    </nav>
                </div>
            </div>
        </header>
    );
}
