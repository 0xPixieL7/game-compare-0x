import { Link, usePage } from '@inertiajs/react'
import { home, dashboard, compare } from '@/routes'

interface NavLink {
    name: string
    href: string
    icon?: string
}

export default function Header() {
    const { url } = usePage()

    const navigation: NavLink[] = [
        { name: 'Home', href: home.url() },
        { name: 'Dashboard', href: dashboard.url() },
        { name: 'Games', href: '/games' },
        { name: 'Compare', href: compare.url() },
    ]

    const isActive = (href: string) => {
        if (href === '/' && url === '/') return true
        if (href !== '/' && url.startsWith(href)) return true
        return false
    }

    return (
        <header className="bg-black/90 backdrop-blur-sm border-b border-white/10 sticky top-0 z-50">
            <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
                <div className="flex justify-between items-center h-16">
                    {/* Logo */}
                    <Link
                        href={home.url()}
                        className="flex items-center space-x-2"
                    >
                        <div className="w-8 h-8 bg-gradient-to-r from-blue-500 to-purple-600 rounded-lg flex items-center justify-center">
                            <span className="text-white font-bold text-sm">GC</span>
                        </div>
                        <span className="text-white font-semibold text-lg hidden sm:block">
                            Game Compare
                        </span>
                    </Link>

                    {/* Navigation */}
                    <nav className="flex space-x-1">
                        {navigation.map((item) => (
                            <Link
                                key={item.name}
                                href={item.href}
                                className={`px-3 py-2 rounded-md text-sm font-medium transition-colors ${
                                    isActive(item.href)
                                        ? 'bg-white/10 text-white'
                                        : 'text-white/70 hover:text-white hover:bg-white/5'
                                }`}
                            >
                                {item.name}
                            </Link>
                        ))}
                    </nav>
                </div>
            </div>
        </header>
    )
}