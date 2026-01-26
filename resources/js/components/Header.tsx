import { Avatar, AvatarFallback, AvatarImage } from '@/components/ui/avatar';
import {
    DropdownMenu,
    DropdownMenuContent,
    DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu';
import { UserMenuContent } from '@/components/user-menu-content';
import { useInitials } from '@/hooks/use-initials';
import { compare, dashboard, home, login, register } from '@/routes';
import { type SharedData } from '@/types';
import { Link, usePage } from '@inertiajs/react';
import { Bell } from 'lucide-react';

interface NavLink {
    name: string;
    href: string;
    icon?: string;
}

export default function Header() {
    const { url, props } = usePage<SharedData>();
    const { auth } = props;
    const getInitials = useInitials();

    const navigation: NavLink[] = [
        { name: 'Dashboard', href: dashboard.url() },
        { name: 'Compare', href: compare.url() },
        { name: 'Catalogue', href: '/games' },
    ];

    const isActive = (href: string) => {
        if (href === '/' && url === '/') return true;
        if (href !== '/' && url.startsWith(href)) return true;
        return false;
    };

    return (
        <header className="fixed top-0 z-50 w-full px-4 py-4 sm:px-6 lg:px-12">
            <nav className="mx-auto flex max-w-7xl items-center justify-between rounded-full border border-white/5 bg-[#050505]/60 px-5 py-2.5 shadow-2xl backdrop-blur-2xl transition-all hover:bg-[#050505]/80">
                {/* Logo */}
                <Link
                    href={home.url()}
                    className="flex items-center space-x-3 transition-transform hover:scale-105"
                >
                    <img
                        src="/GC Landing Page Logo.png"
                        alt="GC"
                        className="h-9 w-9 rounded-xl shadow-lg ring-1 ring-white/10"
                    />
                    <span className="hidden text-xs font-bold tracking-[0.3em] text-white uppercase sm:block">
                        Game Compare
                    </span>
                </Link>

                {/* Navigation */}
                <div className="flex items-center gap-2">
                    <nav className="flex items-center gap-1">
                        {navigation.map((item) => (
                            <Link
                                key={item.name}
                                href={item.href}
                                className={`rounded-full px-4 py-1.5 text-[10px] font-bold tracking-[0.2em] uppercase transition-all ${
                                    isActive(item.href)
                                        ? 'bg-blue-500 text-white shadow-lg shadow-blue-500/25'
                                        : 'text-white/60 hover:bg-white/5 hover:text-white'
                                }`}
                            >
                                {item.name}
                            </Link>
                        ))}
                    </nav>

                    <div className="mx-2 ml-2 hidden h-4 w-px bg-white/10 sm:block" />

                    <Link
                        href={home.url()}
                        className="hidden rounded-full border border-white/10 bg-white/5 px-4 py-1.5 text-[10px] font-bold tracking-[0.2em] text-white uppercase transition-all hover:bg-white/10 sm:block"
                    >
                        Home
                    </Link>

                    <div className="mx-2 ml-2 h-4 w-px bg-white/10" />

                    {/* Alert Me Button */}
                    <button className="hidden items-center gap-2 rounded-full border border-blue-500/20 bg-blue-500/10 px-4 py-1.5 text-[10px] font-bold tracking-[0.2em] text-blue-400 uppercase transition-all hover:bg-blue-500/20 lg:flex">
                        <Bell className="h-3.5 w-3.5" />
                        Alert Me
                    </button>

                    {/* User Profile / Auth */}
                    <div className="flex items-center">
                        {auth.user ? (
                            <DropdownMenu>
                                <DropdownMenuTrigger asChild>
                                    <button className="group flex items-center space-x-3 rounded-full border border-white/5 bg-white/5 p-1 pr-4 transition-all hover:bg-white/10">
                                        <Avatar className="h-8 w-8 border border-white/20 shadow-inner transition-colors group-hover:border-blue-500/50">
                                            <AvatarImage
                                                src={
                                                    auth.user.avatar ||
                                                    `https://api.dicebear.com/7.x/avataaars/svg?seed=${auth.user.name}`
                                                }
                                                className="object-cover"
                                            />
                                            <AvatarFallback className="bg-gradient-to-br from-blue-600 to-indigo-700 text-[10px] font-black text-white uppercase">
                                                {getInitials(auth.user.name)}
                                            </AvatarFallback>
                                        </Avatar>
                                        <div className="flex flex-col items-start">
                                            <span className="text-[10px] font-bold tracking-widest text-white/90 uppercase group-hover:text-white">
                                                {auth.user.name.split(' ')[0]}
                                            </span>
                                            <span className="text-[8px] font-medium tracking-tight text-blue-400 opacity-0 transition-opacity group-hover:opacity-100">
                                                Titan Rank
                                            </span>
                                        </div>
                                    </button>
                                </DropdownMenuTrigger>
                                <DropdownMenuContent
                                    className="w-56 border-white/10 bg-[#0c0c0c]/95 text-white backdrop-blur-xl"
                                    align="end"
                                >
                                    <UserMenuContent user={auth.user} />
                                </DropdownMenuContent>
                            </DropdownMenu>
                        ) : (
                            <div className="flex items-center gap-3">
                                <Link
                                    href={login().url}
                                    className="text-[10px] font-bold tracking-[0.2em] text-white/60 uppercase transition-all hover:text-white"
                                >
                                    Log in
                                </Link>
                                <Link
                                    href={register().url}
                                    className="rounded-full bg-blue-500 px-4 py-1.5 text-[10px] font-bold tracking-[0.2em] text-white uppercase shadow-lg shadow-blue-500/20 transition-all hover:bg-blue-400"
                                >
                                    Join
                                </Link>
                            </div>
                        )}
                    </div>
                </div>
            </nav>
        </header>
    );
}
