import {
    DropdownMenu,
    DropdownMenuContent,
    DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu';
import {
    SidebarMenu,
    SidebarMenuButton,
    SidebarMenuItem,
    useSidebar,
} from '@/components/ui/sidebar';
import { UserInfo } from '@/components/user-info';
import { UserMenuContent } from '@/components/user-menu-content';
import { useIsMobile } from '@/hooks/use-mobile';
import { type SharedData } from '@/types';
import { Link, usePage } from '@inertiajs/react';
import { ChevronsUpDown } from 'lucide-react';

export function NavUser() {
    const { auth } = usePage<SharedData>().props;
    const { state } = useSidebar();
    const isMobile = useIsMobile();

    const user = auth.user;

    const navUserComponent = user ? (
        <DropdownMenu>
            <DropdownMenuTrigger asChild>
                <SidebarMenuButton
                    size="lg"
                    className="group text-sidebar-accent-foreground data-[state=open]:bg-sidebar-accent"
                    data-test="sidebar-menu-button"
                >
                    <UserInfo user={user} />
                    <ChevronsUpDown className="ml-auto size-4" />
                </SidebarMenuButton>
            </DropdownMenuTrigger>
            <DropdownMenuContent
                className="w-(--radix-dropdown-menu-trigger-width) min-w-56 rounded-lg"
                align="end"
                side={
                    isMobile
                        ? 'bottom'
                        : state === 'collapsed'
                          ? 'left'
                          : 'bottom'
                }
            >
                <UserMenuContent user={user} />
            </DropdownMenuContent>
        </DropdownMenu>
    ) : (
        <Link
            href="/login"
            className="flex items-center gap-2 p-2 text-sm font-medium text-sidebar-foreground hover:bg-sidebar-accent hover:text-sidebar-accent-foreground"
        >
            <ChevronsUpDown className="size-4" />
            <span>Login / Register</span>
        </Link>
    );

    return (
        <SidebarMenu>
            <SidebarMenuItem>{navUserComponent}</SidebarMenuItem>
        </SidebarMenu>
    );
}
