import { queryParams, type RouteQueryOptions, type RouteDefinition, applyUrlDefaults } from './../../wayfinder'
/**
* @see \App\Http\Controllers\DashboardController::show
* @see app/Http/Controllers/DashboardController.php:15
* @route '/dashboard/{gameId}'
*/
export const show = (args: { gameId: string | number } | [gameId: string | number ] | string | number, options?: RouteQueryOptions): RouteDefinition<'get'> => ({
    url: show.url(args, options),
    method: 'get',
})

show.definition = {
    methods: ["get","head"],
    url: '/dashboard/{gameId}',
} satisfies RouteDefinition<["get","head"]>

/**
* @see \App\Http\Controllers\DashboardController::show
* @see app/Http/Controllers/DashboardController.php:15
* @route '/dashboard/{gameId}'
*/
show.url = (args: { gameId: string | number } | [gameId: string | number ] | string | number, options?: RouteQueryOptions) => {
    if (typeof args === 'string' || typeof args === 'number') {
        args = { gameId: args }
    }

    if (Array.isArray(args)) {
        args = {
            gameId: args[0],
        }
    }

    args = applyUrlDefaults(args)

    const parsedArgs = {
        gameId: args.gameId,
    }

    return show.definition.url
            .replace('{gameId}', parsedArgs.gameId.toString())
            .replace(/\/+$/, '') + queryParams(options)
}

/**
* @see \App\Http\Controllers\DashboardController::show
* @see app/Http/Controllers/DashboardController.php:15
* @route '/dashboard/{gameId}'
*/
show.get = (args: { gameId: string | number } | [gameId: string | number ] | string | number, options?: RouteQueryOptions): RouteDefinition<'get'> => ({
    url: show.url(args, options),
    method: 'get',
})

/**
* @see \App\Http\Controllers\DashboardController::show
* @see app/Http/Controllers/DashboardController.php:15
* @route '/dashboard/{gameId}'
*/
show.head = (args: { gameId: string | number } | [gameId: string | number ] | string | number, options?: RouteQueryOptions): RouteDefinition<'head'> => ({
    url: show.url(args, options),
    method: 'head',
})

const dashboard = {
    show: Object.assign(show, show),
}

export default dashboard