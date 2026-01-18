import { queryParams, type RouteQueryOptions, type RouteDefinition, type RouteFormDefinition, applyUrlDefaults } from './../../wayfinder'
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

/**
* @see \App\Http\Controllers\DashboardController::show
* @see app/Http/Controllers/DashboardController.php:15
* @route '/dashboard/{gameId}'
*/
const showForm = (args: { gameId: string | number } | [gameId: string | number ] | string | number, options?: RouteQueryOptions): RouteFormDefinition<'get'> => ({
    action: show.url(args, options),
    method: 'get',
})

/**
* @see \App\Http\Controllers\DashboardController::show
* @see app/Http/Controllers/DashboardController.php:15
* @route '/dashboard/{gameId}'
*/
showForm.get = (args: { gameId: string | number } | [gameId: string | number ] | string | number, options?: RouteQueryOptions): RouteFormDefinition<'get'> => ({
    action: show.url(args, options),
    method: 'get',
})

/**
* @see \App\Http\Controllers\DashboardController::show
* @see app/Http/Controllers/DashboardController.php:15
* @route '/dashboard/{gameId}'
*/
showForm.head = (args: { gameId: string | number } | [gameId: string | number ] | string | number, options?: RouteQueryOptions): RouteFormDefinition<'get'> => ({
    action: show.url(args, {
        [options?.mergeQuery ? 'mergeQuery' : 'query']: {
            _method: 'HEAD',
            ...(options?.query ?? options?.mergeQuery ?? {}),
        }
    }),
    method: 'get',
})

show.form = showForm

const dashboard = {
    show: Object.assign(show, show),
}

export default dashboard