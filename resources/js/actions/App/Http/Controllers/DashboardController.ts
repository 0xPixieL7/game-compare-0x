import { queryParams, type RouteQueryOptions, type RouteDefinition, type RouteFormDefinition, applyUrlDefaults } from './../../../../wayfinder'
/**
* @see \App\Http\Controllers\DashboardController::index
* @see app/Http/Controllers/DashboardController.php:392
* @route '//127.0.0.1:8000/dashboard'
*/
export const index = (options?: RouteQueryOptions): RouteDefinition<'get'> => ({
    url: index.url(options),
    method: 'get',
})

index.definition = {
    methods: ["get","head"],
    url: '//127.0.0.1:8000/dashboard',
} satisfies RouteDefinition<["get","head"]>

/**
* @see \App\Http\Controllers\DashboardController::index
* @see app/Http/Controllers/DashboardController.php:392
* @route '//127.0.0.1:8000/dashboard'
*/
index.url = (options?: RouteQueryOptions) => {
    return index.definition.url + queryParams(options)
}

/**
* @see \App\Http\Controllers\DashboardController::index
* @see app/Http/Controllers/DashboardController.php:392
* @route '//127.0.0.1:8000/dashboard'
*/
index.get = (options?: RouteQueryOptions): RouteDefinition<'get'> => ({
    url: index.url(options),
    method: 'get',
})

/**
* @see \App\Http\Controllers\DashboardController::index
* @see app/Http/Controllers/DashboardController.php:392
* @route '//127.0.0.1:8000/dashboard'
*/
index.head = (options?: RouteQueryOptions): RouteDefinition<'head'> => ({
    url: index.url(options),
    method: 'head',
})

/**
* @see \App\Http\Controllers\DashboardController::index
* @see app/Http/Controllers/DashboardController.php:392
* @route '//127.0.0.1:8000/dashboard'
*/
const indexForm = (options?: RouteQueryOptions): RouteFormDefinition<'get'> => ({
    action: index.url(options),
    method: 'get',
})

/**
* @see \App\Http\Controllers\DashboardController::index
* @see app/Http/Controllers/DashboardController.php:392
* @route '//127.0.0.1:8000/dashboard'
*/
indexForm.get = (options?: RouteQueryOptions): RouteFormDefinition<'get'> => ({
    action: index.url(options),
    method: 'get',
})

/**
* @see \App\Http\Controllers\DashboardController::index
* @see app/Http/Controllers/DashboardController.php:392
* @route '//127.0.0.1:8000/dashboard'
*/
indexForm.head = (options?: RouteQueryOptions): RouteFormDefinition<'get'> => ({
    action: index.url({
        [options?.mergeQuery ? 'mergeQuery' : 'query']: {
            _method: 'HEAD',
            ...(options?.query ?? options?.mergeQuery ?? {}),
        }
    }),
    method: 'get',
})

index.form = indexForm

/**
* @see \App\Http\Controllers\DashboardController::show
* @see app/Http/Controllers/DashboardController.php:15
* @route '//127.0.0.1:8000/dashboard/{gameId}'
*/
export const show = (args: { gameId: string | number } | [gameId: string | number ] | string | number, options?: RouteQueryOptions): RouteDefinition<'get'> => ({
    url: show.url(args, options),
    method: 'get',
})

show.definition = {
    methods: ["get","head"],
    url: '//127.0.0.1:8000/dashboard/{gameId}',
} satisfies RouteDefinition<["get","head"]>

/**
* @see \App\Http\Controllers\DashboardController::show
* @see app/Http/Controllers/DashboardController.php:15
* @route '//127.0.0.1:8000/dashboard/{gameId}'
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
* @route '//127.0.0.1:8000/dashboard/{gameId}'
*/
show.get = (args: { gameId: string | number } | [gameId: string | number ] | string | number, options?: RouteQueryOptions): RouteDefinition<'get'> => ({
    url: show.url(args, options),
    method: 'get',
})

/**
* @see \App\Http\Controllers\DashboardController::show
* @see app/Http/Controllers/DashboardController.php:15
* @route '//127.0.0.1:8000/dashboard/{gameId}'
*/
show.head = (args: { gameId: string | number } | [gameId: string | number ] | string | number, options?: RouteQueryOptions): RouteDefinition<'head'> => ({
    url: show.url(args, options),
    method: 'head',
})

/**
* @see \App\Http\Controllers\DashboardController::show
* @see app/Http/Controllers/DashboardController.php:15
* @route '//127.0.0.1:8000/dashboard/{gameId}'
*/
const showForm = (args: { gameId: string | number } | [gameId: string | number ] | string | number, options?: RouteQueryOptions): RouteFormDefinition<'get'> => ({
    action: show.url(args, options),
    method: 'get',
})

/**
* @see \App\Http\Controllers\DashboardController::show
* @see app/Http/Controllers/DashboardController.php:15
* @route '//127.0.0.1:8000/dashboard/{gameId}'
*/
showForm.get = (args: { gameId: string | number } | [gameId: string | number ] | string | number, options?: RouteQueryOptions): RouteFormDefinition<'get'> => ({
    action: show.url(args, options),
    method: 'get',
})

/**
* @see \App\Http\Controllers\DashboardController::show
* @see app/Http/Controllers/DashboardController.php:15
* @route '//127.0.0.1:8000/dashboard/{gameId}'
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

const DashboardController = { index, show }

export default DashboardController