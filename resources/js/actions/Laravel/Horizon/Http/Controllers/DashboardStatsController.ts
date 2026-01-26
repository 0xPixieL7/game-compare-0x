import { queryParams, type RouteQueryOptions, type RouteDefinition, type RouteFormDefinition } from './../../../../../wayfinder'
/**
* @see \Laravel\Horizon\Http\Controllers\DashboardStatsController::index
* @see vendor/laravel/horizon/src/Http/Controllers/DashboardStatsController.php:18
* @route '//127.0.0.1:8000/horizon/api/stats'
*/
export const index = (options?: RouteQueryOptions): RouteDefinition<'get'> => ({
    url: index.url(options),
    method: 'get',
})

index.definition = {
    methods: ["get","head"],
    url: '//127.0.0.1:8000/horizon/api/stats',
} satisfies RouteDefinition<["get","head"]>

/**
* @see \Laravel\Horizon\Http\Controllers\DashboardStatsController::index
* @see vendor/laravel/horizon/src/Http/Controllers/DashboardStatsController.php:18
* @route '//127.0.0.1:8000/horizon/api/stats'
*/
index.url = (options?: RouteQueryOptions) => {
    return index.definition.url + queryParams(options)
}

/**
* @see \Laravel\Horizon\Http\Controllers\DashboardStatsController::index
* @see vendor/laravel/horizon/src/Http/Controllers/DashboardStatsController.php:18
* @route '//127.0.0.1:8000/horizon/api/stats'
*/
index.get = (options?: RouteQueryOptions): RouteDefinition<'get'> => ({
    url: index.url(options),
    method: 'get',
})

/**
* @see \Laravel\Horizon\Http\Controllers\DashboardStatsController::index
* @see vendor/laravel/horizon/src/Http/Controllers/DashboardStatsController.php:18
* @route '//127.0.0.1:8000/horizon/api/stats'
*/
index.head = (options?: RouteQueryOptions): RouteDefinition<'head'> => ({
    url: index.url(options),
    method: 'head',
})

/**
* @see \Laravel\Horizon\Http\Controllers\DashboardStatsController::index
* @see vendor/laravel/horizon/src/Http/Controllers/DashboardStatsController.php:18
* @route '//127.0.0.1:8000/horizon/api/stats'
*/
const indexForm = (options?: RouteQueryOptions): RouteFormDefinition<'get'> => ({
    action: index.url(options),
    method: 'get',
})

/**
* @see \Laravel\Horizon\Http\Controllers\DashboardStatsController::index
* @see vendor/laravel/horizon/src/Http/Controllers/DashboardStatsController.php:18
* @route '//127.0.0.1:8000/horizon/api/stats'
*/
indexForm.get = (options?: RouteQueryOptions): RouteFormDefinition<'get'> => ({
    action: index.url(options),
    method: 'get',
})

/**
* @see \Laravel\Horizon\Http\Controllers\DashboardStatsController::index
* @see vendor/laravel/horizon/src/Http/Controllers/DashboardStatsController.php:18
* @route '//127.0.0.1:8000/horizon/api/stats'
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

const DashboardStatsController = { index }

export default DashboardStatsController