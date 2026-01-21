import { queryParams, type RouteQueryOptions, type RouteDefinition } from './../../../../wayfinder'
/**
* @see \App\Http\Controllers\CompareController::index
* @see app/Http/Controllers/CompareController.php:17
* @route '/compare'
*/
export const index = (options?: RouteQueryOptions): RouteDefinition<'get'> => ({
    url: index.url(options),
    method: 'get',
})

index.definition = {
    methods: ["get","head"],
    url: '/compare',
} satisfies RouteDefinition<["get","head"]>

/**
* @see \App\Http\Controllers\CompareController::index
* @see app/Http/Controllers/CompareController.php:17
* @route '/compare'
*/
index.url = (options?: RouteQueryOptions) => {
    return index.definition.url + queryParams(options)
}

/**
* @see \App\Http\Controllers\CompareController::index
* @see app/Http/Controllers/CompareController.php:17
* @route '/compare'
*/
index.get = (options?: RouteQueryOptions): RouteDefinition<'get'> => ({
    url: index.url(options),
    method: 'get',
})

/**
* @see \App\Http\Controllers\CompareController::index
* @see app/Http/Controllers/CompareController.php:17
* @route '/compare'
*/
index.head = (options?: RouteQueryOptions): RouteDefinition<'head'> => ({
    url: index.url(options),
    method: 'head',
})

/**
* @see \App\Http\Controllers\CompareController::stats
* @see app/Http/Controllers/CompareController.php:61
* @route '/compare/stats'
*/
export const stats = (options?: RouteQueryOptions): RouteDefinition<'get'> => ({
    url: stats.url(options),
    method: 'get',
})

stats.definition = {
    methods: ["get","head"],
    url: '/compare/stats',
} satisfies RouteDefinition<["get","head"]>

/**
* @see \App\Http\Controllers\CompareController::stats
* @see app/Http/Controllers/CompareController.php:61
* @route '/compare/stats'
*/
stats.url = (options?: RouteQueryOptions) => {
    return stats.definition.url + queryParams(options)
}

/**
* @see \App\Http\Controllers\CompareController::stats
* @see app/Http/Controllers/CompareController.php:61
* @route '/compare/stats'
*/
stats.get = (options?: RouteQueryOptions): RouteDefinition<'get'> => ({
    url: stats.url(options),
    method: 'get',
})

/**
* @see \App\Http\Controllers\CompareController::stats
* @see app/Http/Controllers/CompareController.php:61
* @route '/compare/stats'
*/
stats.head = (options?: RouteQueryOptions): RouteDefinition<'head'> => ({
    url: stats.url(options),
    method: 'head',
})

/**
* @see \App\Http\Controllers\CompareController::entries
* @see app/Http/Controllers/CompareController.php:66
* @route '/compare/entries'
*/
export const entries = (options?: RouteQueryOptions): RouteDefinition<'get'> => ({
    url: entries.url(options),
    method: 'get',
})

entries.definition = {
    methods: ["get","head"],
    url: '/compare/entries',
} satisfies RouteDefinition<["get","head"]>

/**
* @see \App\Http\Controllers\CompareController::entries
* @see app/Http/Controllers/CompareController.php:66
* @route '/compare/entries'
*/
entries.url = (options?: RouteQueryOptions) => {
    return entries.definition.url + queryParams(options)
}

/**
* @see \App\Http\Controllers\CompareController::entries
* @see app/Http/Controllers/CompareController.php:66
* @route '/compare/entries'
*/
entries.get = (options?: RouteQueryOptions): RouteDefinition<'get'> => ({
    url: entries.url(options),
    method: 'get',
})

/**
* @see \App\Http\Controllers\CompareController::entries
* @see app/Http/Controllers/CompareController.php:66
* @route '/compare/entries'
*/
entries.head = (options?: RouteQueryOptions): RouteDefinition<'head'> => ({
    url: entries.url(options),
    method: 'head',
})

/**
* @see \App\Http\Controllers\CompareController::spotlight
* @see app/Http/Controllers/CompareController.php:74
* @route '/compare/spotlight'
*/
export const spotlight = (options?: RouteQueryOptions): RouteDefinition<'get'> => ({
    url: spotlight.url(options),
    method: 'get',
})

spotlight.definition = {
    methods: ["get","head"],
    url: '/compare/spotlight',
} satisfies RouteDefinition<["get","head"]>

/**
* @see \App\Http\Controllers\CompareController::spotlight
* @see app/Http/Controllers/CompareController.php:74
* @route '/compare/spotlight'
*/
spotlight.url = (options?: RouteQueryOptions) => {
    return spotlight.definition.url + queryParams(options)
}

/**
* @see \App\Http\Controllers\CompareController::spotlight
* @see app/Http/Controllers/CompareController.php:74
* @route '/compare/spotlight'
*/
spotlight.get = (options?: RouteQueryOptions): RouteDefinition<'get'> => ({
    url: spotlight.url(options),
    method: 'get',
})

/**
* @see \App\Http\Controllers\CompareController::spotlight
* @see app/Http/Controllers/CompareController.php:74
* @route '/compare/spotlight'
*/
spotlight.head = (options?: RouteQueryOptions): RouteDefinition<'head'> => ({
    url: spotlight.url(options),
    method: 'head',
})

const CompareController = { index, stats, entries, spotlight }

export default CompareController