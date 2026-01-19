import { queryParams, type RouteQueryOptions, type RouteDefinition, type RouteFormDefinition } from './../../../../wayfinder'
/**
* @see \App\Http\Controllers\CompareController::index
* @see app/Http/Controllers/CompareController.php:16
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
* @see app/Http/Controllers/CompareController.php:16
* @route '/compare'
*/
index.url = (options?: RouteQueryOptions) => {
    return index.definition.url + queryParams(options)
}

/**
* @see \App\Http\Controllers\CompareController::index
* @see app/Http/Controllers/CompareController.php:16
* @route '/compare'
*/
index.get = (options?: RouteQueryOptions): RouteDefinition<'get'> => ({
    url: index.url(options),
    method: 'get',
})

/**
* @see \App\Http\Controllers\CompareController::index
* @see app/Http/Controllers/CompareController.php:16
* @route '/compare'
*/
index.head = (options?: RouteQueryOptions): RouteDefinition<'head'> => ({
    url: index.url(options),
    method: 'head',
})

/**
* @see \App\Http\Controllers\CompareController::index
* @see app/Http/Controllers/CompareController.php:16
* @route '/compare'
*/
const indexForm = (options?: RouteQueryOptions): RouteFormDefinition<'get'> => ({
    action: index.url(options),
    method: 'get',
})

/**
* @see \App\Http\Controllers\CompareController::index
* @see app/Http/Controllers/CompareController.php:16
* @route '/compare'
*/
indexForm.get = (options?: RouteQueryOptions): RouteFormDefinition<'get'> => ({
    action: index.url(options),
    method: 'get',
})

/**
* @see \App\Http\Controllers\CompareController::index
* @see app/Http/Controllers/CompareController.php:16
* @route '/compare'
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
* @see \App\Http\Controllers\CompareController::stats
* @see app/Http/Controllers/CompareController.php:60
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
* @see app/Http/Controllers/CompareController.php:60
* @route '/compare/stats'
*/
stats.url = (options?: RouteQueryOptions) => {
    return stats.definition.url + queryParams(options)
}

/**
* @see \App\Http\Controllers\CompareController::stats
* @see app/Http/Controllers/CompareController.php:60
* @route '/compare/stats'
*/
stats.get = (options?: RouteQueryOptions): RouteDefinition<'get'> => ({
    url: stats.url(options),
    method: 'get',
})

/**
* @see \App\Http\Controllers\CompareController::stats
* @see app/Http/Controllers/CompareController.php:60
* @route '/compare/stats'
*/
stats.head = (options?: RouteQueryOptions): RouteDefinition<'head'> => ({
    url: stats.url(options),
    method: 'head',
})

/**
* @see \App\Http\Controllers\CompareController::stats
* @see app/Http/Controllers/CompareController.php:60
* @route '/compare/stats'
*/
const statsForm = (options?: RouteQueryOptions): RouteFormDefinition<'get'> => ({
    action: stats.url(options),
    method: 'get',
})

/**
* @see \App\Http\Controllers\CompareController::stats
* @see app/Http/Controllers/CompareController.php:60
* @route '/compare/stats'
*/
statsForm.get = (options?: RouteQueryOptions): RouteFormDefinition<'get'> => ({
    action: stats.url(options),
    method: 'get',
})

/**
* @see \App\Http\Controllers\CompareController::stats
* @see app/Http/Controllers/CompareController.php:60
* @route '/compare/stats'
*/
statsForm.head = (options?: RouteQueryOptions): RouteFormDefinition<'get'> => ({
    action: stats.url({
        [options?.mergeQuery ? 'mergeQuery' : 'query']: {
            _method: 'HEAD',
            ...(options?.query ?? options?.mergeQuery ?? {}),
        }
    }),
    method: 'get',
})

stats.form = statsForm

/**
* @see \App\Http\Controllers\CompareController::entries
* @see app/Http/Controllers/CompareController.php:65
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
* @see app/Http/Controllers/CompareController.php:65
* @route '/compare/entries'
*/
entries.url = (options?: RouteQueryOptions) => {
    return entries.definition.url + queryParams(options)
}

/**
* @see \App\Http\Controllers\CompareController::entries
* @see app/Http/Controllers/CompareController.php:65
* @route '/compare/entries'
*/
entries.get = (options?: RouteQueryOptions): RouteDefinition<'get'> => ({
    url: entries.url(options),
    method: 'get',
})

/**
* @see \App\Http\Controllers\CompareController::entries
* @see app/Http/Controllers/CompareController.php:65
* @route '/compare/entries'
*/
entries.head = (options?: RouteQueryOptions): RouteDefinition<'head'> => ({
    url: entries.url(options),
    method: 'head',
})

/**
* @see \App\Http\Controllers\CompareController::entries
* @see app/Http/Controllers/CompareController.php:65
* @route '/compare/entries'
*/
const entriesForm = (options?: RouteQueryOptions): RouteFormDefinition<'get'> => ({
    action: entries.url(options),
    method: 'get',
})

/**
* @see \App\Http\Controllers\CompareController::entries
* @see app/Http/Controllers/CompareController.php:65
* @route '/compare/entries'
*/
entriesForm.get = (options?: RouteQueryOptions): RouteFormDefinition<'get'> => ({
    action: entries.url(options),
    method: 'get',
})

/**
* @see \App\Http\Controllers\CompareController::entries
* @see app/Http/Controllers/CompareController.php:65
* @route '/compare/entries'
*/
entriesForm.head = (options?: RouteQueryOptions): RouteFormDefinition<'get'> => ({
    action: entries.url({
        [options?.mergeQuery ? 'mergeQuery' : 'query']: {
            _method: 'HEAD',
            ...(options?.query ?? options?.mergeQuery ?? {}),
        }
    }),
    method: 'get',
})

entries.form = entriesForm

/**
* @see \App\Http\Controllers\CompareController::spotlight
* @see app/Http/Controllers/CompareController.php:73
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
* @see app/Http/Controllers/CompareController.php:73
* @route '/compare/spotlight'
*/
spotlight.url = (options?: RouteQueryOptions) => {
    return spotlight.definition.url + queryParams(options)
}

/**
* @see \App\Http\Controllers\CompareController::spotlight
* @see app/Http/Controllers/CompareController.php:73
* @route '/compare/spotlight'
*/
spotlight.get = (options?: RouteQueryOptions): RouteDefinition<'get'> => ({
    url: spotlight.url(options),
    method: 'get',
})

/**
* @see \App\Http\Controllers\CompareController::spotlight
* @see app/Http/Controllers/CompareController.php:73
* @route '/compare/spotlight'
*/
spotlight.head = (options?: RouteQueryOptions): RouteDefinition<'head'> => ({
    url: spotlight.url(options),
    method: 'head',
})

/**
* @see \App\Http\Controllers\CompareController::spotlight
* @see app/Http/Controllers/CompareController.php:73
* @route '/compare/spotlight'
*/
const spotlightForm = (options?: RouteQueryOptions): RouteFormDefinition<'get'> => ({
    action: spotlight.url(options),
    method: 'get',
})

/**
* @see \App\Http\Controllers\CompareController::spotlight
* @see app/Http/Controllers/CompareController.php:73
* @route '/compare/spotlight'
*/
spotlightForm.get = (options?: RouteQueryOptions): RouteFormDefinition<'get'> => ({
    action: spotlight.url(options),
    method: 'get',
})

/**
* @see \App\Http\Controllers\CompareController::spotlight
* @see app/Http/Controllers/CompareController.php:73
* @route '/compare/spotlight'
*/
spotlightForm.head = (options?: RouteQueryOptions): RouteFormDefinition<'get'> => ({
    action: spotlight.url({
        [options?.mergeQuery ? 'mergeQuery' : 'query']: {
            _method: 'HEAD',
            ...(options?.query ?? options?.mergeQuery ?? {}),
        }
    }),
    method: 'get',
})

spotlight.form = spotlightForm

const CompareController = { index, stats, entries, spotlight }

export default CompareController