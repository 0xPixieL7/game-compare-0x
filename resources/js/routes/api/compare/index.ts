import { queryParams, type RouteQueryOptions, type RouteDefinition, type RouteFormDefinition } from './../../../wayfinder'
/**
* @see routes/api.php:18
* @route '/api/compare/stats'
*/
export const stats = (options?: RouteQueryOptions): RouteDefinition<'get'> => ({
    url: stats.url(options),
    method: 'get',
})

stats.definition = {
    methods: ["get","head"],
    url: '/api/compare/stats',
} satisfies RouteDefinition<["get","head"]>

/**
* @see routes/api.php:18
* @route '/api/compare/stats'
*/
stats.url = (options?: RouteQueryOptions) => {
    return stats.definition.url + queryParams(options)
}

/**
* @see routes/api.php:18
* @route '/api/compare/stats'
*/
stats.get = (options?: RouteQueryOptions): RouteDefinition<'get'> => ({
    url: stats.url(options),
    method: 'get',
})

/**
* @see routes/api.php:18
* @route '/api/compare/stats'
*/
stats.head = (options?: RouteQueryOptions): RouteDefinition<'head'> => ({
    url: stats.url(options),
    method: 'head',
})

/**
* @see routes/api.php:18
* @route '/api/compare/stats'
*/
const statsForm = (options?: RouteQueryOptions): RouteFormDefinition<'get'> => ({
    action: stats.url(options),
    method: 'get',
})

/**
* @see routes/api.php:18
* @route '/api/compare/stats'
*/
statsForm.get = (options?: RouteQueryOptions): RouteFormDefinition<'get'> => ({
    action: stats.url(options),
    method: 'get',
})

/**
* @see routes/api.php:18
* @route '/api/compare/stats'
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
* @see routes/api.php:26
* @route '/api/compare/entries'
*/
export const entries = (options?: RouteQueryOptions): RouteDefinition<'get'> => ({
    url: entries.url(options),
    method: 'get',
})

entries.definition = {
    methods: ["get","head"],
    url: '/api/compare/entries',
} satisfies RouteDefinition<["get","head"]>

/**
* @see routes/api.php:26
* @route '/api/compare/entries'
*/
entries.url = (options?: RouteQueryOptions) => {
    return entries.definition.url + queryParams(options)
}

/**
* @see routes/api.php:26
* @route '/api/compare/entries'
*/
entries.get = (options?: RouteQueryOptions): RouteDefinition<'get'> => ({
    url: entries.url(options),
    method: 'get',
})

/**
* @see routes/api.php:26
* @route '/api/compare/entries'
*/
entries.head = (options?: RouteQueryOptions): RouteDefinition<'head'> => ({
    url: entries.url(options),
    method: 'head',
})

/**
* @see routes/api.php:26
* @route '/api/compare/entries'
*/
const entriesForm = (options?: RouteQueryOptions): RouteFormDefinition<'get'> => ({
    action: entries.url(options),
    method: 'get',
})

/**
* @see routes/api.php:26
* @route '/api/compare/entries'
*/
entriesForm.get = (options?: RouteQueryOptions): RouteFormDefinition<'get'> => ({
    action: entries.url(options),
    method: 'get',
})

/**
* @see routes/api.php:26
* @route '/api/compare/entries'
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
* @see routes/api.php:35
* @route '/api/compare/spotlight'
*/
export const spotlight = (options?: RouteQueryOptions): RouteDefinition<'get'> => ({
    url: spotlight.url(options),
    method: 'get',
})

spotlight.definition = {
    methods: ["get","head"],
    url: '/api/compare/spotlight',
} satisfies RouteDefinition<["get","head"]>

/**
* @see routes/api.php:35
* @route '/api/compare/spotlight'
*/
spotlight.url = (options?: RouteQueryOptions) => {
    return spotlight.definition.url + queryParams(options)
}

/**
* @see routes/api.php:35
* @route '/api/compare/spotlight'
*/
spotlight.get = (options?: RouteQueryOptions): RouteDefinition<'get'> => ({
    url: spotlight.url(options),
    method: 'get',
})

/**
* @see routes/api.php:35
* @route '/api/compare/spotlight'
*/
spotlight.head = (options?: RouteQueryOptions): RouteDefinition<'head'> => ({
    url: spotlight.url(options),
    method: 'head',
})

/**
* @see routes/api.php:35
* @route '/api/compare/spotlight'
*/
const spotlightForm = (options?: RouteQueryOptions): RouteFormDefinition<'get'> => ({
    action: spotlight.url(options),
    method: 'get',
})

/**
* @see routes/api.php:35
* @route '/api/compare/spotlight'
*/
spotlightForm.get = (options?: RouteQueryOptions): RouteFormDefinition<'get'> => ({
    action: spotlight.url(options),
    method: 'get',
})

/**
* @see routes/api.php:35
* @route '/api/compare/spotlight'
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

const compare = {
    stats: Object.assign(stats, stats),
    entries: Object.assign(entries, entries),
    spotlight: Object.assign(spotlight, spotlight),
}

export default compare