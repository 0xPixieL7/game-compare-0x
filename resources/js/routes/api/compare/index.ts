import { queryParams, type RouteQueryOptions, type RouteDefinition } from './../../../wayfinder'
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

const compare = {
    stats: Object.assign(stats, stats),
    entries: Object.assign(entries, entries),
    spotlight: Object.assign(spotlight, spotlight),
}

export default compare