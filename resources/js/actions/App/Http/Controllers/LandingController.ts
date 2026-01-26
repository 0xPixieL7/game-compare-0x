import { queryParams, type RouteQueryOptions, type RouteDefinition, type RouteFormDefinition } from './../../../../wayfinder'
/**
* @see \App\Http\Controllers\LandingController::index
* @see app/Http/Controllers/LandingController.php:33
* @route '//127.0.0.1:8000/'
*/
export const index = (options?: RouteQueryOptions): RouteDefinition<'get'> => ({
    url: index.url(options),
    method: 'get',
})

index.definition = {
    methods: ["get","head"],
    url: '//127.0.0.1:8000/',
} satisfies RouteDefinition<["get","head"]>

/**
* @see \App\Http\Controllers\LandingController::index
* @see app/Http/Controllers/LandingController.php:33
* @route '//127.0.0.1:8000/'
*/
index.url = (options?: RouteQueryOptions) => {
    return index.definition.url + queryParams(options)
}

/**
* @see \App\Http\Controllers\LandingController::index
* @see app/Http/Controllers/LandingController.php:33
* @route '//127.0.0.1:8000/'
*/
index.get = (options?: RouteQueryOptions): RouteDefinition<'get'> => ({
    url: index.url(options),
    method: 'get',
})

/**
* @see \App\Http\Controllers\LandingController::index
* @see app/Http/Controllers/LandingController.php:33
* @route '//127.0.0.1:8000/'
*/
index.head = (options?: RouteQueryOptions): RouteDefinition<'head'> => ({
    url: index.url(options),
    method: 'head',
})

/**
* @see \App\Http\Controllers\LandingController::index
* @see app/Http/Controllers/LandingController.php:33
* @route '//127.0.0.1:8000/'
*/
const indexForm = (options?: RouteQueryOptions): RouteFormDefinition<'get'> => ({
    action: index.url(options),
    method: 'get',
})

/**
* @see \App\Http\Controllers\LandingController::index
* @see app/Http/Controllers/LandingController.php:33
* @route '//127.0.0.1:8000/'
*/
indexForm.get = (options?: RouteQueryOptions): RouteFormDefinition<'get'> => ({
    action: index.url(options),
    method: 'get',
})

/**
* @see \App\Http\Controllers\LandingController::index
* @see app/Http/Controllers/LandingController.php:33
* @route '//127.0.0.1:8000/'
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
* @see \App\Http\Controllers\LandingController::debugSpotlight
* @see app/Http/Controllers/LandingController.php:122
* @route '//127.0.0.1:8000/api/debug/spotlight'
*/
export const debugSpotlight = (options?: RouteQueryOptions): RouteDefinition<'get'> => ({
    url: debugSpotlight.url(options),
    method: 'get',
})

debugSpotlight.definition = {
    methods: ["get","head"],
    url: '//127.0.0.1:8000/api/debug/spotlight',
} satisfies RouteDefinition<["get","head"]>

/**
* @see \App\Http\Controllers\LandingController::debugSpotlight
* @see app/Http/Controllers/LandingController.php:122
* @route '//127.0.0.1:8000/api/debug/spotlight'
*/
debugSpotlight.url = (options?: RouteQueryOptions) => {
    return debugSpotlight.definition.url + queryParams(options)
}

/**
* @see \App\Http\Controllers\LandingController::debugSpotlight
* @see app/Http/Controllers/LandingController.php:122
* @route '//127.0.0.1:8000/api/debug/spotlight'
*/
debugSpotlight.get = (options?: RouteQueryOptions): RouteDefinition<'get'> => ({
    url: debugSpotlight.url(options),
    method: 'get',
})

/**
* @see \App\Http\Controllers\LandingController::debugSpotlight
* @see app/Http/Controllers/LandingController.php:122
* @route '//127.0.0.1:8000/api/debug/spotlight'
*/
debugSpotlight.head = (options?: RouteQueryOptions): RouteDefinition<'head'> => ({
    url: debugSpotlight.url(options),
    method: 'head',
})

/**
* @see \App\Http\Controllers\LandingController::debugSpotlight
* @see app/Http/Controllers/LandingController.php:122
* @route '//127.0.0.1:8000/api/debug/spotlight'
*/
const debugSpotlightForm = (options?: RouteQueryOptions): RouteFormDefinition<'get'> => ({
    action: debugSpotlight.url(options),
    method: 'get',
})

/**
* @see \App\Http\Controllers\LandingController::debugSpotlight
* @see app/Http/Controllers/LandingController.php:122
* @route '//127.0.0.1:8000/api/debug/spotlight'
*/
debugSpotlightForm.get = (options?: RouteQueryOptions): RouteFormDefinition<'get'> => ({
    action: debugSpotlight.url(options),
    method: 'get',
})

/**
* @see \App\Http\Controllers\LandingController::debugSpotlight
* @see app/Http/Controllers/LandingController.php:122
* @route '//127.0.0.1:8000/api/debug/spotlight'
*/
debugSpotlightForm.head = (options?: RouteQueryOptions): RouteFormDefinition<'get'> => ({
    action: debugSpotlight.url({
        [options?.mergeQuery ? 'mergeQuery' : 'query']: {
            _method: 'HEAD',
            ...(options?.query ?? options?.mergeQuery ?? {}),
        }
    }),
    method: 'get',
})

debugSpotlight.form = debugSpotlightForm

const LandingController = { index, debugSpotlight }

export default LandingController