import { queryParams, type RouteQueryOptions, type RouteDefinition, type RouteFormDefinition } from './../../../../../wayfinder'
/**
* @see \Laravel\Fortify\Http\Controllers\PasswordResetLinkController::create
* @see vendor/laravel/fortify/src/Http/Controllers/PasswordResetLinkController.php:22
* @route '//127.0.0.1:8000/forgot-password'
*/
export const create = (options?: RouteQueryOptions): RouteDefinition<'get'> => ({
    url: create.url(options),
    method: 'get',
})

create.definition = {
    methods: ["get","head"],
    url: '//127.0.0.1:8000/forgot-password',
} satisfies RouteDefinition<["get","head"]>

/**
* @see \Laravel\Fortify\Http\Controllers\PasswordResetLinkController::create
* @see vendor/laravel/fortify/src/Http/Controllers/PasswordResetLinkController.php:22
* @route '//127.0.0.1:8000/forgot-password'
*/
create.url = (options?: RouteQueryOptions) => {
    return create.definition.url + queryParams(options)
}

/**
* @see \Laravel\Fortify\Http\Controllers\PasswordResetLinkController::create
* @see vendor/laravel/fortify/src/Http/Controllers/PasswordResetLinkController.php:22
* @route '//127.0.0.1:8000/forgot-password'
*/
create.get = (options?: RouteQueryOptions): RouteDefinition<'get'> => ({
    url: create.url(options),
    method: 'get',
})

/**
* @see \Laravel\Fortify\Http\Controllers\PasswordResetLinkController::create
* @see vendor/laravel/fortify/src/Http/Controllers/PasswordResetLinkController.php:22
* @route '//127.0.0.1:8000/forgot-password'
*/
create.head = (options?: RouteQueryOptions): RouteDefinition<'head'> => ({
    url: create.url(options),
    method: 'head',
})

/**
* @see \Laravel\Fortify\Http\Controllers\PasswordResetLinkController::create
* @see vendor/laravel/fortify/src/Http/Controllers/PasswordResetLinkController.php:22
* @route '//127.0.0.1:8000/forgot-password'
*/
const createForm = (options?: RouteQueryOptions): RouteFormDefinition<'get'> => ({
    action: create.url(options),
    method: 'get',
})

/**
* @see \Laravel\Fortify\Http\Controllers\PasswordResetLinkController::create
* @see vendor/laravel/fortify/src/Http/Controllers/PasswordResetLinkController.php:22
* @route '//127.0.0.1:8000/forgot-password'
*/
createForm.get = (options?: RouteQueryOptions): RouteFormDefinition<'get'> => ({
    action: create.url(options),
    method: 'get',
})

/**
* @see \Laravel\Fortify\Http\Controllers\PasswordResetLinkController::create
* @see vendor/laravel/fortify/src/Http/Controllers/PasswordResetLinkController.php:22
* @route '//127.0.0.1:8000/forgot-password'
*/
createForm.head = (options?: RouteQueryOptions): RouteFormDefinition<'get'> => ({
    action: create.url({
        [options?.mergeQuery ? 'mergeQuery' : 'query']: {
            _method: 'HEAD',
            ...(options?.query ?? options?.mergeQuery ?? {}),
        }
    }),
    method: 'get',
})

create.form = createForm

/**
* @see \Laravel\Fortify\Http\Controllers\PasswordResetLinkController::store
* @see vendor/laravel/fortify/src/Http/Controllers/PasswordResetLinkController.php:30
* @route '//127.0.0.1:8000/forgot-password'
*/
export const store = (options?: RouteQueryOptions): RouteDefinition<'post'> => ({
    url: store.url(options),
    method: 'post',
})

store.definition = {
    methods: ["post"],
    url: '//127.0.0.1:8000/forgot-password',
} satisfies RouteDefinition<["post"]>

/**
* @see \Laravel\Fortify\Http\Controllers\PasswordResetLinkController::store
* @see vendor/laravel/fortify/src/Http/Controllers/PasswordResetLinkController.php:30
* @route '//127.0.0.1:8000/forgot-password'
*/
store.url = (options?: RouteQueryOptions) => {
    return store.definition.url + queryParams(options)
}

/**
* @see \Laravel\Fortify\Http\Controllers\PasswordResetLinkController::store
* @see vendor/laravel/fortify/src/Http/Controllers/PasswordResetLinkController.php:30
* @route '//127.0.0.1:8000/forgot-password'
*/
store.post = (options?: RouteQueryOptions): RouteDefinition<'post'> => ({
    url: store.url(options),
    method: 'post',
})

/**
* @see \Laravel\Fortify\Http\Controllers\PasswordResetLinkController::store
* @see vendor/laravel/fortify/src/Http/Controllers/PasswordResetLinkController.php:30
* @route '//127.0.0.1:8000/forgot-password'
*/
const storeForm = (options?: RouteQueryOptions): RouteFormDefinition<'post'> => ({
    action: store.url(options),
    method: 'post',
})

/**
* @see \Laravel\Fortify\Http\Controllers\PasswordResetLinkController::store
* @see vendor/laravel/fortify/src/Http/Controllers/PasswordResetLinkController.php:30
* @route '//127.0.0.1:8000/forgot-password'
*/
storeForm.post = (options?: RouteQueryOptions): RouteFormDefinition<'post'> => ({
    action: store.url(options),
    method: 'post',
})

store.form = storeForm

const PasswordResetLinkController = { create, store }

export default PasswordResetLinkController