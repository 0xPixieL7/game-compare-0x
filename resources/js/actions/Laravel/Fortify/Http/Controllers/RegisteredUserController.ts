import { queryParams, type RouteQueryOptions, type RouteDefinition, type RouteFormDefinition } from './../../../../../wayfinder'
/**
* @see \Laravel\Fortify\Http\Controllers\RegisteredUserController::create
* @see vendor/laravel/fortify/src/Http/Controllers/RegisteredUserController.php:41
* @route '//127.0.0.1:8000/register'
*/
export const create = (options?: RouteQueryOptions): RouteDefinition<'get'> => ({
    url: create.url(options),
    method: 'get',
})

create.definition = {
    methods: ["get","head"],
    url: '//127.0.0.1:8000/register',
} satisfies RouteDefinition<["get","head"]>

/**
* @see \Laravel\Fortify\Http\Controllers\RegisteredUserController::create
* @see vendor/laravel/fortify/src/Http/Controllers/RegisteredUserController.php:41
* @route '//127.0.0.1:8000/register'
*/
create.url = (options?: RouteQueryOptions) => {
    return create.definition.url + queryParams(options)
}

/**
* @see \Laravel\Fortify\Http\Controllers\RegisteredUserController::create
* @see vendor/laravel/fortify/src/Http/Controllers/RegisteredUserController.php:41
* @route '//127.0.0.1:8000/register'
*/
create.get = (options?: RouteQueryOptions): RouteDefinition<'get'> => ({
    url: create.url(options),
    method: 'get',
})

/**
* @see \Laravel\Fortify\Http\Controllers\RegisteredUserController::create
* @see vendor/laravel/fortify/src/Http/Controllers/RegisteredUserController.php:41
* @route '//127.0.0.1:8000/register'
*/
create.head = (options?: RouteQueryOptions): RouteDefinition<'head'> => ({
    url: create.url(options),
    method: 'head',
})

/**
* @see \Laravel\Fortify\Http\Controllers\RegisteredUserController::create
* @see vendor/laravel/fortify/src/Http/Controllers/RegisteredUserController.php:41
* @route '//127.0.0.1:8000/register'
*/
const createForm = (options?: RouteQueryOptions): RouteFormDefinition<'get'> => ({
    action: create.url(options),
    method: 'get',
})

/**
* @see \Laravel\Fortify\Http\Controllers\RegisteredUserController::create
* @see vendor/laravel/fortify/src/Http/Controllers/RegisteredUserController.php:41
* @route '//127.0.0.1:8000/register'
*/
createForm.get = (options?: RouteQueryOptions): RouteFormDefinition<'get'> => ({
    action: create.url(options),
    method: 'get',
})

/**
* @see \Laravel\Fortify\Http\Controllers\RegisteredUserController::create
* @see vendor/laravel/fortify/src/Http/Controllers/RegisteredUserController.php:41
* @route '//127.0.0.1:8000/register'
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
* @see \Laravel\Fortify\Http\Controllers\RegisteredUserController::store
* @see vendor/laravel/fortify/src/Http/Controllers/RegisteredUserController.php:53
* @route '//127.0.0.1:8000/register'
*/
export const store = (options?: RouteQueryOptions): RouteDefinition<'post'> => ({
    url: store.url(options),
    method: 'post',
})

store.definition = {
    methods: ["post"],
    url: '//127.0.0.1:8000/register',
} satisfies RouteDefinition<["post"]>

/**
* @see \Laravel\Fortify\Http\Controllers\RegisteredUserController::store
* @see vendor/laravel/fortify/src/Http/Controllers/RegisteredUserController.php:53
* @route '//127.0.0.1:8000/register'
*/
store.url = (options?: RouteQueryOptions) => {
    return store.definition.url + queryParams(options)
}

/**
* @see \Laravel\Fortify\Http\Controllers\RegisteredUserController::store
* @see vendor/laravel/fortify/src/Http/Controllers/RegisteredUserController.php:53
* @route '//127.0.0.1:8000/register'
*/
store.post = (options?: RouteQueryOptions): RouteDefinition<'post'> => ({
    url: store.url(options),
    method: 'post',
})

/**
* @see \Laravel\Fortify\Http\Controllers\RegisteredUserController::store
* @see vendor/laravel/fortify/src/Http/Controllers/RegisteredUserController.php:53
* @route '//127.0.0.1:8000/register'
*/
const storeForm = (options?: RouteQueryOptions): RouteFormDefinition<'post'> => ({
    action: store.url(options),
    method: 'post',
})

/**
* @see \Laravel\Fortify\Http\Controllers\RegisteredUserController::store
* @see vendor/laravel/fortify/src/Http/Controllers/RegisteredUserController.php:53
* @route '//127.0.0.1:8000/register'
*/
storeForm.post = (options?: RouteQueryOptions): RouteFormDefinition<'post'> => ({
    action: store.url(options),
    method: 'post',
})

store.form = storeForm

const RegisteredUserController = { create, store }

export default RegisteredUserController