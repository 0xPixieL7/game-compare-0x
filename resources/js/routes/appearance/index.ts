import { queryParams, type RouteQueryOptions, type RouteDefinition, type RouteFormDefinition } from './../../wayfinder'
/**
* @see routes/settings.php:22
* @route '//127.0.0.1:8000/settings/appearance'
*/
export const edit = (options?: RouteQueryOptions): RouteDefinition<'get'> => ({
    url: edit.url(options),
    method: 'get',
})

edit.definition = {
    methods: ["get","head"],
    url: '//127.0.0.1:8000/settings/appearance',
} satisfies RouteDefinition<["get","head"]>

/**
* @see routes/settings.php:22
* @route '//127.0.0.1:8000/settings/appearance'
*/
edit.url = (options?: RouteQueryOptions) => {
    return edit.definition.url + queryParams(options)
}

/**
* @see routes/settings.php:22
* @route '//127.0.0.1:8000/settings/appearance'
*/
edit.get = (options?: RouteQueryOptions): RouteDefinition<'get'> => ({
    url: edit.url(options),
    method: 'get',
})

/**
* @see routes/settings.php:22
* @route '//127.0.0.1:8000/settings/appearance'
*/
edit.head = (options?: RouteQueryOptions): RouteDefinition<'head'> => ({
    url: edit.url(options),
    method: 'head',
})

/**
* @see routes/settings.php:22
* @route '//127.0.0.1:8000/settings/appearance'
*/
const editForm = (options?: RouteQueryOptions): RouteFormDefinition<'get'> => ({
    action: edit.url(options),
    method: 'get',
})

/**
* @see routes/settings.php:22
* @route '//127.0.0.1:8000/settings/appearance'
*/
editForm.get = (options?: RouteQueryOptions): RouteFormDefinition<'get'> => ({
    action: edit.url(options),
    method: 'get',
})

/**
* @see routes/settings.php:22
* @route '//127.0.0.1:8000/settings/appearance'
*/
editForm.head = (options?: RouteQueryOptions): RouteFormDefinition<'get'> => ({
    action: edit.url({
        [options?.mergeQuery ? 'mergeQuery' : 'query']: {
            _method: 'HEAD',
            ...(options?.query ?? options?.mergeQuery ?? {}),
        }
    }),
    method: 'get',
})

edit.form = editForm

const appearance = {
    edit: Object.assign(edit, edit),
}

export default appearance