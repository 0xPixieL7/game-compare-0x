import { queryParams, type RouteQueryOptions, type RouteDefinition, type RouteFormDefinition } from './../../../../wayfinder'
/**
* @see \App\Http\Controllers\AIAssistantController::generateModel
* @see app/Http/Controllers/AIAssistantController.php:0
* @route '/api/ai/generate-model'
*/
export const generateModel = (options?: RouteQueryOptions): RouteDefinition<'post'> => ({
    url: generateModel.url(options),
    method: 'post',
})

generateModel.definition = {
    methods: ["post"],
    url: '/api/ai/generate-model',
} satisfies RouteDefinition<["post"]>

/**
* @see \App\Http\Controllers\AIAssistantController::generateModel
* @see app/Http/Controllers/AIAssistantController.php:0
* @route '/api/ai/generate-model'
*/
generateModel.url = (options?: RouteQueryOptions) => {
    return generateModel.definition.url + queryParams(options)
}

/**
* @see \App\Http\Controllers\AIAssistantController::generateModel
* @see app/Http/Controllers/AIAssistantController.php:0
* @route '/api/ai/generate-model'
*/
generateModel.post = (options?: RouteQueryOptions): RouteDefinition<'post'> => ({
    url: generateModel.url(options),
    method: 'post',
})

/**
* @see \App\Http\Controllers\AIAssistantController::generateModel
* @see app/Http/Controllers/AIAssistantController.php:0
* @route '/api/ai/generate-model'
*/
const generateModelForm = (options?: RouteQueryOptions): RouteFormDefinition<'post'> => ({
    action: generateModel.url(options),
    method: 'post',
})

/**
* @see \App\Http\Controllers\AIAssistantController::generateModel
* @see app/Http/Controllers/AIAssistantController.php:0
* @route '/api/ai/generate-model'
*/
generateModelForm.post = (options?: RouteQueryOptions): RouteFormDefinition<'post'> => ({
    action: generateModel.url(options),
    method: 'post',
})

generateModel.form = generateModelForm

/**
* @see \App\Http\Controllers\AIAssistantController::generateMigration
* @see app/Http/Controllers/AIAssistantController.php:0
* @route '/api/ai/generate-migration'
*/
export const generateMigration = (options?: RouteQueryOptions): RouteDefinition<'post'> => ({
    url: generateMigration.url(options),
    method: 'post',
})

generateMigration.definition = {
    methods: ["post"],
    url: '/api/ai/generate-migration',
} satisfies RouteDefinition<["post"]>

/**
* @see \App\Http\Controllers\AIAssistantController::generateMigration
* @see app/Http/Controllers/AIAssistantController.php:0
* @route '/api/ai/generate-migration'
*/
generateMigration.url = (options?: RouteQueryOptions) => {
    return generateMigration.definition.url + queryParams(options)
}

/**
* @see \App\Http\Controllers\AIAssistantController::generateMigration
* @see app/Http/Controllers/AIAssistantController.php:0
* @route '/api/ai/generate-migration'
*/
generateMigration.post = (options?: RouteQueryOptions): RouteDefinition<'post'> => ({
    url: generateMigration.url(options),
    method: 'post',
})

/**
* @see \App\Http\Controllers\AIAssistantController::generateMigration
* @see app/Http/Controllers/AIAssistantController.php:0
* @route '/api/ai/generate-migration'
*/
const generateMigrationForm = (options?: RouteQueryOptions): RouteFormDefinition<'post'> => ({
    action: generateMigration.url(options),
    method: 'post',
})

/**
* @see \App\Http\Controllers\AIAssistantController::generateMigration
* @see app/Http/Controllers/AIAssistantController.php:0
* @route '/api/ai/generate-migration'
*/
generateMigrationForm.post = (options?: RouteQueryOptions): RouteFormDefinition<'post'> => ({
    action: generateMigration.url(options),
    method: 'post',
})

generateMigration.form = generateMigrationForm

/**
* @see \App\Http\Controllers\AIAssistantController::generateTests
* @see app/Http/Controllers/AIAssistantController.php:0
* @route '/api/ai/generate-tests'
*/
export const generateTests = (options?: RouteQueryOptions): RouteDefinition<'post'> => ({
    url: generateTests.url(options),
    method: 'post',
})

generateTests.definition = {
    methods: ["post"],
    url: '/api/ai/generate-tests',
} satisfies RouteDefinition<["post"]>

/**
* @see \App\Http\Controllers\AIAssistantController::generateTests
* @see app/Http/Controllers/AIAssistantController.php:0
* @route '/api/ai/generate-tests'
*/
generateTests.url = (options?: RouteQueryOptions) => {
    return generateTests.definition.url + queryParams(options)
}

/**
* @see \App\Http\Controllers\AIAssistantController::generateTests
* @see app/Http/Controllers/AIAssistantController.php:0
* @route '/api/ai/generate-tests'
*/
generateTests.post = (options?: RouteQueryOptions): RouteDefinition<'post'> => ({
    url: generateTests.url(options),
    method: 'post',
})

/**
* @see \App\Http\Controllers\AIAssistantController::generateTests
* @see app/Http/Controllers/AIAssistantController.php:0
* @route '/api/ai/generate-tests'
*/
const generateTestsForm = (options?: RouteQueryOptions): RouteFormDefinition<'post'> => ({
    action: generateTests.url(options),
    method: 'post',
})

/**
* @see \App\Http\Controllers\AIAssistantController::generateTests
* @see app/Http/Controllers/AIAssistantController.php:0
* @route '/api/ai/generate-tests'
*/
generateTestsForm.post = (options?: RouteQueryOptions): RouteFormDefinition<'post'> => ({
    action: generateTests.url(options),
    method: 'post',
})

generateTests.form = generateTestsForm

/**
* @see \App\Http\Controllers\AIAssistantController::validateSchema
* @see app/Http/Controllers/AIAssistantController.php:0
* @route '/api/ai/validate-schema'
*/
export const validateSchema = (options?: RouteQueryOptions): RouteDefinition<'post'> => ({
    url: validateSchema.url(options),
    method: 'post',
})

validateSchema.definition = {
    methods: ["post"],
    url: '/api/ai/validate-schema',
} satisfies RouteDefinition<["post"]>

/**
* @see \App\Http\Controllers\AIAssistantController::validateSchema
* @see app/Http/Controllers/AIAssistantController.php:0
* @route '/api/ai/validate-schema'
*/
validateSchema.url = (options?: RouteQueryOptions) => {
    return validateSchema.definition.url + queryParams(options)
}

/**
* @see \App\Http\Controllers\AIAssistantController::validateSchema
* @see app/Http/Controllers/AIAssistantController.php:0
* @route '/api/ai/validate-schema'
*/
validateSchema.post = (options?: RouteQueryOptions): RouteDefinition<'post'> => ({
    url: validateSchema.url(options),
    method: 'post',
})

/**
* @see \App\Http\Controllers\AIAssistantController::validateSchema
* @see app/Http/Controllers/AIAssistantController.php:0
* @route '/api/ai/validate-schema'
*/
const validateSchemaForm = (options?: RouteQueryOptions): RouteFormDefinition<'post'> => ({
    action: validateSchema.url(options),
    method: 'post',
})

/**
* @see \App\Http\Controllers\AIAssistantController::validateSchema
* @see app/Http/Controllers/AIAssistantController.php:0
* @route '/api/ai/validate-schema'
*/
validateSchemaForm.post = (options?: RouteQueryOptions): RouteFormDefinition<'post'> => ({
    action: validateSchema.url(options),
    method: 'post',
})

validateSchema.form = validateSchemaForm

/**
* @see \App\Http\Controllers\AIAssistantController::optimizeQuery
* @see app/Http/Controllers/AIAssistantController.php:0
* @route '/api/ai/optimize-query'
*/
export const optimizeQuery = (options?: RouteQueryOptions): RouteDefinition<'post'> => ({
    url: optimizeQuery.url(options),
    method: 'post',
})

optimizeQuery.definition = {
    methods: ["post"],
    url: '/api/ai/optimize-query',
} satisfies RouteDefinition<["post"]>

/**
* @see \App\Http\Controllers\AIAssistantController::optimizeQuery
* @see app/Http/Controllers/AIAssistantController.php:0
* @route '/api/ai/optimize-query'
*/
optimizeQuery.url = (options?: RouteQueryOptions) => {
    return optimizeQuery.definition.url + queryParams(options)
}

/**
* @see \App\Http\Controllers\AIAssistantController::optimizeQuery
* @see app/Http/Controllers/AIAssistantController.php:0
* @route '/api/ai/optimize-query'
*/
optimizeQuery.post = (options?: RouteQueryOptions): RouteDefinition<'post'> => ({
    url: optimizeQuery.url(options),
    method: 'post',
})

/**
* @see \App\Http\Controllers\AIAssistantController::optimizeQuery
* @see app/Http/Controllers/AIAssistantController.php:0
* @route '/api/ai/optimize-query'
*/
const optimizeQueryForm = (options?: RouteQueryOptions): RouteFormDefinition<'post'> => ({
    action: optimizeQuery.url(options),
    method: 'post',
})

/**
* @see \App\Http\Controllers\AIAssistantController::optimizeQuery
* @see app/Http/Controllers/AIAssistantController.php:0
* @route '/api/ai/optimize-query'
*/
optimizeQueryForm.post = (options?: RouteQueryOptions): RouteFormDefinition<'post'> => ({
    action: optimizeQuery.url(options),
    method: 'post',
})

optimizeQuery.form = optimizeQueryForm

/**
* @see \App\Http\Controllers\AIAssistantController::autoFixTypes
* @see app/Http/Controllers/AIAssistantController.php:0
* @route '/api/ai/auto-fix-types'
*/
export const autoFixTypes = (options?: RouteQueryOptions): RouteDefinition<'post'> => ({
    url: autoFixTypes.url(options),
    method: 'post',
})

autoFixTypes.definition = {
    methods: ["post"],
    url: '/api/ai/auto-fix-types',
} satisfies RouteDefinition<["post"]>

/**
* @see \App\Http\Controllers\AIAssistantController::autoFixTypes
* @see app/Http/Controllers/AIAssistantController.php:0
* @route '/api/ai/auto-fix-types'
*/
autoFixTypes.url = (options?: RouteQueryOptions) => {
    return autoFixTypes.definition.url + queryParams(options)
}

/**
* @see \App\Http\Controllers\AIAssistantController::autoFixTypes
* @see app/Http/Controllers/AIAssistantController.php:0
* @route '/api/ai/auto-fix-types'
*/
autoFixTypes.post = (options?: RouteQueryOptions): RouteDefinition<'post'> => ({
    url: autoFixTypes.url(options),
    method: 'post',
})

/**
* @see \App\Http\Controllers\AIAssistantController::autoFixTypes
* @see app/Http/Controllers/AIAssistantController.php:0
* @route '/api/ai/auto-fix-types'
*/
const autoFixTypesForm = (options?: RouteQueryOptions): RouteFormDefinition<'post'> => ({
    action: autoFixTypes.url(options),
    method: 'post',
})

/**
* @see \App\Http\Controllers\AIAssistantController::autoFixTypes
* @see app/Http/Controllers/AIAssistantController.php:0
* @route '/api/ai/auto-fix-types'
*/
autoFixTypesForm.post = (options?: RouteQueryOptions): RouteFormDefinition<'post'> => ({
    action: autoFixTypes.url(options),
    method: 'post',
})

autoFixTypes.form = autoFixTypesForm

/**
* @see \App\Http\Controllers\AIAssistantController::generateApiDocs
* @see app/Http/Controllers/AIAssistantController.php:0
* @route '/api/ai/generate-api-docs'
*/
export const generateApiDocs = (options?: RouteQueryOptions): RouteDefinition<'post'> => ({
    url: generateApiDocs.url(options),
    method: 'post',
})

generateApiDocs.definition = {
    methods: ["post"],
    url: '/api/ai/generate-api-docs',
} satisfies RouteDefinition<["post"]>

/**
* @see \App\Http\Controllers\AIAssistantController::generateApiDocs
* @see app/Http/Controllers/AIAssistantController.php:0
* @route '/api/ai/generate-api-docs'
*/
generateApiDocs.url = (options?: RouteQueryOptions) => {
    return generateApiDocs.definition.url + queryParams(options)
}

/**
* @see \App\Http\Controllers\AIAssistantController::generateApiDocs
* @see app/Http/Controllers/AIAssistantController.php:0
* @route '/api/ai/generate-api-docs'
*/
generateApiDocs.post = (options?: RouteQueryOptions): RouteDefinition<'post'> => ({
    url: generateApiDocs.url(options),
    method: 'post',
})

/**
* @see \App\Http\Controllers\AIAssistantController::generateApiDocs
* @see app/Http/Controllers/AIAssistantController.php:0
* @route '/api/ai/generate-api-docs'
*/
const generateApiDocsForm = (options?: RouteQueryOptions): RouteFormDefinition<'post'> => ({
    action: generateApiDocs.url(options),
    method: 'post',
})

/**
* @see \App\Http\Controllers\AIAssistantController::generateApiDocs
* @see app/Http/Controllers/AIAssistantController.php:0
* @route '/api/ai/generate-api-docs'
*/
generateApiDocsForm.post = (options?: RouteQueryOptions): RouteFormDefinition<'post'> => ({
    action: generateApiDocs.url(options),
    method: 'post',
})

generateApiDocs.form = generateApiDocsForm

const AIAssistantController = { generateModel, generateMigration, generateTests, validateSchema, optimizeQuery, autoFixTypes, generateApiDocs }

export default AIAssistantController