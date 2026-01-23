<?php

declare(strict_types=1);

namespace App\Http\Requests;

use Illuminate\Foundation\Http\FormRequest;
use Illuminate\Validation\Rule;

class StoreVideoGamePriceRequest extends FormRequest
{
    public function authorize(): bool
    {
        return $this->user() !== null;
    }

    /**
     * @return array<string, mixed>
     */
    public function rules(): array
    {
        return [
            'retailer' => ['required', 'string', 'max:255'],
            'country_code' => ['required', 'string', 'size:2'],
            'currency' => [
                'required',
                'string',
                'size:3',
                Rule::exists('currencies', 'code'),
            ],
            'amount_minor' => ['required', 'integer', 'min:0'],

            'recorded_at' => ['sometimes', 'date'],
            'url' => ['sometimes', 'nullable', 'string', 'max:2048'],
            'tax_inclusive' => ['sometimes', 'boolean'],
            'region_code' => ['sometimes', 'nullable', 'string', 'size:2'],
            'condition' => ['sometimes', 'nullable', 'string', 'max:255'],
            'sku' => ['sometimes', 'nullable', 'string', 'max:255'],
            'is_active' => ['sometimes', 'boolean'],
            'metadata' => ['sometimes', 'nullable', 'array'],
        ];
    }
}
