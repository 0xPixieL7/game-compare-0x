<?php

namespace App\Services\Import;

class IdentityMap
{
    protected static array $map = [];

    /**
     * Generate a unique key for the map.
     */
    public static function key(string $type, string $provider, string $externalId): string
    {
        return strtolower("{$type}:{$provider}:{$externalId}");
    }

    /**
     * Store a mapping.
     */
    public static function put(string $type, string $provider, string $externalId, int $newId): void
    {
        self::$map[self::key($type, $provider, $externalId)] = $newId;
    }

    /**
     * Retrieve a new ID.
     */
    public static function get(string $type, string $provider, string $externalId): ?int
    {
        return self::$map[self::key($type, $provider, $externalId)] ?? null;
    }

    /**
     * Clear the map (memory management).
     */
    public static function clear(): void
    {
        self::$map = [];
    }
    
    /**
     * Bulk load logic if needed
     */
}
