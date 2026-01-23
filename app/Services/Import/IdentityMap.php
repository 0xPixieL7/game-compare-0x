<?php

namespace App\Services\Import;

use Illuminate\Support\Facades\Cache;

class IdentityMap
{
    // Memory cache to avoid hitting Redis/File for repeated lookups in same process
    protected static array $localCache = [];

    /**
     * Generate a unique key for the map.
     */
    public static function key(string $type, string $provider, string $externalId): string
    {
        return strtolower("imap:{$type}:{$provider}:{$externalId}");
    }

    /**
     * Store a mapping.
     */
    public static function put(string $type, string $provider, string $externalId, int $newId): void
    {
        $key = self::key($type, $provider, $externalId);
        self::$localCache[$key] = $newId;
        Cache::put($key, $newId, now()->addDays(2));
    }

    /**
     * Store multiple mappings at once.
     */
    public static function putMany(string $type, string $provider, array $mappings): void
    {
        $cacheData = [];
        foreach ($mappings as $externalId => $newId) {
            $key = self::key($type, $provider, (string) $externalId);
            self::$localCache[$key] = $newId;
            $cacheData[$key] = $newId;
        }

        if (! empty($cacheData)) {
            Cache::putMany($cacheData, now()->addDays(2));
        }
    }

    /**
     * Retrieve a new ID.
     */
    public static function get(string $type, string $provider, string $externalId): ?int
    {
        $key = self::key($type, $provider, $externalId);

        if (isset(self::$localCache[$key])) {
            return self::$localCache[$key];
        }

        $val = Cache::get($key);
        if ($val) {
            self::$localCache[$key] = $val;

            return $val;
        }

        return null;
    }

    /**
     * Retrieve multiple mappings at once.
     */
    public static function getMany(string $type, string $provider, array $externalIds): array
    {
        $keys = [];
        $results = [];

        foreach ($externalIds as $id) {
            $k = self::key($type, $provider, (string) $id);
            if (isset(self::$localCache[$k])) {
                $results[$id] = self::$localCache[$k];
            } else {
                $keys[$k] = $id;
            }
        }

        if (! empty($keys)) {
            $cached = Cache::many(array_keys($keys));
            foreach ($cached as $k => $val) {
                if ($val !== null) {
                    $originalId = $keys[$k];
                    self::$localCache[$k] = $val;
                    $results[$originalId] = $val;
                }
            }
        }

        return $results;
    }

    /**
     * Clear the local map.
     */
    public static function clear(): void
    {
        self::$localCache = [];
    }
}
