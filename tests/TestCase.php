<?php

namespace Tests;

use Illuminate\Foundation\Testing\TestCase as BaseTestCase;
use RuntimeException;

abstract class TestCase extends BaseTestCase
{
    protected function setUp(): void
    {
        // IMPORTANT SAFETY: refuse running tests against non-local DB.
        // Do this BEFORE bootstrapping the application to avoid any DB-resetting traits.

        $env = (string) ($_ENV['APP_ENV'] ?? getenv('APP_ENV') ?: 'testing');
        if ($env !== 'testing') {
            parent::setUp();

            return;
        }

        $driver = strtolower((string) ($_ENV['DB_CONNECTION'] ?? getenv('DB_CONNECTION') ?: 'sqlite'));
        if ($driver !== 'sqlite') {
            $host = (string) ($_ENV['DB_HOST'] ?? getenv('DB_HOST') ?: '');

            if ($host === '') {
                throw new RuntimeException('Refusing to run tests without DB_HOST set.');
            }

            if (! in_array($host, ['127.0.0.1', 'localhost'], true)) {
                throw new RuntimeException(
                    "Refusing to run tests against non-local DB_HOST={$host}. Use docker-compose.test.yml.",
                );
            }
        }

        parent::setUp();
    }
}
