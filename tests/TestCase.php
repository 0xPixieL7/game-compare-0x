<?php

namespace Tests;

use Illuminate\Foundation\Testing\TestCase as BaseTestCase;
use RuntimeException;

abstract class TestCase extends BaseTestCase
{
    protected function setUp(): void
    {
        parent::setUp();

        if (! app()->environment('testing')) {
            return;
        }

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
}
