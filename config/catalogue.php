<?php

return [
    'trending_seed_limit' => env('SEED_TRENDING_LIMIT', 7),
    'trending_seed_window_days' => env('SEED_TRENDING_WINDOW_DAYS', 7),
    'skip_verify_links' => env('CATALOGUE_SKIP_VERIFY_LINKS', false),
    'nexarda' => [
        // Dedicated feed key for /api/v3/feed requests (do not commit the secret; set in .env)
        'feed_key' => env('NEXARDA_FEED_KEY'),
        // Optional local catalogue file path; defaults to project root nexarda_product_catalogue.json
        'local_catalogue_file' => env('NEXARDA_CATALOGUE_FILE'),
    ],
    'cross_reference' => [
        'giant_bomb_catalogue_file' => env('GIANTBOMB_CATALOGUE_FILE', 'giant_bomb_games_detailed.json'),
        'price_guide_file' => env('PRICE_GUIDE_CSV_FILE', 'price-guide.csv'),
        'price_guide_connection' => env('PRICE_GUIDE_DB_CONNECTION'),
        'price_guide_table' => env('PRICE_GUIDE_DB_TABLE', 'price_guide_entries'),
        'cache_minutes' => (int) env('CROSS_REFERENCE_CACHE_MINUTES', 45),
        'frontend_limit' => env('CROSS_REFERENCE_FRONTEND_LIMIT', 'all'),
    ],
    'igdb_top' => [
        'limit_per_platform' => (int) env('IGDB_TOP_LIMIT_PER_PLATFORM', 100),
        'min_aggregated_rating' => (float) env('IGDB_TOP_MIN_AGGREGATED_RATING', 70),
        'min_rating_count' => (int) env('IGDB_TOP_MIN_RATING_COUNT', 15),
        'platforms' => [
            'pc' => [
                'id' => 6,
                'label' => 'PC (Microsoft Windows)',
                'family' => 'pc',
            ],
            'ps5' => [
                'id' => 167,
                'label' => 'PlayStation 5',
                'family' => 'playstation',
            ],
            'ps4' => [
                'id' => 48,
                'label' => 'PlayStation 4',
                'family' => 'playstation',
            ],
            'xbox-series' => [
                'id' => 169,
                'label' => 'Xbox Series X|S',
                'family' => 'xbox',
            ],
            'xbox-one' => [
                'id' => 49,
                'label' => 'Xbox One',
                'family' => 'xbox',
            ],
            'switch' => [
                'id' => 130,
                'label' => 'Nintendo Switch',
                'family' => 'nintendo',
            ],
        ],
    ],
    'sources' => [
        'rawg' => [
            'enabled' => env('CATALOGUE_SOURCE_RAWG_ENABLED', true),
            'limit' => env('CATALOGUE_SOURCE_RAWG_LIMIT', null),
        ],
        'thegamesdb_mirror' => [
            'enabled' => env('CATALOGUE_SOURCE_TGDB_ENABLED', true),
            'limit' => env('CATALOGUE_SOURCE_TGDB_LIMIT', 250),
            'categories' => ['Hardware', 'Console', 'Game'],
        ],
        'nexarda' => [
            'enabled' => env('CATALOGUE_SOURCE_NEXARDA_ENABLED', true),
            'limit' => env('CATALOGUE_SOURCE_NEXARDA_LIMIT', 200),
            'min_score' => env('CATALOGUE_SOURCE_NEXARDA_MIN_SCORE', 70),
        ],
        'nexarda_feed' => [
            'enabled' => env('CATALOGUE_SOURCE_NEXARDA_FEED_ENABLED', true),
            'limit' => env('CATALOGUE_SOURCE_NEXARDA_FEED_LIMIT', 200),
        ],
        'giantbomb' => [
            'enabled' => env('CATALOGUE_SOURCE_GIANTBOMB_ENABLED', false),
            'limit' => env('CATALOGUE_SOURCE_GIANTBOMB_LIMIT', 40),
            'min_user_reviews' => env('CATALOGUE_SOURCE_GIANTBOMB_MIN_REVIEWS', 25),
        ],
    ],
];
