-- WARNING: This schema is for context only and is not meant to be run.
-- Table order and constraints may not be valid for execution.
CREATE TABLE public.cache (
    key character varying NOT NULL,
    value text NOT NULL,
    expiration integer NOT NULL,
    CONSTRAINT cache_pkey PRIMARY KEY (key)
);
CREATE TABLE public.cache_locks (
    key character varying NOT NULL,
    owner character varying NOT NULL,
    expiration integer NOT NULL,
    CONSTRAINT cache_locks_pkey PRIMARY KEY (key)
);
CREATE TABLE public.countries (
    id bigint NOT NULL DEFAULT nextval('countries_id_seq'::regclass),
    code character NOT NULL UNIQUE,
    name character varying NOT NULL,
    currency_id bigint NOT NULL,
    region character varying,
    metadata json,
    created_at timestamp without time zone,
    updated_at timestamp without time zone,
    CONSTRAINT countries_pkey PRIMARY KEY (id),
    CONSTRAINT countries_currency_id_foreign FOREIGN KEY (currency_id) REFERENCES public.currencies(id)
);
CREATE TABLE public.currencies (
    id bigint NOT NULL DEFAULT nextval('currencies_id_seq'::regclass),
    code character NOT NULL UNIQUE,
    name character varying NOT NULL,
    symbol character varying,
    decimals smallint NOT NULL DEFAULT '2'::smallint,
    is_crypto boolean NOT NULL DEFAULT false,
    metadata json,
    created_at timestamp without time zone,
    updated_at timestamp without time zone,
    CONSTRAINT currencies_pkey PRIMARY KEY (id)
);
CREATE TABLE public.exchange_rates (
    id bigint NOT NULL DEFAULT nextval('exchange_rates_id_seq'::regclass),
    base_currency character NOT NULL,
    quote_currency character NOT NULL,
    rate numeric NOT NULL,
    fetched_at timestamp without time zone NOT NULL,
    provider character varying,
    metadata json,
    created_at timestamp without time zone,
    updated_at timestamp without time zone,
    CONSTRAINT exchange_rates_pkey PRIMARY KEY (id),
    CONSTRAINT exchange_rates_base_currency_foreign FOREIGN KEY (base_currency) REFERENCES public.currencies(code),
    CONSTRAINT exchange_rates_quote_currency_foreign FOREIGN KEY (quote_currency) REFERENCES public.currencies(code)
);
CREATE TABLE public.failed_jobs (
    id bigint NOT NULL DEFAULT nextval('failed_jobs_id_seq'::regclass),
    uuid character varying NOT NULL UNIQUE,
    connection text NOT NULL,
    queue text NOT NULL,
    payload text NOT NULL,
    exception text NOT NULL,
    failed_at timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT failed_jobs_pkey PRIMARY KEY (id)
);
CREATE TABLE public.images (
    id bigint NOT NULL DEFAULT nextval('images_id_seq'::regclass),
    uuid uuid,
    collection_names json,
    primary_collection character varying,
    imageable_type character varying NOT NULL,
    imageable_id bigint NOT NULL,
    video_game_id bigint,
    media_id bigint,
    url text NOT NULL,
    external_id character varying,
    provider character varying,
    source_url text,
    width integer,
    height integer,
    alt_text text,
    caption text,
    is_thumbnail boolean NOT NULL DEFAULT false,
    order_column integer,
    urls json,
    metadata json,
    created_at timestamp without time zone,
    updated_at timestamp without time zone,
    CONSTRAINT images_pkey PRIMARY KEY (id),
    CONSTRAINT images_video_game_id_foreign FOREIGN KEY (video_game_id) REFERENCES public.video_games(id),
    CONSTRAINT images_media_id_foreign FOREIGN KEY (media_id) REFERENCES public.media(id)
);
CREATE TABLE public.job_batches (
    id character varying NOT NULL,
    name character varying NOT NULL,
    total_jobs integer NOT NULL,
    pending_jobs integer NOT NULL,
    failed_jobs integer NOT NULL,
    failed_job_ids text NOT NULL,
    options text,
    cancelled_at integer,
    created_at integer NOT NULL,
    finished_at integer,
    CONSTRAINT job_batches_pkey PRIMARY KEY (id)
);
CREATE TABLE public.jobs (
    id bigint NOT NULL DEFAULT nextval('jobs_id_seq'::regclass),
    queue character varying NOT NULL,
    payload text NOT NULL,
    attempts smallint NOT NULL,
    reserved_at integer,
    available_at integer NOT NULL,
    created_at integer NOT NULL,
    CONSTRAINT jobs_pkey PRIMARY KEY (id)
);
CREATE TABLE public.media (
    id bigint NOT NULL DEFAULT nextval('media_id_seq'::regclass),
    model_type character varying NOT NULL,
    model_id bigint NOT NULL,
    uuid uuid UNIQUE,
    collection_name character varying NOT NULL,
    name character varying NOT NULL,
    file_name character varying NOT NULL,
    mime_type character varying,
    disk character varying NOT NULL,
    conversions_disk character varying,
    size bigint NOT NULL,
    manipulations json NOT NULL,
    custom_properties json NOT NULL,
    generated_conversions json NOT NULL,
    responsive_images json NOT NULL,
    order_column integer,
    created_at timestamp without time zone,
    updated_at timestamp without time zone,
    CONSTRAINT media_pkey PRIMARY KEY (id)
);
CREATE TABLE public.migrations (
    id integer NOT NULL DEFAULT nextval('migrations_id_seq'::regclass),
    migration character varying NOT NULL,
    batch integer NOT NULL,
    CONSTRAINT migrations_pkey PRIMARY KEY (id)
);
CREATE TABLE public.password_reset_tokens (
    email character varying NOT NULL,
    token character varying NOT NULL,
    created_at timestamp without time zone,
    CONSTRAINT password_reset_tokens_pkey PRIMARY KEY (email)
);
CREATE TABLE public.personal_access_tokens (
    id bigint NOT NULL DEFAULT nextval('personal_access_tokens_id_seq'::regclass),
    tokenable_type character varying NOT NULL,
    tokenable_id bigint NOT NULL,
    name text NOT NULL,
    token character varying NOT NULL UNIQUE,
    abilities text,
    last_used_at timestamp without time zone,
    expires_at timestamp without time zone,
    created_at timestamp without time zone,
    updated_at timestamp without time zone,
    CONSTRAINT personal_access_tokens_pkey PRIMARY KEY (id)
);
CREATE TABLE public.price_charting_igdb_mappings (
    id bigint NOT NULL DEFAULT nextval('price_charting_igdb_mappings_id_seq'::regclass),
    price_charting_id character varying NOT NULL,
    price_charting_name character varying NOT NULL,
    price_charting_console character varying NOT NULL,
    price_charting_price character varying,
    video_game_title_id bigint NOT NULL,
    igdb_name character varying NOT NULL,
    igdb_platforms text,
    igdb_slug character varying,
    igdb_external_id character varying,
    confidence_score numeric NOT NULL DEFAULT '1'::numeric,
    match_type character varying NOT NULL DEFAULT 'exact'::character varying,
    created_at timestamp without time zone,
    updated_at timestamp without time zone,
    CONSTRAINT price_charting_igdb_mappings_pkey PRIMARY KEY (id),
    CONSTRAINT price_charting_igdb_mappings_video_game_title_id_foreign FOREIGN KEY (video_game_title_id) REFERENCES public.video_game_titles(id)
);
CREATE TABLE public.products (
    id bigint NOT NULL DEFAULT nextval('products_id_seq'::regclass),
    type character varying NOT NULL DEFAULT 'video_game'::character varying,
    created_at timestamp without time zone,
    updated_at timestamp without time zone,
    name character varying NOT NULL,
    slug character varying NOT NULL UNIQUE,
    platform character varying,
    category character varying,
    title character varying,
    normalized_title character varying,
    synopsis text,
    release_date date,
    popularity_score numeric,
    rating numeric,
    external_ids jsonb,
    metadata jsonb,
    CONSTRAINT products_pkey PRIMARY KEY (id)
);
CREATE TABLE public.retailers (
    id bigint NOT NULL DEFAULT nextval('retailers_id_seq'::regclass),
    name character varying NOT NULL,
    slug character varying NOT NULL UNIQUE,
    base_url character varying,
    domain_matcher character varying NOT NULL,
    is_active boolean NOT NULL DEFAULT true,
    config json,
    created_at timestamp without time zone,
    updated_at timestamp without time zone,
    CONSTRAINT retailers_pkey PRIMARY KEY (id)
);
CREATE TABLE public.sessions (
    id character varying NOT NULL,
    user_id bigint,
    ip_address character varying,
    user_agent text,
    payload text NOT NULL,
    last_activity integer NOT NULL,
    CONSTRAINT sessions_pkey PRIMARY KEY (id)
);
CREATE TABLE public.tax_profiles (
    id bigint NOT NULL DEFAULT nextval('tax_profiles_id_seq'::regclass),
    region_code character NOT NULL UNIQUE,
    vat_rate numeric NOT NULL DEFAULT '0'::numeric,
    effective_from timestamp without time zone,
    notes text,
    created_at timestamp without time zone,
    updated_at timestamp without time zone,
    CONSTRAINT tax_profiles_pkey PRIMARY KEY (id)
);
CREATE TABLE public.users (
    id bigint NOT NULL DEFAULT nextval('users_id_seq'::regclass),
    name character varying NOT NULL,
    email character varying NOT NULL UNIQUE,
    email_verified_at timestamp without time zone,
    password character varying NOT NULL,
    remember_token character varying,
    created_at timestamp without time zone,
    updated_at timestamp without time zone,
    two_factor_secret text,
    two_factor_recovery_codes text,
    two_factor_confirmed_at timestamp without time zone,
    is_admin boolean NOT NULL DEFAULT false,
    discord_id character varying,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
CREATE TABLE public.video_game_alternative_names (
    id bigint NOT NULL DEFAULT nextval('video_game_alternative_names_id_seq'::regclass),
    video_game_id bigint NOT NULL,
    name character varying NOT NULL,
    comment character varying,
    created_at timestamp without time zone,
    updated_at timestamp without time zone,
    CONSTRAINT video_game_alternative_names_pkey PRIMARY KEY (id),
    CONSTRAINT video_game_alternative_names_video_game_id_foreign FOREIGN KEY (video_game_id) REFERENCES public.video_games(id)
);
CREATE TABLE public.video_game_external_links (
    id bigint NOT NULL DEFAULT nextval('video_game_external_links_id_seq'::regclass),
    video_game_id bigint NOT NULL,
    category integer NOT NULL,
    external_id character varying NOT NULL,
    url character varying,
    created_at timestamp without time zone,
    updated_at timestamp without time zone,
    CONSTRAINT video_game_external_links_pkey PRIMARY KEY (id),
    CONSTRAINT video_game_external_links_video_game_id_foreign FOREIGN KEY (video_game_id) REFERENCES public.video_games(id)
);
CREATE TABLE public.video_game_platform_families (
    id bigint NOT NULL DEFAULT nextval('video_game_platform_families_id_seq'::regclass),
    name character varying NOT NULL UNIQUE,
    slug character varying NOT NULL UNIQUE,
    created_at timestamp without time zone,
    updated_at timestamp without time zone,
    CONSTRAINT video_game_platform_families_pkey PRIMARY KEY (id)
);
CREATE TABLE public.video_game_platforms (
    id bigint NOT NULL DEFAULT nextval('video_game_platforms_id_seq'::regclass),
    platform_family_id bigint,
    name character varying NOT NULL UNIQUE,
    slug character varying NOT NULL UNIQUE,
    abbreviation character varying,
    summary text,
    logo_path character varying,
    created_at timestamp without time zone,
    updated_at timestamp without time zone,
    CONSTRAINT video_game_platforms_pkey PRIMARY KEY (id),
    CONSTRAINT video_game_platforms_platform_family_id_foreign FOREIGN KEY (platform_family_id) REFERENCES public.video_game_platform_families(id)
);
CREATE TABLE public.video_game_prices (
    id bigint NOT NULL DEFAULT nextval('video_game_prices_id_seq'::regclass),
    video_game_id bigint NOT NULL,
    product_id bigint,
    currency character NOT NULL,
    country_code character,
    region_code character,
    condition character varying,
    amount_minor bigint NOT NULL,
    recorded_at timestamp without time zone NOT NULL,
    retailer character varying,
    url text,
    tax_inclusive boolean NOT NULL DEFAULT false,
    sku character varying,
    is_active boolean NOT NULL DEFAULT true,
    is_retail_buy boolean NOT NULL DEFAULT false,
    sales_volume integer,
    metadata jsonb,
    created_at timestamp without time zone,
    updated_at timestamp without time zone,
    bucket character varying NOT NULL DEFAULT 'snapshot'::character varying,
    window_start timestamp without time zone,
    window_end timestamp without time zone,
    btc_value_sats bigint,
    aggregation_count integer NOT NULL DEFAULT 1,
    series_key character varying,
    retailer_id bigint,
    amount_btc numeric,
    CONSTRAINT video_game_prices_pkey PRIMARY KEY (id),
    CONSTRAINT video_game_prices_video_game_id_foreign FOREIGN KEY (video_game_id) REFERENCES public.video_games(id),
    CONSTRAINT video_game_prices_product_id_foreign FOREIGN KEY (product_id) REFERENCES public.products(id),
    CONSTRAINT video_game_prices_currency_foreign FOREIGN KEY (currency) REFERENCES public.currencies(code),
    CONSTRAINT video_game_prices_retailer_id_foreign FOREIGN KEY (retailer_id) REFERENCES public.retailers(id)
);
CREATE TABLE public.video_game_sources (
    id bigint NOT NULL DEFAULT nextval('video_game_sources_id_seq'::regclass),
    provider character varying NOT NULL UNIQUE,
    provider_key character varying,
    display_name character varying,
    category character varying,
    slug character varying,
    base_url text,
    metadata json,
    items_count bigint NOT NULL DEFAULT '0'::bigint,
    created_at timestamp without time zone,
    updated_at timestamp without time zone,
    CONSTRAINT video_game_sources_pkey PRIMARY KEY (id)
);
CREATE TABLE public.video_game_title_sources (
    id bigint NOT NULL DEFAULT nextval('video_game_title_sources_id_seq'::regclass),
    video_game_title_id bigint,
    video_game_source_id bigint NOT NULL,
    provider character varying NOT NULL DEFAULT '__invalid_provider__'::character varying,
    external_id bigint NOT NULL,
    slug character varying,
    name character varying,
    description text,
    release_date date,
    provider_item_id bigint NOT NULL,
    platform json,
    rating numeric,
    rating_count integer,
    developer character varying,
    publisher character varying,
    genre json,
    raw_payload json,
    created_at timestamp without time zone,
    updated_at timestamp without time zone,
    CONSTRAINT video_game_title_sources_pkey PRIMARY KEY (id),
    CONSTRAINT video_game_title_sources_video_game_source_id_foreign FOREIGN KEY (video_game_source_id) REFERENCES public.video_game_sources(id),
    CONSTRAINT vg_title_sources_source_provider_fk FOREIGN KEY (video_game_source_id) REFERENCES public.video_game_sources(id),
    CONSTRAINT vg_title_sources_source_provider_fk FOREIGN KEY (video_game_source_id) REFERENCES public.video_game_sources(provider),
    CONSTRAINT vg_title_sources_source_provider_fk FOREIGN KEY (provider) REFERENCES public.video_game_sources(id),
    CONSTRAINT vg_title_sources_source_provider_fk FOREIGN KEY (provider) REFERENCES public.video_game_sources(provider),
    CONSTRAINT video_game_title_sources_video_game_title_id_foreign FOREIGN KEY (video_game_title_id) REFERENCES public.video_game_titles(id)
);
CREATE TABLE public.video_game_titles (
    id bigint NOT NULL DEFAULT nextval('video_game_titles_id_seq'::regclass),
    product_id bigint NOT NULL,
    name character varying NOT NULL,
    normalized_title character varying,
    slug character varying NOT NULL UNIQUE,
    providers json,
    created_at timestamp without time zone,
    updated_at timestamp without time zone,
    CONSTRAINT video_game_titles_pkey PRIMARY KEY (id),
    CONSTRAINT video_game_titles_product_id_foreign FOREIGN KEY (product_id) REFERENCES public.products(id)
);
CREATE TABLE public.video_game_websites (
    id bigint NOT NULL DEFAULT nextval('video_game_websites_id_seq'::regclass),
    video_game_id bigint NOT NULL,
    category integer NOT NULL,
    url character varying NOT NULL,
    trusted boolean NOT NULL DEFAULT false,
    created_at timestamp without time zone,
    updated_at timestamp without time zone,
    CONSTRAINT video_game_websites_pkey PRIMARY KEY (id),
    CONSTRAINT video_game_websites_video_game_id_foreign FOREIGN KEY (video_game_id) REFERENCES public.video_games(id)
);
CREATE TABLE public.video_games (
    id bigint NOT NULL DEFAULT nextval('video_games_id_seq'::regclass),
    video_game_title_id bigint,
    slug character varying NOT NULL,
    provider character varying NOT NULL,
    external_id character varying NOT NULL,
    name character varying,
    description text,
    summary text,
    storyline text,
    url character varying,
    release_date date,
    platform json,
    rating numeric,
    rating_count integer,
    developer character varying,
    publisher character varying,
    genre json,
    media json,
    source_payload json,
    created_at timestamp without time zone,
    updated_at timestamp without time zone,
    attributes json,
    last_enriched_at timestamp without time zone,
    hypes integer,
    follows integer,
    CONSTRAINT video_games_pkey PRIMARY KEY (id),
    CONSTRAINT video_games_video_game_title_id_foreign FOREIGN KEY (video_game_title_id) REFERENCES public.video_game_titles(id)
);
CREATE TABLE public.videos (
    id bigint NOT NULL DEFAULT nextval('videos_id_seq'::regclass),
    uuid uuid,
    collection_names json,
    primary_collection character varying,
    videoable_type character varying NOT NULL,
    videoable_id bigint NOT NULL,
    video_game_id bigint,
    media_id bigint,
    url text NOT NULL,
    video_id character varying,
    external_id character varying,
    source_url text,
    urls json,
    provider character varying,
    duration integer,
    width integer,
    height integer,
    thumbnail_url text,
    title character varying,
    description text,
    order_column integer,
    metadata json,
    created_at timestamp without time zone,
    updated_at timestamp without time zone,
    CONSTRAINT videos_pkey PRIMARY KEY (id),
    CONSTRAINT videos_video_game_id_foreign FOREIGN KEY (video_game_id) REFERENCES public.video_games(id),
    CONSTRAINT videos_media_id_foreign FOREIGN KEY (media_id) REFERENCES public.media(id)
);
CREATE TABLE public.webhook_events (
    id bigint NOT NULL DEFAULT nextval('webhook_events_id_seq'::regclass),
    provider character varying NOT NULL DEFAULT 'igdb'::character varying,
    event_type character varying NOT NULL,
    igdb_game_id character varying NOT NULL,
    payload jsonb NOT NULL,
    headers jsonb,
    status character varying NOT NULL DEFAULT 'pending'::character varying,
    error_message text,
    processed_at timestamp without time zone,
    created_at timestamp without time zone,
    updated_at timestamp without time zone,
    CONSTRAINT webhook_events_pkey PRIMARY KEY (id)
);