--
-- PostgreSQL database dump
--

\restrict txxX0DnC5F08EaNy11azVW8xrVVz5exT1yJxznWXgLqRBigBinicqIZoMsnhKHW

-- Dumped from database version 15.15 (Debian 15.15-1.pgdg13+1)
-- Dumped by pg_dump version 18.1

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET transaction_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: ext; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA ext;


--
-- Name: citext; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS citext WITH SCHEMA public;


--
-- Name: EXTENSION citext; Type: COMMENT; Schema: -; Owner: -
--

COMMENT ON EXTENSION citext IS 'data type for case-insensitive character strings';


--
-- Name: pg_trgm; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS pg_trgm WITH SCHEMA ext;


--
-- Name: EXTENSION pg_trgm; Type: COMMENT; Schema: -; Owner: -
--

COMMENT ON EXTENSION pg_trgm IS 'text similarity measurement and index searching based on trigrams';


--
-- Name: cmp_op; Type: TYPE; Schema: public; Owner: -
--

CREATE TYPE public.cmp_op AS ENUM (
    'above',
    'below'
);


--
-- Name: sellable_kind; Type: TYPE; Schema: public; Owner: -
--

CREATE TYPE public.sellable_kind AS ENUM (
    'software',
    'hardware'
);


--
-- Name: countries_iso_autofill(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.countries_iso_autofill() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
  IF NEW.code2 IS NOT NULL THEN
    IF NEW.iso2 IS NULL THEN
      SELECT m.iso2 INTO NEW.iso2 FROM country_iso_map m WHERE m.code2 = NEW.code2;
    END IF;
    IF NEW.iso3 IS NULL THEN
      SELECT m.iso3 INTO NEW.iso3 FROM country_iso_map m WHERE m.code2 = NEW.code2;
    END IF;
  END IF;
  RETURN NEW;
END $$;


--
-- Name: ensure_price_partition(timestamp with time zone); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.ensure_price_partition(ts timestamp with time zone) RETURNS void
    LANGUAGE plpgsql
    AS $$ DECLARE start_month date := date_trunc('month', ts)::date; next_month date := (date_trunc('month', ts) + interval '1 month')::date; part_name text := format('prices_%s', to_char(start_month,'YYYY_MM')); BEGIN IF to_regclass(part_name) IS NULL THEN EXECUTE format('CREATE TABLE %I PARTITION OF prices FOR VALUES FROM (%L) TO (%L);', part_name, start_month, next_month); EXECUTE format('CREATE INDEX IF NOT EXISTS %I_oj_recorded_at ON %I (offer_jurisdiction_id, recorded_at);', part_name, part_name); EXECUTE format('CREATE INDEX IF NOT EXISTS %I_recorded_at ON %I (recorded_at);', part_name, part_name); EXECUTE format('CREATE INDEX IF NOT EXISTS %I_recorded_at_brin ON %I USING brin (recorded_at);', part_name, part_name); EXECUTE format('CREATE INDEX IF NOT EXISTS %I_oj_recorded_at_desc ON %I (offer_jurisdiction_id, recorded_at DESC) INCLUDE (amount_minor);', part_name, part_name); END IF; END $$;


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: alerts; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.alerts (
    id bigint NOT NULL,
    user_id bigint NOT NULL,
    product_id bigint NOT NULL,
    region_code character varying(2) NOT NULL,
    threshold_btc numeric(18,8) NOT NULL,
    comparison_operator character varying(255) DEFAULT 'below'::character varying NOT NULL,
    channel character varying(255) NOT NULL,
    is_active boolean DEFAULT true NOT NULL,
    last_triggered_at timestamp with time zone,
    settings jsonb,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT alerts_channel_check CHECK (((channel)::text = ANY ((ARRAY['email'::character varying, 'discord'::character varying])::text[]))),
    CONSTRAINT alerts_comparison_operator_check CHECK (((comparison_operator)::text = ANY ((ARRAY['below'::character varying, 'above'::character varying])::text[])))
);


--
-- Name: alerts_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.alerts_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: alerts_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.alerts_id_seq OWNED BY public.alerts.id;


--
-- Name: countries; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.countries (
    id bigint NOT NULL,
    iso2 character(2) NOT NULL,
    iso3 character(3) NOT NULL,
    name text NOT NULL,
    currency_id bigint
);


--
-- Name: countries_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.countries_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: countries_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.countries_id_seq OWNED BY public.countries.id;


--
-- Name: country_iso_map; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.country_iso_map (
    code2 character(2) NOT NULL,
    iso2 character(2) NOT NULL,
    iso3 character(3) NOT NULL
);


--
-- Name: currencies; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.currencies (
    id bigint NOT NULL,
    code text NOT NULL,
    name text NOT NULL,
    minor_unit smallint NOT NULL
);


--
-- Name: currencies_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.currencies_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: currencies_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.currencies_id_seq OWNED BY public.currencies.id;


--
-- Name: current_price; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.current_price (
    offer_jurisdiction_id bigint NOT NULL,
    amount_minor bigint NOT NULL,
    recorded_at timestamp with time zone NOT NULL
);


--
-- Name: exchange_rates; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.exchange_rates (
    id bigint NOT NULL,
    base_currency text NOT NULL,
    quote_currency text NOT NULL,
    rate double precision NOT NULL,
    provider text NOT NULL,
    fetched_at timestamp with time zone NOT NULL,
    metadata jsonb
);


--
-- Name: exchange_rates_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.exchange_rates_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: exchange_rates_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.exchange_rates_id_seq OWNED BY public.exchange_rates.id;


--
-- Name: game_consoles; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.game_consoles (
    id bigint NOT NULL,
    product_id bigint NOT NULL,
    model text NOT NULL,
    variant text,
    slug public.citext,
    release_date date,
    metadata jsonb,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: game_consoles_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.game_consoles_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: game_consoles_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.game_consoles_id_seq OWNED BY public.game_consoles.id;


--
-- Name: game_images; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.game_images (
    id bigint NOT NULL,
    game_provider_id bigint NOT NULL,
    image_key character varying(255),
    url character varying(255) NOT NULL,
    mime_type character varying(255),
    width integer,
    height integer,
    rank integer DEFAULT 0 NOT NULL,
    caption character varying(255),
    variants json,
    metadata json,
    created_at timestamp with time zone,
    updated_at timestamp with time zone,
    media_id bigint,
    storage_disk character varying(255),
    storage_path character varying(255),
    provider_item_id character varying(255),
    video_game_source_id bigint,
    provider_payload json,
    small_url text,
    super_url text,
    platforms text[]
);


--
-- Name: game_images_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.game_images_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: game_images_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.game_images_id_seq OWNED BY public.game_images.id;


--
-- Name: game_providers; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.game_providers (
    id bigint NOT NULL,
    provider_key character varying(64) NOT NULL,
    name character varying(255),
    slug character varying(255),
    website_url character varying(255),
    providable_type character varying(255),
    providable_id bigint,
    credentials json,
    metadata json,
    last_synced_at timestamp with time zone,
    refreshed_at timestamp with time zone,
    created_at timestamp with time zone,
    updated_at timestamp with time zone,
    video_game_source_id bigint
);


--
-- Name: game_providers_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.game_providers_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: game_providers_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.game_providers_id_seq OWNED BY public.game_providers.id;


--
-- Name: game_videos; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.game_videos (
    id bigint NOT NULL,
    game_provider_id bigint NOT NULL,
    video_key character varying(255),
    name character varying(255),
    description text,
    site_detail_url character varying(255),
    embed_url character varying(255),
    stream_url character varying(255),
    duration_seconds integer,
    published_at timestamp with time zone,
    thumbnails json,
    metadata json,
    created_at timestamp with time zone,
    updated_at timestamp with time zone,
    media_id bigint,
    storage_disk character varying(255),
    storage_path character varying(255),
    provider_item_id character varying(255),
    video_game_source_id bigint,
    provider_payload json
);


--
-- Name: game_videos_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.game_videos_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: game_videos_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.game_videos_id_seq OWNED BY public.game_videos.id;


--
-- Name: hardware; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.hardware (
    product_id bigint NOT NULL
);


--
-- Name: jurisdictions; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.jurisdictions (
    id bigint NOT NULL,
    country_id bigint NOT NULL,
    region_code text,
    region_key text GENERATED ALWAYS AS (COALESCE(region_code, ''::text)) STORED
);


--
-- Name: jurisdictions_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.jurisdictions_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: jurisdictions_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.jurisdictions_id_seq OWNED BY public.jurisdictions.id;


--
-- Name: offer_jurisdictions; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.offer_jurisdictions (
    id bigint NOT NULL,
    offer_id bigint NOT NULL,
    jurisdiction_id bigint NOT NULL,
    currency_id bigint NOT NULL
);


--
-- Name: offer_jurisdictions_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.offer_jurisdictions_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: offer_jurisdictions_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.offer_jurisdictions_id_seq OWNED BY public.offer_jurisdictions.id;


--
-- Name: offers; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.offers (
    id bigint NOT NULL,
    sellable_id bigint NOT NULL,
    retailer_id bigint NOT NULL,
    sku text,
    is_active boolean DEFAULT true NOT NULL,
    metadata jsonb,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: offers_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.offers_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: offers_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.offers_id_seq OWNED BY public.offers.id;


--
-- Name: platforms; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.platforms (
    id bigint NOT NULL,
    code text,
    name text NOT NULL,
    family text
);


--
-- Name: platforms_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.platforms_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: platforms_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.platforms_id_seq OWNED BY public.platforms.id;


--
-- Name: prices; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.prices (
    id bigint NOT NULL,
    offer_jurisdiction_id bigint NOT NULL,
    provider_item_id bigint,
    recorded_at timestamp with time zone NOT NULL,
    amount_minor bigint NOT NULL,
    tax_inclusive boolean NOT NULL,
    fx_minor_per_unit bigint,
    btc_sats_per_unit bigint,
    meta jsonb,
    CONSTRAINT prices_amount_minor_check CHECK ((amount_minor >= 0))
)
PARTITION BY RANGE (recorded_at);


--
-- Name: prices_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.prices_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: prices_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.prices_id_seq OWNED BY public.prices.id;


--
-- Name: prices_2024_11; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.prices_2024_11 (
    id bigint DEFAULT nextval('public.prices_id_seq'::regclass) NOT NULL,
    offer_jurisdiction_id bigint NOT NULL,
    provider_item_id bigint,
    recorded_at timestamp with time zone NOT NULL,
    amount_minor bigint NOT NULL,
    tax_inclusive boolean NOT NULL,
    fx_minor_per_unit bigint,
    btc_sats_per_unit bigint,
    meta jsonb,
    CONSTRAINT prices_amount_minor_check CHECK ((amount_minor >= 0))
);


--
-- Name: prices_2024_12; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.prices_2024_12 (
    id bigint DEFAULT nextval('public.prices_id_seq'::regclass) NOT NULL,
    offer_jurisdiction_id bigint NOT NULL,
    provider_item_id bigint,
    recorded_at timestamp with time zone NOT NULL,
    amount_minor bigint NOT NULL,
    tax_inclusive boolean NOT NULL,
    fx_minor_per_unit bigint,
    btc_sats_per_unit bigint,
    meta jsonb,
    CONSTRAINT prices_amount_minor_check CHECK ((amount_minor >= 0))
);


--
-- Name: prices_2025_01; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.prices_2025_01 (
    id bigint DEFAULT nextval('public.prices_id_seq'::regclass) NOT NULL,
    offer_jurisdiction_id bigint NOT NULL,
    provider_item_id bigint,
    recorded_at timestamp with time zone NOT NULL,
    amount_minor bigint NOT NULL,
    tax_inclusive boolean NOT NULL,
    fx_minor_per_unit bigint,
    btc_sats_per_unit bigint,
    meta jsonb,
    CONSTRAINT prices_amount_minor_check CHECK ((amount_minor >= 0))
);


--
-- Name: prices_2025_02; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.prices_2025_02 (
    id bigint DEFAULT nextval('public.prices_id_seq'::regclass) NOT NULL,
    offer_jurisdiction_id bigint NOT NULL,
    provider_item_id bigint,
    recorded_at timestamp with time zone NOT NULL,
    amount_minor bigint NOT NULL,
    tax_inclusive boolean NOT NULL,
    fx_minor_per_unit bigint,
    btc_sats_per_unit bigint,
    meta jsonb,
    CONSTRAINT prices_amount_minor_check CHECK ((amount_minor >= 0))
);


--
-- Name: prices_2025_03; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.prices_2025_03 (
    id bigint DEFAULT nextval('public.prices_id_seq'::regclass) NOT NULL,
    offer_jurisdiction_id bigint NOT NULL,
    provider_item_id bigint,
    recorded_at timestamp with time zone NOT NULL,
    amount_minor bigint NOT NULL,
    tax_inclusive boolean NOT NULL,
    fx_minor_per_unit bigint,
    btc_sats_per_unit bigint,
    meta jsonb,
    CONSTRAINT prices_amount_minor_check CHECK ((amount_minor >= 0))
);


--
-- Name: prices_2025_04; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.prices_2025_04 (
    id bigint DEFAULT nextval('public.prices_id_seq'::regclass) NOT NULL,
    offer_jurisdiction_id bigint NOT NULL,
    provider_item_id bigint,
    recorded_at timestamp with time zone NOT NULL,
    amount_minor bigint NOT NULL,
    tax_inclusive boolean NOT NULL,
    fx_minor_per_unit bigint,
    btc_sats_per_unit bigint,
    meta jsonb,
    CONSTRAINT prices_amount_minor_check CHECK ((amount_minor >= 0))
);


--
-- Name: prices_2025_05; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.prices_2025_05 (
    id bigint DEFAULT nextval('public.prices_id_seq'::regclass) NOT NULL,
    offer_jurisdiction_id bigint NOT NULL,
    provider_item_id bigint,
    recorded_at timestamp with time zone NOT NULL,
    amount_minor bigint NOT NULL,
    tax_inclusive boolean NOT NULL,
    fx_minor_per_unit bigint,
    btc_sats_per_unit bigint,
    meta jsonb,
    CONSTRAINT prices_amount_minor_check CHECK ((amount_minor >= 0))
);


--
-- Name: prices_2025_06; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.prices_2025_06 (
    id bigint DEFAULT nextval('public.prices_id_seq'::regclass) NOT NULL,
    offer_jurisdiction_id bigint NOT NULL,
    provider_item_id bigint,
    recorded_at timestamp with time zone NOT NULL,
    amount_minor bigint NOT NULL,
    tax_inclusive boolean NOT NULL,
    fx_minor_per_unit bigint,
    btc_sats_per_unit bigint,
    meta jsonb,
    CONSTRAINT prices_amount_minor_check CHECK ((amount_minor >= 0))
);


--
-- Name: prices_2025_07; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.prices_2025_07 (
    id bigint DEFAULT nextval('public.prices_id_seq'::regclass) NOT NULL,
    offer_jurisdiction_id bigint NOT NULL,
    provider_item_id bigint,
    recorded_at timestamp with time zone NOT NULL,
    amount_minor bigint NOT NULL,
    tax_inclusive boolean NOT NULL,
    fx_minor_per_unit bigint,
    btc_sats_per_unit bigint,
    meta jsonb,
    CONSTRAINT prices_amount_minor_check CHECK ((amount_minor >= 0))
);


--
-- Name: prices_2025_08; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.prices_2025_08 (
    id bigint DEFAULT nextval('public.prices_id_seq'::regclass) NOT NULL,
    offer_jurisdiction_id bigint NOT NULL,
    provider_item_id bigint,
    recorded_at timestamp with time zone NOT NULL,
    amount_minor bigint NOT NULL,
    tax_inclusive boolean NOT NULL,
    fx_minor_per_unit bigint,
    btc_sats_per_unit bigint,
    meta jsonb,
    CONSTRAINT prices_amount_minor_check CHECK ((amount_minor >= 0))
);


--
-- Name: prices_2025_09; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.prices_2025_09 (
    id bigint DEFAULT nextval('public.prices_id_seq'::regclass) NOT NULL,
    offer_jurisdiction_id bigint NOT NULL,
    provider_item_id bigint,
    recorded_at timestamp with time zone NOT NULL,
    amount_minor bigint NOT NULL,
    tax_inclusive boolean NOT NULL,
    fx_minor_per_unit bigint,
    btc_sats_per_unit bigint,
    meta jsonb,
    CONSTRAINT prices_amount_minor_check CHECK ((amount_minor >= 0))
);


--
-- Name: prices_2025_10; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.prices_2025_10 (
    id bigint DEFAULT nextval('public.prices_id_seq'::regclass) NOT NULL,
    offer_jurisdiction_id bigint NOT NULL,
    provider_item_id bigint,
    recorded_at timestamp with time zone NOT NULL,
    amount_minor bigint NOT NULL,
    tax_inclusive boolean NOT NULL,
    fx_minor_per_unit bigint,
    btc_sats_per_unit bigint,
    meta jsonb,
    CONSTRAINT prices_amount_minor_check CHECK ((amount_minor >= 0))
);


--
-- Name: prices_2025_11; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.prices_2025_11 (
    id bigint DEFAULT nextval('public.prices_id_seq'::regclass) NOT NULL,
    offer_jurisdiction_id bigint NOT NULL,
    provider_item_id bigint,
    recorded_at timestamp with time zone NOT NULL,
    amount_minor bigint NOT NULL,
    tax_inclusive boolean NOT NULL,
    fx_minor_per_unit bigint,
    btc_sats_per_unit bigint,
    meta jsonb,
    CONSTRAINT prices_amount_minor_check CHECK ((amount_minor >= 0))
);


--
-- Name: prices_2025_12; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.prices_2025_12 (
    id bigint DEFAULT nextval('public.prices_id_seq'::regclass) NOT NULL,
    offer_jurisdiction_id bigint NOT NULL,
    provider_item_id bigint,
    recorded_at timestamp with time zone NOT NULL,
    amount_minor bigint NOT NULL,
    tax_inclusive boolean NOT NULL,
    fx_minor_per_unit bigint,
    btc_sats_per_unit bigint,
    meta jsonb,
    CONSTRAINT prices_amount_minor_check CHECK ((amount_minor >= 0))
);


--
-- Name: prices_2026_01; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.prices_2026_01 (
    id bigint DEFAULT nextval('public.prices_id_seq'::regclass) NOT NULL,
    offer_jurisdiction_id bigint NOT NULL,
    provider_item_id bigint,
    recorded_at timestamp with time zone NOT NULL,
    amount_minor bigint NOT NULL,
    tax_inclusive boolean NOT NULL,
    fx_minor_per_unit bigint,
    btc_sats_per_unit bigint,
    meta jsonb,
    CONSTRAINT prices_amount_minor_check CHECK ((amount_minor >= 0))
);


--
-- Name: prices_2026_02; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.prices_2026_02 (
    id bigint DEFAULT nextval('public.prices_id_seq'::regclass) NOT NULL,
    offer_jurisdiction_id bigint NOT NULL,
    provider_item_id bigint,
    recorded_at timestamp with time zone NOT NULL,
    amount_minor bigint NOT NULL,
    tax_inclusive boolean NOT NULL,
    fx_minor_per_unit bigint,
    btc_sats_per_unit bigint,
    meta jsonb,
    CONSTRAINT prices_amount_minor_check CHECK ((amount_minor >= 0))
);


--
-- Name: prices_2026_03; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.prices_2026_03 (
    id bigint DEFAULT nextval('public.prices_id_seq'::regclass) NOT NULL,
    offer_jurisdiction_id bigint NOT NULL,
    provider_item_id bigint,
    recorded_at timestamp with time zone NOT NULL,
    amount_minor bigint NOT NULL,
    tax_inclusive boolean NOT NULL,
    fx_minor_per_unit bigint,
    btc_sats_per_unit bigint,
    meta jsonb,
    CONSTRAINT prices_amount_minor_check CHECK ((amount_minor >= 0))
);


--
-- Name: prices_2026_04; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.prices_2026_04 (
    id bigint DEFAULT nextval('public.prices_id_seq'::regclass) NOT NULL,
    offer_jurisdiction_id bigint NOT NULL,
    provider_item_id bigint,
    recorded_at timestamp with time zone NOT NULL,
    amount_minor bigint NOT NULL,
    tax_inclusive boolean NOT NULL,
    fx_minor_per_unit bigint,
    btc_sats_per_unit bigint,
    meta jsonb,
    CONSTRAINT prices_amount_minor_check CHECK ((amount_minor >= 0))
);


--
-- Name: prices_2026_05; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.prices_2026_05 (
    id bigint DEFAULT nextval('public.prices_id_seq'::regclass) NOT NULL,
    offer_jurisdiction_id bigint NOT NULL,
    provider_item_id bigint,
    recorded_at timestamp with time zone NOT NULL,
    amount_minor bigint NOT NULL,
    tax_inclusive boolean NOT NULL,
    fx_minor_per_unit bigint,
    btc_sats_per_unit bigint,
    meta jsonb,
    CONSTRAINT prices_amount_minor_check CHECK ((amount_minor >= 0))
);


--
-- Name: products; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.products (
    id bigint NOT NULL,
    slug public.citext NOT NULL,
    name text NOT NULL,
    category text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: products_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.products_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: products_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.products_id_seq OWNED BY public.products.id;


--
-- Name: provider_ingest_runs; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.provider_ingest_runs (
    id bigint NOT NULL,
    provider_id bigint NOT NULL,
    started_at timestamp with time zone DEFAULT now() NOT NULL,
    finished_at timestamp with time zone,
    status text,
    stats jsonb
);


--
-- Name: provider_ingest_runs_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.provider_ingest_runs_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: provider_ingest_runs_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.provider_ingest_runs_id_seq OWNED BY public.provider_ingest_runs.id;


--
-- Name: provider_items; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.provider_items (
    id bigint NOT NULL,
    provider_id bigint NOT NULL,
    external_id text,
    kind text,
    metadata jsonb,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: provider_items_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.provider_items_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: provider_items_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.provider_items_id_seq OWNED BY public.provider_items.id;


--
-- Name: provider_media_links; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.provider_media_links (
    id bigint NOT NULL,
    provider_item_id bigint NOT NULL,
    media_id bigint,
    url text,
    video_game_id bigint,
    media_type text,
    title text,
    role text,
    source text,
    metadata jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT provider_media_links_check CHECK (((media_id IS NOT NULL) OR (url IS NOT NULL)))
);


--
-- Name: provider_media_links_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.provider_media_links_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: provider_media_links_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.provider_media_links_id_seq OWNED BY public.provider_media_links.id;


--
-- Name: provider_offers; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.provider_offers (
    id bigint NOT NULL,
    provider_item_id bigint NOT NULL,
    offer_id bigint,
    metadata jsonb,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: provider_offers_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.provider_offers_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: provider_offers_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.provider_offers_id_seq OWNED BY public.provider_offers.id;


--
-- Name: providers; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.providers (
    id bigint NOT NULL,
    slug public.citext,
    name text NOT NULL,
    kind text,
    api_source text,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: providers_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.providers_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: providers_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.providers_id_seq OWNED BY public.providers.id;


--
-- Name: retailer_providers; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.retailer_providers (
    id bigint NOT NULL,
    retailer_id bigint NOT NULL,
    provider_id bigint NOT NULL
);


--
-- Name: retailer_providers_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.retailer_providers_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: retailer_providers_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.retailer_providers_id_seq OWNED BY public.retailer_providers.id;


--
-- Name: retailers; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.retailers (
    id bigint NOT NULL,
    slug public.citext,
    name text NOT NULL
);


--
-- Name: retailers_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.retailers_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: retailers_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.retailers_id_seq OWNED BY public.retailers.id;


--
-- Name: sellables; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.sellables (
    id bigint NOT NULL,
    kind public.sellable_kind NOT NULL,
    software_title_id bigint,
    console_id bigint,
    CONSTRAINT sellables_check CHECK ((((kind = 'software'::public.sellable_kind) AND (software_title_id IS NOT NULL) AND (console_id IS NULL)) OR ((kind = 'hardware'::public.sellable_kind) AND (console_id IS NOT NULL) AND (software_title_id IS NULL))))
);


--
-- Name: sellables_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.sellables_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: sellables_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.sellables_id_seq OWNED BY public.sellables.id;


--
-- Name: software; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.software (
    product_id bigint NOT NULL
);


--
-- Name: tax_rules; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.tax_rules (
    id bigint NOT NULL,
    jurisdiction_id bigint NOT NULL,
    effective_from date NOT NULL,
    effective_to date,
    rate_basis_points integer NOT NULL,
    inclusive boolean DEFAULT true NOT NULL,
    notes text
);


--
-- Name: tax_rules_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.tax_rules_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: tax_rules_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.tax_rules_id_seq OWNED BY public.tax_rules.id;


--
-- Name: users; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.users (
    id bigint NOT NULL,
    email public.citext NOT NULL,
    name text,
    timezone text DEFAULT 'UTC'::text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: users_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.users_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: users_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.users_id_seq OWNED BY public.users.id;


--
-- Name: video_game_ratings_by_locale; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.video_game_ratings_by_locale (
    id bigint NOT NULL,
    video_game_id bigint NOT NULL,
    locale text NOT NULL,
    average_rating numeric,
    rating_count bigint,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: video_game_ratings_by_locale_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.video_game_ratings_by_locale_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: video_game_ratings_by_locale_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.video_game_ratings_by_locale_id_seq OWNED BY public.video_game_ratings_by_locale.id;


--
-- Name: video_game_sources; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.video_game_sources (
    id bigint NOT NULL,
    video_game_id bigint,
    provider character varying(64),
    provider_game_id character varying(128),
    provider_slug character varying(128),
    provider_hash character varying(64),
    payload json,
    links json,
    media json,
    synced_at timestamp with time zone,
    created_at timestamp with time zone,
    updated_at timestamp with time zone,
    provider_key character varying(64),
    display_name character varying(255),
    category character varying(64),
    slug character varying(255),
    metadata json
);


--
-- Name: video_game_sources_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.video_game_sources_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: video_game_sources_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.video_game_sources_id_seq OWNED BY public.video_game_sources.id;


--
-- Name: video_game_title_sources; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.video_game_title_sources (
    id bigint NOT NULL,
    video_game_title_id bigint NOT NULL,
    source text NOT NULL,
    source_id text,
    metadata jsonb,
    fetched_at timestamp with time zone
);


--
-- Name: video_game_title_sources_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.video_game_title_sources_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: video_game_title_sources_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.video_game_title_sources_id_seq OWNED BY public.video_game_title_sources.id;


--
-- Name: video_game_titles; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.video_game_titles (
    id bigint NOT NULL,
    product_id bigint,
    video_game_id bigint,
    title text NOT NULL,
    normalized_title text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: video_game_titles_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.video_game_titles_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: video_game_titles_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.video_game_titles_id_seq OWNED BY public.video_game_titles.id;


--
-- Name: video_games; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.video_games (
    id bigint NOT NULL,
    title_id bigint NOT NULL,
    platform_id bigint NOT NULL,
    edition text,
    sellable_id bigint,
    slug public.citext,
    release_date date,
    metadata jsonb,
    average_rating real,
    rating_count bigint,
    rating_updated_at timestamp with time zone,
    genres text[],
    display_title text,
    developer text,
    region_codes text[],
    popularity_score numeric DEFAULT 0 NOT NULL,
    rating numeric DEFAULT 0 NOT NULL,
    synopsis text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: video_games_enriched; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW public.video_games_enriched AS
 SELECT vg.id,
    vgt.product_id,
    vgt.id AS title_id,
    vgt.title AS canonical_title,
    COALESCE(vg.display_title, vgt.title) AS source_title,
    vg.slug,
    vgt.normalized_title,
    vgt.normalized_title AS title_normalized,
    NULL::text AS locale,
    NULL::text AS genre,
    vg.genres,
    NULL::text[] AS platform_codes,
    vg.region_codes,
    NULL::jsonb AS external_ids,
    NULL::jsonb AS external_links,
    vg.release_date,
    vg.developer,
    vg.metadata,
    vg.created_at,
    vg.updated_at,
    NULL::timestamp with time zone AS last_synced_at
   FROM (public.video_games vg
     LEFT JOIN public.video_game_titles vgt ON ((vgt.id = vg.title_id)));


--
-- Name: video_games_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.video_games_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: video_games_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.video_games_id_seq OWNED BY public.video_games.id;


--
-- Name: prices_2024_11; Type: TABLE ATTACH; Schema: public; Owner: -
--

ALTER TABLE ONLY public.prices ATTACH PARTITION public.prices_2024_11 FOR VALUES FROM ('2024-11-01 00:00:00+00') TO ('2024-12-01 00:00:00+00');


--
-- Name: prices_2024_12; Type: TABLE ATTACH; Schema: public; Owner: -
--

ALTER TABLE ONLY public.prices ATTACH PARTITION public.prices_2024_12 FOR VALUES FROM ('2024-12-01 00:00:00+00') TO ('2025-01-01 00:00:00+00');


--
-- Name: prices_2025_01; Type: TABLE ATTACH; Schema: public; Owner: -
--

ALTER TABLE ONLY public.prices ATTACH PARTITION public.prices_2025_01 FOR VALUES FROM ('2025-01-01 00:00:00+00') TO ('2025-02-01 00:00:00+00');


--
-- Name: prices_2025_02; Type: TABLE ATTACH; Schema: public; Owner: -
--

ALTER TABLE ONLY public.prices ATTACH PARTITION public.prices_2025_02 FOR VALUES FROM ('2025-02-01 00:00:00+00') TO ('2025-03-01 00:00:00+00');


--
-- Name: prices_2025_03; Type: TABLE ATTACH; Schema: public; Owner: -
--

ALTER TABLE ONLY public.prices ATTACH PARTITION public.prices_2025_03 FOR VALUES FROM ('2025-03-01 00:00:00+00') TO ('2025-04-01 00:00:00+00');


--
-- Name: prices_2025_04; Type: TABLE ATTACH; Schema: public; Owner: -
--

ALTER TABLE ONLY public.prices ATTACH PARTITION public.prices_2025_04 FOR VALUES FROM ('2025-04-01 00:00:00+00') TO ('2025-05-01 00:00:00+00');


--
-- Name: prices_2025_05; Type: TABLE ATTACH; Schema: public; Owner: -
--

ALTER TABLE ONLY public.prices ATTACH PARTITION public.prices_2025_05 FOR VALUES FROM ('2025-05-01 00:00:00+00') TO ('2025-06-01 00:00:00+00');


--
-- Name: prices_2025_06; Type: TABLE ATTACH; Schema: public; Owner: -
--

ALTER TABLE ONLY public.prices ATTACH PARTITION public.prices_2025_06 FOR VALUES FROM ('2025-06-01 00:00:00+00') TO ('2025-07-01 00:00:00+00');


--
-- Name: prices_2025_07; Type: TABLE ATTACH; Schema: public; Owner: -
--

ALTER TABLE ONLY public.prices ATTACH PARTITION public.prices_2025_07 FOR VALUES FROM ('2025-07-01 00:00:00+00') TO ('2025-08-01 00:00:00+00');


--
-- Name: prices_2025_08; Type: TABLE ATTACH; Schema: public; Owner: -
--

ALTER TABLE ONLY public.prices ATTACH PARTITION public.prices_2025_08 FOR VALUES FROM ('2025-08-01 00:00:00+00') TO ('2025-09-01 00:00:00+00');


--
-- Name: prices_2025_09; Type: TABLE ATTACH; Schema: public; Owner: -
--

ALTER TABLE ONLY public.prices ATTACH PARTITION public.prices_2025_09 FOR VALUES FROM ('2025-09-01 00:00:00+00') TO ('2025-10-01 00:00:00+00');


--
-- Name: prices_2025_10; Type: TABLE ATTACH; Schema: public; Owner: -
--

ALTER TABLE ONLY public.prices ATTACH PARTITION public.prices_2025_10 FOR VALUES FROM ('2025-10-01 00:00:00+00') TO ('2025-11-01 00:00:00+00');


--
-- Name: prices_2025_11; Type: TABLE ATTACH; Schema: public; Owner: -
--

ALTER TABLE ONLY public.prices ATTACH PARTITION public.prices_2025_11 FOR VALUES FROM ('2025-11-01 00:00:00+00') TO ('2025-12-01 00:00:00+00');


--
-- Name: prices_2025_12; Type: TABLE ATTACH; Schema: public; Owner: -
--

ALTER TABLE ONLY public.prices ATTACH PARTITION public.prices_2025_12 FOR VALUES FROM ('2025-12-01 00:00:00+00') TO ('2026-01-01 00:00:00+00');


--
-- Name: prices_2026_01; Type: TABLE ATTACH; Schema: public; Owner: -
--

ALTER TABLE ONLY public.prices ATTACH PARTITION public.prices_2026_01 FOR VALUES FROM ('2026-01-01 00:00:00+00') TO ('2026-02-01 00:00:00+00');


--
-- Name: prices_2026_02; Type: TABLE ATTACH; Schema: public; Owner: -
--

ALTER TABLE ONLY public.prices ATTACH PARTITION public.prices_2026_02 FOR VALUES FROM ('2026-02-01 00:00:00+00') TO ('2026-03-01 00:00:00+00');


--
-- Name: prices_2026_03; Type: TABLE ATTACH; Schema: public; Owner: -
--

ALTER TABLE ONLY public.prices ATTACH PARTITION public.prices_2026_03 FOR VALUES FROM ('2026-03-01 00:00:00+00') TO ('2026-04-01 00:00:00+00');


--
-- Name: prices_2026_04; Type: TABLE ATTACH; Schema: public; Owner: -
--

ALTER TABLE ONLY public.prices ATTACH PARTITION public.prices_2026_04 FOR VALUES FROM ('2026-04-01 00:00:00+00') TO ('2026-05-01 00:00:00+00');


--
-- Name: prices_2026_05; Type: TABLE ATTACH; Schema: public; Owner: -
--

ALTER TABLE ONLY public.prices ATTACH PARTITION public.prices_2026_05 FOR VALUES FROM ('2026-05-01 00:00:00+00') TO ('2026-06-01 00:00:00+00');


--
-- Name: alerts id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.alerts ALTER COLUMN id SET DEFAULT nextval('public.alerts_id_seq'::regclass);


--
-- Name: countries id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.countries ALTER COLUMN id SET DEFAULT nextval('public.countries_id_seq'::regclass);


--
-- Name: currencies id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.currencies ALTER COLUMN id SET DEFAULT nextval('public.currencies_id_seq'::regclass);


--
-- Name: exchange_rates id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.exchange_rates ALTER COLUMN id SET DEFAULT nextval('public.exchange_rates_id_seq'::regclass);


--
-- Name: game_consoles id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.game_consoles ALTER COLUMN id SET DEFAULT nextval('public.game_consoles_id_seq'::regclass);


--
-- Name: game_images id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.game_images ALTER COLUMN id SET DEFAULT nextval('public.game_images_id_seq'::regclass);


--
-- Name: game_providers id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.game_providers ALTER COLUMN id SET DEFAULT nextval('public.game_providers_id_seq'::regclass);


--
-- Name: game_videos id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.game_videos ALTER COLUMN id SET DEFAULT nextval('public.game_videos_id_seq'::regclass);


--
-- Name: jurisdictions id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.jurisdictions ALTER COLUMN id SET DEFAULT nextval('public.jurisdictions_id_seq'::regclass);


--
-- Name: offer_jurisdictions id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.offer_jurisdictions ALTER COLUMN id SET DEFAULT nextval('public.offer_jurisdictions_id_seq'::regclass);


--
-- Name: offers id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.offers ALTER COLUMN id SET DEFAULT nextval('public.offers_id_seq'::regclass);


--
-- Name: platforms id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.platforms ALTER COLUMN id SET DEFAULT nextval('public.platforms_id_seq'::regclass);


--
-- Name: prices id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.prices ALTER COLUMN id SET DEFAULT nextval('public.prices_id_seq'::regclass);


--
-- Name: products id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.products ALTER COLUMN id SET DEFAULT nextval('public.products_id_seq'::regclass);


--
-- Name: provider_ingest_runs id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.provider_ingest_runs ALTER COLUMN id SET DEFAULT nextval('public.provider_ingest_runs_id_seq'::regclass);


--
-- Name: provider_items id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.provider_items ALTER COLUMN id SET DEFAULT nextval('public.provider_items_id_seq'::regclass);


--
-- Name: provider_media_links id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.provider_media_links ALTER COLUMN id SET DEFAULT nextval('public.provider_media_links_id_seq'::regclass);


--
-- Name: provider_offers id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.provider_offers ALTER COLUMN id SET DEFAULT nextval('public.provider_offers_id_seq'::regclass);


--
-- Name: providers id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.providers ALTER COLUMN id SET DEFAULT nextval('public.providers_id_seq'::regclass);


--
-- Name: retailer_providers id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.retailer_providers ALTER COLUMN id SET DEFAULT nextval('public.retailer_providers_id_seq'::regclass);


--
-- Name: retailers id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.retailers ALTER COLUMN id SET DEFAULT nextval('public.retailers_id_seq'::regclass);


--
-- Name: sellables id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.sellables ALTER COLUMN id SET DEFAULT nextval('public.sellables_id_seq'::regclass);


--
-- Name: tax_rules id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.tax_rules ALTER COLUMN id SET DEFAULT nextval('public.tax_rules_id_seq'::regclass);


--
-- Name: users id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.users ALTER COLUMN id SET DEFAULT nextval('public.users_id_seq'::regclass);


--
-- Name: video_game_ratings_by_locale id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.video_game_ratings_by_locale ALTER COLUMN id SET DEFAULT nextval('public.video_game_ratings_by_locale_id_seq'::regclass);


--
-- Name: video_game_sources id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.video_game_sources ALTER COLUMN id SET DEFAULT nextval('public.video_game_sources_id_seq'::regclass);


--
-- Name: video_game_title_sources id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.video_game_title_sources ALTER COLUMN id SET DEFAULT nextval('public.video_game_title_sources_id_seq'::regclass);


--
-- Name: video_game_titles id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.video_game_titles ALTER COLUMN id SET DEFAULT nextval('public.video_game_titles_id_seq'::regclass);


--
-- Name: video_games id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.video_games ALTER COLUMN id SET DEFAULT nextval('public.video_games_id_seq'::regclass);


--
-- Name: alerts alerts_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.alerts
    ADD CONSTRAINT alerts_pkey PRIMARY KEY (id);


--
-- Name: countries countries_iso2_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.countries
    ADD CONSTRAINT countries_iso2_key UNIQUE (iso2);


--
-- Name: countries countries_iso3_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.countries
    ADD CONSTRAINT countries_iso3_key UNIQUE (iso3);


--
-- Name: countries countries_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.countries
    ADD CONSTRAINT countries_pkey PRIMARY KEY (id);


--
-- Name: country_iso_map country_iso_map_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.country_iso_map
    ADD CONSTRAINT country_iso_map_pkey PRIMARY KEY (code2);


--
-- Name: currencies currencies_code_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.currencies
    ADD CONSTRAINT currencies_code_key UNIQUE (code);


--
-- Name: currencies currencies_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.currencies
    ADD CONSTRAINT currencies_pkey PRIMARY KEY (id);


--
-- Name: current_price current_price_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.current_price
    ADD CONSTRAINT current_price_pkey PRIMARY KEY (offer_jurisdiction_id);


--
-- Name: exchange_rates exchange_rates_base_currency_quote_currency_provider_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.exchange_rates
    ADD CONSTRAINT exchange_rates_base_currency_quote_currency_provider_key UNIQUE (base_currency, quote_currency, provider);


--
-- Name: exchange_rates exchange_rates_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.exchange_rates
    ADD CONSTRAINT exchange_rates_pkey PRIMARY KEY (id);


--
-- Name: game_consoles game_consoles_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.game_consoles
    ADD CONSTRAINT game_consoles_pkey PRIMARY KEY (id);


--
-- Name: game_consoles game_consoles_slug_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.game_consoles
    ADD CONSTRAINT game_consoles_slug_key UNIQUE (slug);


--
-- Name: game_images game_images_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.game_images
    ADD CONSTRAINT game_images_pkey PRIMARY KEY (id);


--
-- Name: game_providers game_providers_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.game_providers
    ADD CONSTRAINT game_providers_pkey PRIMARY KEY (id);


--
-- Name: game_videos game_videos_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.game_videos
    ADD CONSTRAINT game_videos_pkey PRIMARY KEY (id);


--
-- Name: hardware hardware_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.hardware
    ADD CONSTRAINT hardware_pkey PRIMARY KEY (product_id);


--
-- Name: jurisdictions jurisdictions_country_region_key_uq; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.jurisdictions
    ADD CONSTRAINT jurisdictions_country_region_key_uq UNIQUE (country_id, region_key);


--
-- Name: jurisdictions jurisdictions_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.jurisdictions
    ADD CONSTRAINT jurisdictions_pkey PRIMARY KEY (id);


--
-- Name: offer_jurisdictions offer_jurisdictions_offer_jurisdiction_uq; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.offer_jurisdictions
    ADD CONSTRAINT offer_jurisdictions_offer_jurisdiction_uq UNIQUE (offer_id, jurisdiction_id);


--
-- Name: offer_jurisdictions offer_jurisdictions_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.offer_jurisdictions
    ADD CONSTRAINT offer_jurisdictions_pkey PRIMARY KEY (id);


--
-- Name: offers offers_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.offers
    ADD CONSTRAINT offers_pkey PRIMARY KEY (id);


--
-- Name: platforms platforms_code_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.platforms
    ADD CONSTRAINT platforms_code_key UNIQUE (code);


--
-- Name: platforms platforms_name_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.platforms
    ADD CONSTRAINT platforms_name_key UNIQUE (name);


--
-- Name: platforms platforms_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.platforms
    ADD CONSTRAINT platforms_pkey PRIMARY KEY (id);


--
-- Name: prices prices_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.prices
    ADD CONSTRAINT prices_pkey PRIMARY KEY (id, recorded_at);


--
-- Name: prices_2024_11 prices_2024_11_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.prices_2024_11
    ADD CONSTRAINT prices_2024_11_pkey PRIMARY KEY (id, recorded_at);


--
-- Name: prices_2024_12 prices_2024_12_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.prices_2024_12
    ADD CONSTRAINT prices_2024_12_pkey PRIMARY KEY (id, recorded_at);


--
-- Name: prices_2025_01 prices_2025_01_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.prices_2025_01
    ADD CONSTRAINT prices_2025_01_pkey PRIMARY KEY (id, recorded_at);


--
-- Name: prices_2025_02 prices_2025_02_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.prices_2025_02
    ADD CONSTRAINT prices_2025_02_pkey PRIMARY KEY (id, recorded_at);


--
-- Name: prices_2025_03 prices_2025_03_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.prices_2025_03
    ADD CONSTRAINT prices_2025_03_pkey PRIMARY KEY (id, recorded_at);


--
-- Name: prices_2025_04 prices_2025_04_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.prices_2025_04
    ADD CONSTRAINT prices_2025_04_pkey PRIMARY KEY (id, recorded_at);


--
-- Name: prices_2025_05 prices_2025_05_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.prices_2025_05
    ADD CONSTRAINT prices_2025_05_pkey PRIMARY KEY (id, recorded_at);


--
-- Name: prices_2025_06 prices_2025_06_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.prices_2025_06
    ADD CONSTRAINT prices_2025_06_pkey PRIMARY KEY (id, recorded_at);


--
-- Name: prices_2025_07 prices_2025_07_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.prices_2025_07
    ADD CONSTRAINT prices_2025_07_pkey PRIMARY KEY (id, recorded_at);


--
-- Name: prices_2025_08 prices_2025_08_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.prices_2025_08
    ADD CONSTRAINT prices_2025_08_pkey PRIMARY KEY (id, recorded_at);


--
-- Name: prices_2025_09 prices_2025_09_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.prices_2025_09
    ADD CONSTRAINT prices_2025_09_pkey PRIMARY KEY (id, recorded_at);


--
-- Name: prices_2025_10 prices_2025_10_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.prices_2025_10
    ADD CONSTRAINT prices_2025_10_pkey PRIMARY KEY (id, recorded_at);


--
-- Name: prices_2025_11 prices_2025_11_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.prices_2025_11
    ADD CONSTRAINT prices_2025_11_pkey PRIMARY KEY (id, recorded_at);


--
-- Name: prices_2025_12 prices_2025_12_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.prices_2025_12
    ADD CONSTRAINT prices_2025_12_pkey PRIMARY KEY (id, recorded_at);


--
-- Name: prices_2026_01 prices_2026_01_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.prices_2026_01
    ADD CONSTRAINT prices_2026_01_pkey PRIMARY KEY (id, recorded_at);


--
-- Name: prices_2026_02 prices_2026_02_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.prices_2026_02
    ADD CONSTRAINT prices_2026_02_pkey PRIMARY KEY (id, recorded_at);


--
-- Name: prices_2026_03 prices_2026_03_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.prices_2026_03
    ADD CONSTRAINT prices_2026_03_pkey PRIMARY KEY (id, recorded_at);


--
-- Name: prices_2026_04 prices_2026_04_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.prices_2026_04
    ADD CONSTRAINT prices_2026_04_pkey PRIMARY KEY (id, recorded_at);


--
-- Name: prices_2026_05 prices_2026_05_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.prices_2026_05
    ADD CONSTRAINT prices_2026_05_pkey PRIMARY KEY (id, recorded_at);


--
-- Name: products products_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.products
    ADD CONSTRAINT products_pkey PRIMARY KEY (id);


--
-- Name: products products_slug_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.products
    ADD CONSTRAINT products_slug_key UNIQUE (slug);


--
-- Name: provider_ingest_runs provider_ingest_runs_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.provider_ingest_runs
    ADD CONSTRAINT provider_ingest_runs_pkey PRIMARY KEY (id);


--
-- Name: provider_items provider_items_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.provider_items
    ADD CONSTRAINT provider_items_pkey PRIMARY KEY (id);


--
-- Name: provider_media_links provider_media_links_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.provider_media_links
    ADD CONSTRAINT provider_media_links_pkey PRIMARY KEY (id);


--
-- Name: provider_offers provider_offers_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.provider_offers
    ADD CONSTRAINT provider_offers_pkey PRIMARY KEY (id);


--
-- Name: providers providers_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.providers
    ADD CONSTRAINT providers_pkey PRIMARY KEY (id);


--
-- Name: providers providers_slug_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.providers
    ADD CONSTRAINT providers_slug_key UNIQUE (slug);


--
-- Name: retailer_providers retailer_providers_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.retailer_providers
    ADD CONSTRAINT retailer_providers_pkey PRIMARY KEY (id);


--
-- Name: retailer_providers retailer_providers_retailer_id_provider_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.retailer_providers
    ADD CONSTRAINT retailer_providers_retailer_id_provider_id_key UNIQUE (retailer_id, provider_id);


--
-- Name: retailers retailers_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.retailers
    ADD CONSTRAINT retailers_pkey PRIMARY KEY (id);


--
-- Name: retailers retailers_slug_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.retailers
    ADD CONSTRAINT retailers_slug_key UNIQUE (slug);


--
-- Name: sellables sellables_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.sellables
    ADD CONSTRAINT sellables_pkey PRIMARY KEY (id);


--
-- Name: software software_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.software
    ADD CONSTRAINT software_pkey PRIMARY KEY (product_id);


--
-- Name: tax_rules tax_rules_jurisdiction_id_effective_from_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.tax_rules
    ADD CONSTRAINT tax_rules_jurisdiction_id_effective_from_key UNIQUE (jurisdiction_id, effective_from);


--
-- Name: tax_rules tax_rules_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.tax_rules
    ADD CONSTRAINT tax_rules_pkey PRIMARY KEY (id);


--
-- Name: users users_email_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.users
    ADD CONSTRAINT users_email_key UNIQUE (email);


--
-- Name: users users_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.users
    ADD CONSTRAINT users_pkey PRIMARY KEY (id);


--
-- Name: video_game_ratings_by_locale video_game_ratings_by_locale_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.video_game_ratings_by_locale
    ADD CONSTRAINT video_game_ratings_by_locale_pkey PRIMARY KEY (id);


--
-- Name: video_game_ratings_by_locale video_game_ratings_by_locale_video_game_id_locale_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.video_game_ratings_by_locale
    ADD CONSTRAINT video_game_ratings_by_locale_video_game_id_locale_key UNIQUE (video_game_id, locale);


--
-- Name: video_game_sources video_game_sources_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.video_game_sources
    ADD CONSTRAINT video_game_sources_pkey PRIMARY KEY (id);


--
-- Name: video_game_title_sources video_game_title_sources_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.video_game_title_sources
    ADD CONSTRAINT video_game_title_sources_pkey PRIMARY KEY (id);


--
-- Name: video_game_titles video_game_titles_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.video_game_titles
    ADD CONSTRAINT video_game_titles_pkey PRIMARY KEY (id);


--
-- Name: video_game_titles video_game_titles_product_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.video_game_titles
    ADD CONSTRAINT video_game_titles_product_id_key UNIQUE (product_id);


--
-- Name: video_games video_games_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.video_games
    ADD CONSTRAINT video_games_pkey PRIMARY KEY (id);


--
-- Name: video_games video_games_slug_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.video_games
    ADD CONSTRAINT video_games_slug_key UNIQUE (slug);


--
-- Name: alerts_product_id_region_code_is_active_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX alerts_product_id_region_code_is_active_index ON public.alerts USING btree (product_id, region_code, is_active);


--
-- Name: alerts_user_id_is_active_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX alerts_user_id_is_active_index ON public.alerts USING btree (user_id, is_active);


--
-- Name: consoles_model_trgm_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX consoles_model_trgm_idx ON public.game_consoles USING gin (model ext.gin_trgm_ops);


--
-- Name: game_images_media_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX game_images_media_id_idx ON public.game_images USING btree (media_id);


--
-- Name: game_images_platforms_gin; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX game_images_platforms_gin ON public.game_images USING gin (platforms);


--
-- Name: game_images_provider_image_key_uq; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX game_images_provider_image_key_uq ON public.game_images USING btree (game_provider_id, image_key);


--
-- Name: game_images_provider_rank_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX game_images_provider_rank_idx ON public.game_images USING btree (game_provider_id, rank);


--
-- Name: game_providers_providable_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX game_providers_providable_idx ON public.game_providers USING btree (providable_type, providable_id);


--
-- Name: game_providers_providable_provider_key_uq; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX game_providers_providable_provider_key_uq ON public.game_providers USING btree (providable_type, providable_id, provider_key);


--
-- Name: game_providers_provider_key_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX game_providers_provider_key_idx ON public.game_providers USING btree (provider_key);


--
-- Name: game_providers_slug_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX game_providers_slug_idx ON public.game_providers USING btree (slug);


--
-- Name: game_providers_vgs_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX game_providers_vgs_idx ON public.game_providers USING btree (video_game_source_id);


--
-- Name: game_videos_media_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX game_videos_media_id_idx ON public.game_videos USING btree (media_id);


--
-- Name: game_videos_provider_published_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX game_videos_provider_published_idx ON public.game_videos USING btree (game_provider_id, published_at);


--
-- Name: game_videos_provider_video_key_uq; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX game_videos_provider_video_key_uq ON public.game_videos USING btree (game_provider_id, video_key);


--
-- Name: idx_exchange_rates_lookup; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_exchange_rates_lookup ON public.exchange_rates USING btree (base_currency, quote_currency, provider, fetched_at DESC);


--
-- Name: idx_provider_media_links_vg_type; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_provider_media_links_vg_type ON public.provider_media_links USING btree (video_game_id, media_type);


--
-- Name: idx_video_games_title_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_video_games_title_id ON public.video_games USING btree (title_id);


--
-- Name: offers_active_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX offers_active_idx ON public.offers USING btree (sellable_id) WHERE is_active;


--
-- Name: platforms_name_trgm_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX platforms_name_trgm_idx ON public.platforms USING gin (name ext.gin_trgm_ops);


--
-- Name: prices_series_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX prices_series_idx ON ONLY public.prices USING btree (offer_jurisdiction_id, recorded_at);


--
-- Name: prices_2024_11_offer_jurisdiction_id_recorded_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX prices_2024_11_offer_jurisdiction_id_recorded_at_idx ON public.prices_2024_11 USING btree (offer_jurisdiction_id, recorded_at);


--
-- Name: prices_2024_11_oj_recorded_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX prices_2024_11_oj_recorded_at ON public.prices_2024_11 USING btree (offer_jurisdiction_id, recorded_at);


--
-- Name: prices_2024_11_oj_recorded_at_desc; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX prices_2024_11_oj_recorded_at_desc ON public.prices_2024_11 USING btree (offer_jurisdiction_id, recorded_at DESC) INCLUDE (amount_minor);


--
-- Name: prices_2024_11_recorded_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX prices_2024_11_recorded_at ON public.prices_2024_11 USING btree (recorded_at);


--
-- Name: prices_2024_11_recorded_at_brin; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX prices_2024_11_recorded_at_brin ON public.prices_2024_11 USING brin (recorded_at);


--
-- Name: prices_recorded_at_brin; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX prices_recorded_at_brin ON ONLY public.prices USING brin (recorded_at);


--
-- Name: prices_2024_11_recorded_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX prices_2024_11_recorded_at_idx ON public.prices_2024_11 USING brin (recorded_at);


--
-- Name: prices_2024_12_offer_jurisdiction_id_recorded_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX prices_2024_12_offer_jurisdiction_id_recorded_at_idx ON public.prices_2024_12 USING btree (offer_jurisdiction_id, recorded_at);


--
-- Name: prices_2024_12_oj_recorded_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX prices_2024_12_oj_recorded_at ON public.prices_2024_12 USING btree (offer_jurisdiction_id, recorded_at);


--
-- Name: prices_2024_12_oj_recorded_at_desc; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX prices_2024_12_oj_recorded_at_desc ON public.prices_2024_12 USING btree (offer_jurisdiction_id, recorded_at DESC) INCLUDE (amount_minor);


--
-- Name: prices_2024_12_recorded_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX prices_2024_12_recorded_at ON public.prices_2024_12 USING btree (recorded_at);


--
-- Name: prices_2024_12_recorded_at_brin; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX prices_2024_12_recorded_at_brin ON public.prices_2024_12 USING brin (recorded_at);


--
-- Name: prices_2024_12_recorded_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX prices_2024_12_recorded_at_idx ON public.prices_2024_12 USING brin (recorded_at);


--
-- Name: prices_2025_01_offer_jurisdiction_id_recorded_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX prices_2025_01_offer_jurisdiction_id_recorded_at_idx ON public.prices_2025_01 USING btree (offer_jurisdiction_id, recorded_at);


--
-- Name: prices_2025_01_oj_recorded_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX prices_2025_01_oj_recorded_at ON public.prices_2025_01 USING btree (offer_jurisdiction_id, recorded_at);


--
-- Name: prices_2025_01_oj_recorded_at_desc; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX prices_2025_01_oj_recorded_at_desc ON public.prices_2025_01 USING btree (offer_jurisdiction_id, recorded_at DESC) INCLUDE (amount_minor);


--
-- Name: prices_2025_01_recorded_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX prices_2025_01_recorded_at ON public.prices_2025_01 USING btree (recorded_at);


--
-- Name: prices_2025_01_recorded_at_brin; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX prices_2025_01_recorded_at_brin ON public.prices_2025_01 USING brin (recorded_at);


--
-- Name: prices_2025_01_recorded_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX prices_2025_01_recorded_at_idx ON public.prices_2025_01 USING brin (recorded_at);


--
-- Name: prices_2025_02_offer_jurisdiction_id_recorded_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX prices_2025_02_offer_jurisdiction_id_recorded_at_idx ON public.prices_2025_02 USING btree (offer_jurisdiction_id, recorded_at);


--
-- Name: prices_2025_02_oj_recorded_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX prices_2025_02_oj_recorded_at ON public.prices_2025_02 USING btree (offer_jurisdiction_id, recorded_at);


--
-- Name: prices_2025_02_oj_recorded_at_desc; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX prices_2025_02_oj_recorded_at_desc ON public.prices_2025_02 USING btree (offer_jurisdiction_id, recorded_at DESC) INCLUDE (amount_minor);


--
-- Name: prices_2025_02_recorded_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX prices_2025_02_recorded_at ON public.prices_2025_02 USING btree (recorded_at);


--
-- Name: prices_2025_02_recorded_at_brin; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX prices_2025_02_recorded_at_brin ON public.prices_2025_02 USING brin (recorded_at);


--
-- Name: prices_2025_02_recorded_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX prices_2025_02_recorded_at_idx ON public.prices_2025_02 USING brin (recorded_at);


--
-- Name: prices_2025_03_offer_jurisdiction_id_recorded_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX prices_2025_03_offer_jurisdiction_id_recorded_at_idx ON public.prices_2025_03 USING btree (offer_jurisdiction_id, recorded_at);


--
-- Name: prices_2025_03_oj_recorded_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX prices_2025_03_oj_recorded_at ON public.prices_2025_03 USING btree (offer_jurisdiction_id, recorded_at);


--
-- Name: prices_2025_03_oj_recorded_at_desc; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX prices_2025_03_oj_recorded_at_desc ON public.prices_2025_03 USING btree (offer_jurisdiction_id, recorded_at DESC) INCLUDE (amount_minor);


--
-- Name: prices_2025_03_recorded_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX prices_2025_03_recorded_at ON public.prices_2025_03 USING btree (recorded_at);


--
-- Name: prices_2025_03_recorded_at_brin; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX prices_2025_03_recorded_at_brin ON public.prices_2025_03 USING brin (recorded_at);


--
-- Name: prices_2025_03_recorded_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX prices_2025_03_recorded_at_idx ON public.prices_2025_03 USING brin (recorded_at);


--
-- Name: prices_2025_04_offer_jurisdiction_id_recorded_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX prices_2025_04_offer_jurisdiction_id_recorded_at_idx ON public.prices_2025_04 USING btree (offer_jurisdiction_id, recorded_at);


--
-- Name: prices_2025_04_oj_recorded_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX prices_2025_04_oj_recorded_at ON public.prices_2025_04 USING btree (offer_jurisdiction_id, recorded_at);


--
-- Name: prices_2025_04_oj_recorded_at_desc; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX prices_2025_04_oj_recorded_at_desc ON public.prices_2025_04 USING btree (offer_jurisdiction_id, recorded_at DESC) INCLUDE (amount_minor);


--
-- Name: prices_2025_04_recorded_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX prices_2025_04_recorded_at ON public.prices_2025_04 USING btree (recorded_at);


--
-- Name: prices_2025_04_recorded_at_brin; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX prices_2025_04_recorded_at_brin ON public.prices_2025_04 USING brin (recorded_at);


--
-- Name: prices_2025_04_recorded_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX prices_2025_04_recorded_at_idx ON public.prices_2025_04 USING brin (recorded_at);


--
-- Name: prices_2025_05_offer_jurisdiction_id_recorded_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX prices_2025_05_offer_jurisdiction_id_recorded_at_idx ON public.prices_2025_05 USING btree (offer_jurisdiction_id, recorded_at);


--
-- Name: prices_2025_05_oj_recorded_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX prices_2025_05_oj_recorded_at ON public.prices_2025_05 USING btree (offer_jurisdiction_id, recorded_at);


--
-- Name: prices_2025_05_oj_recorded_at_desc; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX prices_2025_05_oj_recorded_at_desc ON public.prices_2025_05 USING btree (offer_jurisdiction_id, recorded_at DESC) INCLUDE (amount_minor);


--
-- Name: prices_2025_05_recorded_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX prices_2025_05_recorded_at ON public.prices_2025_05 USING btree (recorded_at);


--
-- Name: prices_2025_05_recorded_at_brin; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX prices_2025_05_recorded_at_brin ON public.prices_2025_05 USING brin (recorded_at);


--
-- Name: prices_2025_05_recorded_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX prices_2025_05_recorded_at_idx ON public.prices_2025_05 USING brin (recorded_at);


--
-- Name: prices_2025_06_offer_jurisdiction_id_recorded_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX prices_2025_06_offer_jurisdiction_id_recorded_at_idx ON public.prices_2025_06 USING btree (offer_jurisdiction_id, recorded_at);


--
-- Name: prices_2025_06_oj_recorded_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX prices_2025_06_oj_recorded_at ON public.prices_2025_06 USING btree (offer_jurisdiction_id, recorded_at);


--
-- Name: prices_2025_06_oj_recorded_at_desc; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX prices_2025_06_oj_recorded_at_desc ON public.prices_2025_06 USING btree (offer_jurisdiction_id, recorded_at DESC) INCLUDE (amount_minor);


--
-- Name: prices_2025_06_recorded_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX prices_2025_06_recorded_at ON public.prices_2025_06 USING btree (recorded_at);


--
-- Name: prices_2025_06_recorded_at_brin; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX prices_2025_06_recorded_at_brin ON public.prices_2025_06 USING brin (recorded_at);


--
-- Name: prices_2025_06_recorded_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX prices_2025_06_recorded_at_idx ON public.prices_2025_06 USING brin (recorded_at);


--
-- Name: prices_2025_07_offer_jurisdiction_id_recorded_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX prices_2025_07_offer_jurisdiction_id_recorded_at_idx ON public.prices_2025_07 USING btree (offer_jurisdiction_id, recorded_at);


--
-- Name: prices_2025_07_oj_recorded_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX prices_2025_07_oj_recorded_at ON public.prices_2025_07 USING btree (offer_jurisdiction_id, recorded_at);


--
-- Name: prices_2025_07_oj_recorded_at_desc; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX prices_2025_07_oj_recorded_at_desc ON public.prices_2025_07 USING btree (offer_jurisdiction_id, recorded_at DESC) INCLUDE (amount_minor);


--
-- Name: prices_2025_07_recorded_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX prices_2025_07_recorded_at ON public.prices_2025_07 USING btree (recorded_at);


--
-- Name: prices_2025_07_recorded_at_brin; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX prices_2025_07_recorded_at_brin ON public.prices_2025_07 USING brin (recorded_at);


--
-- Name: prices_2025_07_recorded_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX prices_2025_07_recorded_at_idx ON public.prices_2025_07 USING brin (recorded_at);


--
-- Name: prices_2025_08_offer_jurisdiction_id_recorded_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX prices_2025_08_offer_jurisdiction_id_recorded_at_idx ON public.prices_2025_08 USING btree (offer_jurisdiction_id, recorded_at);


--
-- Name: prices_2025_08_oj_recorded_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX prices_2025_08_oj_recorded_at ON public.prices_2025_08 USING btree (offer_jurisdiction_id, recorded_at);


--
-- Name: prices_2025_08_oj_recorded_at_desc; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX prices_2025_08_oj_recorded_at_desc ON public.prices_2025_08 USING btree (offer_jurisdiction_id, recorded_at DESC) INCLUDE (amount_minor);


--
-- Name: prices_2025_08_recorded_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX prices_2025_08_recorded_at ON public.prices_2025_08 USING btree (recorded_at);


--
-- Name: prices_2025_08_recorded_at_brin; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX prices_2025_08_recorded_at_brin ON public.prices_2025_08 USING brin (recorded_at);


--
-- Name: prices_2025_08_recorded_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX prices_2025_08_recorded_at_idx ON public.prices_2025_08 USING brin (recorded_at);


--
-- Name: prices_2025_09_offer_jurisdiction_id_recorded_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX prices_2025_09_offer_jurisdiction_id_recorded_at_idx ON public.prices_2025_09 USING btree (offer_jurisdiction_id, recorded_at);


--
-- Name: prices_2025_09_oj_recorded_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX prices_2025_09_oj_recorded_at ON public.prices_2025_09 USING btree (offer_jurisdiction_id, recorded_at);


--
-- Name: prices_2025_09_oj_recorded_at_desc; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX prices_2025_09_oj_recorded_at_desc ON public.prices_2025_09 USING btree (offer_jurisdiction_id, recorded_at DESC) INCLUDE (amount_minor);


--
-- Name: prices_2025_09_recorded_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX prices_2025_09_recorded_at ON public.prices_2025_09 USING btree (recorded_at);


--
-- Name: prices_2025_09_recorded_at_brin; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX prices_2025_09_recorded_at_brin ON public.prices_2025_09 USING brin (recorded_at);


--
-- Name: prices_2025_09_recorded_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX prices_2025_09_recorded_at_idx ON public.prices_2025_09 USING brin (recorded_at);


--
-- Name: prices_2025_10_offer_jurisdiction_id_recorded_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX prices_2025_10_offer_jurisdiction_id_recorded_at_idx ON public.prices_2025_10 USING btree (offer_jurisdiction_id, recorded_at);


--
-- Name: prices_2025_10_oj_recorded_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX prices_2025_10_oj_recorded_at ON public.prices_2025_10 USING btree (offer_jurisdiction_id, recorded_at);


--
-- Name: prices_2025_10_oj_recorded_at_desc; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX prices_2025_10_oj_recorded_at_desc ON public.prices_2025_10 USING btree (offer_jurisdiction_id, recorded_at DESC) INCLUDE (amount_minor);


--
-- Name: prices_2025_10_recorded_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX prices_2025_10_recorded_at ON public.prices_2025_10 USING btree (recorded_at);


--
-- Name: prices_2025_10_recorded_at_brin; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX prices_2025_10_recorded_at_brin ON public.prices_2025_10 USING brin (recorded_at);


--
-- Name: prices_2025_10_recorded_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX prices_2025_10_recorded_at_idx ON public.prices_2025_10 USING brin (recorded_at);


--
-- Name: prices_2025_11_offer_jurisdiction_id_recorded_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX prices_2025_11_offer_jurisdiction_id_recorded_at_idx ON public.prices_2025_11 USING btree (offer_jurisdiction_id, recorded_at);


--
-- Name: prices_2025_11_oj_recorded_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX prices_2025_11_oj_recorded_at ON public.prices_2025_11 USING btree (offer_jurisdiction_id, recorded_at);


--
-- Name: prices_2025_11_oj_recorded_at_desc; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX prices_2025_11_oj_recorded_at_desc ON public.prices_2025_11 USING btree (offer_jurisdiction_id, recorded_at DESC) INCLUDE (amount_minor);


--
-- Name: prices_2025_11_recorded_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX prices_2025_11_recorded_at ON public.prices_2025_11 USING btree (recorded_at);


--
-- Name: prices_2025_11_recorded_at_brin; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX prices_2025_11_recorded_at_brin ON public.prices_2025_11 USING brin (recorded_at);


--
-- Name: prices_2025_11_recorded_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX prices_2025_11_recorded_at_idx ON public.prices_2025_11 USING brin (recorded_at);


--
-- Name: prices_2025_12_offer_jurisdiction_id_recorded_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX prices_2025_12_offer_jurisdiction_id_recorded_at_idx ON public.prices_2025_12 USING btree (offer_jurisdiction_id, recorded_at);


--
-- Name: prices_2025_12_oj_recorded_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX prices_2025_12_oj_recorded_at ON public.prices_2025_12 USING btree (offer_jurisdiction_id, recorded_at);


--
-- Name: prices_2025_12_oj_recorded_at_desc; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX prices_2025_12_oj_recorded_at_desc ON public.prices_2025_12 USING btree (offer_jurisdiction_id, recorded_at DESC) INCLUDE (amount_minor);


--
-- Name: prices_2025_12_recorded_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX prices_2025_12_recorded_at ON public.prices_2025_12 USING btree (recorded_at);


--
-- Name: prices_2025_12_recorded_at_brin; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX prices_2025_12_recorded_at_brin ON public.prices_2025_12 USING brin (recorded_at);


--
-- Name: prices_2025_12_recorded_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX prices_2025_12_recorded_at_idx ON public.prices_2025_12 USING brin (recorded_at);


--
-- Name: prices_2026_01_offer_jurisdiction_id_recorded_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX prices_2026_01_offer_jurisdiction_id_recorded_at_idx ON public.prices_2026_01 USING btree (offer_jurisdiction_id, recorded_at);


--
-- Name: prices_2026_01_oj_recorded_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX prices_2026_01_oj_recorded_at ON public.prices_2026_01 USING btree (offer_jurisdiction_id, recorded_at);


--
-- Name: prices_2026_01_oj_recorded_at_desc; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX prices_2026_01_oj_recorded_at_desc ON public.prices_2026_01 USING btree (offer_jurisdiction_id, recorded_at DESC) INCLUDE (amount_minor);


--
-- Name: prices_2026_01_recorded_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX prices_2026_01_recorded_at ON public.prices_2026_01 USING btree (recorded_at);


--
-- Name: prices_2026_01_recorded_at_brin; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX prices_2026_01_recorded_at_brin ON public.prices_2026_01 USING brin (recorded_at);


--
-- Name: prices_2026_01_recorded_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX prices_2026_01_recorded_at_idx ON public.prices_2026_01 USING brin (recorded_at);


--
-- Name: prices_2026_02_offer_jurisdiction_id_recorded_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX prices_2026_02_offer_jurisdiction_id_recorded_at_idx ON public.prices_2026_02 USING btree (offer_jurisdiction_id, recorded_at);


--
-- Name: prices_2026_02_oj_recorded_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX prices_2026_02_oj_recorded_at ON public.prices_2026_02 USING btree (offer_jurisdiction_id, recorded_at);


--
-- Name: prices_2026_02_oj_recorded_at_desc; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX prices_2026_02_oj_recorded_at_desc ON public.prices_2026_02 USING btree (offer_jurisdiction_id, recorded_at DESC) INCLUDE (amount_minor);


--
-- Name: prices_2026_02_recorded_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX prices_2026_02_recorded_at ON public.prices_2026_02 USING btree (recorded_at);


--
-- Name: prices_2026_02_recorded_at_brin; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX prices_2026_02_recorded_at_brin ON public.prices_2026_02 USING brin (recorded_at);


--
-- Name: prices_2026_02_recorded_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX prices_2026_02_recorded_at_idx ON public.prices_2026_02 USING brin (recorded_at);


--
-- Name: prices_2026_03_offer_jurisdiction_id_recorded_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX prices_2026_03_offer_jurisdiction_id_recorded_at_idx ON public.prices_2026_03 USING btree (offer_jurisdiction_id, recorded_at);


--
-- Name: prices_2026_03_oj_recorded_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX prices_2026_03_oj_recorded_at ON public.prices_2026_03 USING btree (offer_jurisdiction_id, recorded_at);


--
-- Name: prices_2026_03_oj_recorded_at_desc; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX prices_2026_03_oj_recorded_at_desc ON public.prices_2026_03 USING btree (offer_jurisdiction_id, recorded_at DESC) INCLUDE (amount_minor);


--
-- Name: prices_2026_03_recorded_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX prices_2026_03_recorded_at ON public.prices_2026_03 USING btree (recorded_at);


--
-- Name: prices_2026_03_recorded_at_brin; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX prices_2026_03_recorded_at_brin ON public.prices_2026_03 USING brin (recorded_at);


--
-- Name: prices_2026_03_recorded_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX prices_2026_03_recorded_at_idx ON public.prices_2026_03 USING brin (recorded_at);


--
-- Name: prices_2026_04_offer_jurisdiction_id_recorded_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX prices_2026_04_offer_jurisdiction_id_recorded_at_idx ON public.prices_2026_04 USING btree (offer_jurisdiction_id, recorded_at);


--
-- Name: prices_2026_04_oj_recorded_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX prices_2026_04_oj_recorded_at ON public.prices_2026_04 USING btree (offer_jurisdiction_id, recorded_at);


--
-- Name: prices_2026_04_oj_recorded_at_desc; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX prices_2026_04_oj_recorded_at_desc ON public.prices_2026_04 USING btree (offer_jurisdiction_id, recorded_at DESC) INCLUDE (amount_minor);


--
-- Name: prices_2026_04_recorded_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX prices_2026_04_recorded_at ON public.prices_2026_04 USING btree (recorded_at);


--
-- Name: prices_2026_04_recorded_at_brin; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX prices_2026_04_recorded_at_brin ON public.prices_2026_04 USING brin (recorded_at);


--
-- Name: prices_2026_04_recorded_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX prices_2026_04_recorded_at_idx ON public.prices_2026_04 USING brin (recorded_at);


--
-- Name: prices_2026_05_offer_jurisdiction_id_recorded_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX prices_2026_05_offer_jurisdiction_id_recorded_at_idx ON public.prices_2026_05 USING btree (offer_jurisdiction_id, recorded_at);


--
-- Name: prices_2026_05_oj_recorded_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX prices_2026_05_oj_recorded_at ON public.prices_2026_05 USING btree (offer_jurisdiction_id, recorded_at);


--
-- Name: prices_2026_05_oj_recorded_at_desc; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX prices_2026_05_oj_recorded_at_desc ON public.prices_2026_05 USING btree (offer_jurisdiction_id, recorded_at DESC) INCLUDE (amount_minor);


--
-- Name: prices_2026_05_recorded_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX prices_2026_05_recorded_at ON public.prices_2026_05 USING btree (recorded_at);


--
-- Name: prices_2026_05_recorded_at_brin; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX prices_2026_05_recorded_at_brin ON public.prices_2026_05 USING brin (recorded_at);


--
-- Name: prices_2026_05_recorded_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX prices_2026_05_recorded_at_idx ON public.prices_2026_05 USING brin (recorded_at);


--
-- Name: products_name_trgm_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX products_name_trgm_idx ON public.products USING gin (name ext.gin_trgm_ops);


--
-- Name: products_slug_trgm_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX products_slug_trgm_idx ON public.products USING gin (((slug)::text) ext.gin_trgm_ops);


--
-- Name: provider_media_links_item_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX provider_media_links_item_idx ON public.provider_media_links USING btree (provider_item_id);


--
-- Name: provider_media_links_source_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX provider_media_links_source_idx ON public.provider_media_links USING btree (source);


--
-- Name: provider_media_links_type_role_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX provider_media_links_type_role_idx ON public.provider_media_links USING btree (media_type, role);


--
-- Name: provider_media_links_video_game_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX provider_media_links_video_game_idx ON public.provider_media_links USING btree (video_game_id);


--
-- Name: titles_title_trgm_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX titles_title_trgm_idx ON public.video_game_titles USING gin (title ext.gin_trgm_ops);


--
-- Name: uq_game_consoles_product_model_variant; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX uq_game_consoles_product_model_variant ON public.game_consoles USING btree (product_id, model, COALESCE(variant, ''::text));


--
-- Name: uq_offers_sellable_retailer_sku; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX uq_offers_sellable_retailer_sku ON public.offers USING btree (sellable_id, retailer_id, COALESCE(sku, ''::text));


--
-- Name: uq_provider_items_provider_external; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX uq_provider_items_provider_external ON public.provider_items USING btree (provider_id, external_id);


--
-- Name: uq_provider_media_links_item_url; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX uq_provider_media_links_item_url ON public.provider_media_links USING btree (provider_item_id, url);


--
-- Name: uq_vg_sellable_platform_edition; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX uq_vg_sellable_platform_edition ON public.video_games USING btree (sellable_id, platform_id, COALESCE(edition, ''::text)) WHERE (sellable_id IS NOT NULL);


--
-- Name: uq_vg_title_sources_vid_source_sid; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX uq_vg_title_sources_vid_source_sid ON public.video_game_title_sources USING btree (video_game_title_id, source, COALESCE(source_id, ''::text));


--
-- Name: uq_video_games_title_platform_edition; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX uq_video_games_title_platform_edition ON public.video_games USING btree (title_id, platform_id, COALESCE(edition, ''::text)) WHERE (edition IS NOT NULL);


--
-- Name: uq_video_games_title_platform_null; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX uq_video_games_title_platform_null ON public.video_games USING btree (title_id, platform_id) WHERE (edition IS NULL);


--
-- Name: video_game_sources_provider_game_uq; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX video_game_sources_provider_game_uq ON public.video_game_sources USING btree (provider, provider_game_id);


--
-- Name: video_game_sources_provider_key_uq; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX video_game_sources_provider_key_uq ON public.video_game_sources USING btree (provider_key);


--
-- Name: video_game_sources_slug_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX video_game_sources_slug_idx ON public.video_game_sources USING btree (slug);


--
-- Name: video_game_sources_video_game_provider_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX video_game_sources_video_game_provider_idx ON public.video_game_sources USING btree (video_game_id, provider);


--
-- Name: prices_2024_11_offer_jurisdiction_id_recorded_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.prices_series_idx ATTACH PARTITION public.prices_2024_11_offer_jurisdiction_id_recorded_at_idx;


--
-- Name: prices_2024_11_pkey; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.prices_pkey ATTACH PARTITION public.prices_2024_11_pkey;


--
-- Name: prices_2024_11_recorded_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.prices_recorded_at_brin ATTACH PARTITION public.prices_2024_11_recorded_at_idx;


--
-- Name: prices_2024_12_offer_jurisdiction_id_recorded_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.prices_series_idx ATTACH PARTITION public.prices_2024_12_offer_jurisdiction_id_recorded_at_idx;


--
-- Name: prices_2024_12_pkey; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.prices_pkey ATTACH PARTITION public.prices_2024_12_pkey;


--
-- Name: prices_2024_12_recorded_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.prices_recorded_at_brin ATTACH PARTITION public.prices_2024_12_recorded_at_idx;


--
-- Name: prices_2025_01_offer_jurisdiction_id_recorded_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.prices_series_idx ATTACH PARTITION public.prices_2025_01_offer_jurisdiction_id_recorded_at_idx;


--
-- Name: prices_2025_01_pkey; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.prices_pkey ATTACH PARTITION public.prices_2025_01_pkey;


--
-- Name: prices_2025_01_recorded_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.prices_recorded_at_brin ATTACH PARTITION public.prices_2025_01_recorded_at_idx;


--
-- Name: prices_2025_02_offer_jurisdiction_id_recorded_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.prices_series_idx ATTACH PARTITION public.prices_2025_02_offer_jurisdiction_id_recorded_at_idx;


--
-- Name: prices_2025_02_pkey; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.prices_pkey ATTACH PARTITION public.prices_2025_02_pkey;


--
-- Name: prices_2025_02_recorded_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.prices_recorded_at_brin ATTACH PARTITION public.prices_2025_02_recorded_at_idx;


--
-- Name: prices_2025_03_offer_jurisdiction_id_recorded_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.prices_series_idx ATTACH PARTITION public.prices_2025_03_offer_jurisdiction_id_recorded_at_idx;


--
-- Name: prices_2025_03_pkey; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.prices_pkey ATTACH PARTITION public.prices_2025_03_pkey;


--
-- Name: prices_2025_03_recorded_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.prices_recorded_at_brin ATTACH PARTITION public.prices_2025_03_recorded_at_idx;


--
-- Name: prices_2025_04_offer_jurisdiction_id_recorded_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.prices_series_idx ATTACH PARTITION public.prices_2025_04_offer_jurisdiction_id_recorded_at_idx;


--
-- Name: prices_2025_04_pkey; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.prices_pkey ATTACH PARTITION public.prices_2025_04_pkey;


--
-- Name: prices_2025_04_recorded_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.prices_recorded_at_brin ATTACH PARTITION public.prices_2025_04_recorded_at_idx;


--
-- Name: prices_2025_05_offer_jurisdiction_id_recorded_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.prices_series_idx ATTACH PARTITION public.prices_2025_05_offer_jurisdiction_id_recorded_at_idx;


--
-- Name: prices_2025_05_pkey; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.prices_pkey ATTACH PARTITION public.prices_2025_05_pkey;


--
-- Name: prices_2025_05_recorded_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.prices_recorded_at_brin ATTACH PARTITION public.prices_2025_05_recorded_at_idx;


--
-- Name: prices_2025_06_offer_jurisdiction_id_recorded_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.prices_series_idx ATTACH PARTITION public.prices_2025_06_offer_jurisdiction_id_recorded_at_idx;


--
-- Name: prices_2025_06_pkey; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.prices_pkey ATTACH PARTITION public.prices_2025_06_pkey;


--
-- Name: prices_2025_06_recorded_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.prices_recorded_at_brin ATTACH PARTITION public.prices_2025_06_recorded_at_idx;


--
-- Name: prices_2025_07_offer_jurisdiction_id_recorded_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.prices_series_idx ATTACH PARTITION public.prices_2025_07_offer_jurisdiction_id_recorded_at_idx;


--
-- Name: prices_2025_07_pkey; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.prices_pkey ATTACH PARTITION public.prices_2025_07_pkey;


--
-- Name: prices_2025_07_recorded_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.prices_recorded_at_brin ATTACH PARTITION public.prices_2025_07_recorded_at_idx;


--
-- Name: prices_2025_08_offer_jurisdiction_id_recorded_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.prices_series_idx ATTACH PARTITION public.prices_2025_08_offer_jurisdiction_id_recorded_at_idx;


--
-- Name: prices_2025_08_pkey; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.prices_pkey ATTACH PARTITION public.prices_2025_08_pkey;


--
-- Name: prices_2025_08_recorded_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.prices_recorded_at_brin ATTACH PARTITION public.prices_2025_08_recorded_at_idx;


--
-- Name: prices_2025_09_offer_jurisdiction_id_recorded_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.prices_series_idx ATTACH PARTITION public.prices_2025_09_offer_jurisdiction_id_recorded_at_idx;


--
-- Name: prices_2025_09_pkey; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.prices_pkey ATTACH PARTITION public.prices_2025_09_pkey;


--
-- Name: prices_2025_09_recorded_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.prices_recorded_at_brin ATTACH PARTITION public.prices_2025_09_recorded_at_idx;


--
-- Name: prices_2025_10_offer_jurisdiction_id_recorded_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.prices_series_idx ATTACH PARTITION public.prices_2025_10_offer_jurisdiction_id_recorded_at_idx;


--
-- Name: prices_2025_10_pkey; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.prices_pkey ATTACH PARTITION public.prices_2025_10_pkey;


--
-- Name: prices_2025_10_recorded_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.prices_recorded_at_brin ATTACH PARTITION public.prices_2025_10_recorded_at_idx;


--
-- Name: prices_2025_11_offer_jurisdiction_id_recorded_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.prices_series_idx ATTACH PARTITION public.prices_2025_11_offer_jurisdiction_id_recorded_at_idx;


--
-- Name: prices_2025_11_pkey; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.prices_pkey ATTACH PARTITION public.prices_2025_11_pkey;


--
-- Name: prices_2025_11_recorded_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.prices_recorded_at_brin ATTACH PARTITION public.prices_2025_11_recorded_at_idx;


--
-- Name: prices_2025_12_offer_jurisdiction_id_recorded_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.prices_series_idx ATTACH PARTITION public.prices_2025_12_offer_jurisdiction_id_recorded_at_idx;


--
-- Name: prices_2025_12_pkey; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.prices_pkey ATTACH PARTITION public.prices_2025_12_pkey;


--
-- Name: prices_2025_12_recorded_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.prices_recorded_at_brin ATTACH PARTITION public.prices_2025_12_recorded_at_idx;


--
-- Name: prices_2026_01_offer_jurisdiction_id_recorded_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.prices_series_idx ATTACH PARTITION public.prices_2026_01_offer_jurisdiction_id_recorded_at_idx;


--
-- Name: prices_2026_01_pkey; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.prices_pkey ATTACH PARTITION public.prices_2026_01_pkey;


--
-- Name: prices_2026_01_recorded_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.prices_recorded_at_brin ATTACH PARTITION public.prices_2026_01_recorded_at_idx;


--
-- Name: prices_2026_02_offer_jurisdiction_id_recorded_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.prices_series_idx ATTACH PARTITION public.prices_2026_02_offer_jurisdiction_id_recorded_at_idx;


--
-- Name: prices_2026_02_pkey; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.prices_pkey ATTACH PARTITION public.prices_2026_02_pkey;


--
-- Name: prices_2026_02_recorded_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.prices_recorded_at_brin ATTACH PARTITION public.prices_2026_02_recorded_at_idx;


--
-- Name: prices_2026_03_offer_jurisdiction_id_recorded_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.prices_series_idx ATTACH PARTITION public.prices_2026_03_offer_jurisdiction_id_recorded_at_idx;


--
-- Name: prices_2026_03_pkey; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.prices_pkey ATTACH PARTITION public.prices_2026_03_pkey;


--
-- Name: prices_2026_03_recorded_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.prices_recorded_at_brin ATTACH PARTITION public.prices_2026_03_recorded_at_idx;


--
-- Name: prices_2026_04_offer_jurisdiction_id_recorded_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.prices_series_idx ATTACH PARTITION public.prices_2026_04_offer_jurisdiction_id_recorded_at_idx;


--
-- Name: prices_2026_04_pkey; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.prices_pkey ATTACH PARTITION public.prices_2026_04_pkey;


--
-- Name: prices_2026_04_recorded_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.prices_recorded_at_brin ATTACH PARTITION public.prices_2026_04_recorded_at_idx;


--
-- Name: prices_2026_05_offer_jurisdiction_id_recorded_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.prices_series_idx ATTACH PARTITION public.prices_2026_05_offer_jurisdiction_id_recorded_at_idx;


--
-- Name: prices_2026_05_pkey; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.prices_pkey ATTACH PARTITION public.prices_2026_05_pkey;


--
-- Name: prices_2026_05_recorded_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.prices_recorded_at_brin ATTACH PARTITION public.prices_2026_05_recorded_at_idx;


--
-- Name: countries countries_iso_autofill_trg; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER countries_iso_autofill_trg BEFORE INSERT ON public.countries FOR EACH ROW EXECUTE FUNCTION public.countries_iso_autofill();


--
-- Name: alerts alerts_product_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.alerts
    ADD CONSTRAINT alerts_product_id_fkey FOREIGN KEY (product_id) REFERENCES public.products(id) ON DELETE CASCADE;


--
-- Name: alerts alerts_user_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.alerts
    ADD CONSTRAINT alerts_user_id_fkey FOREIGN KEY (user_id) REFERENCES public.users(id) ON DELETE CASCADE;


--
-- Name: countries countries_currency_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.countries
    ADD CONSTRAINT countries_currency_id_fkey FOREIGN KEY (currency_id) REFERENCES public.currencies(id);


--
-- Name: current_price current_price_offer_jurisdiction_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.current_price
    ADD CONSTRAINT current_price_offer_jurisdiction_id_fkey FOREIGN KEY (offer_jurisdiction_id) REFERENCES public.offer_jurisdictions(id) ON DELETE CASCADE;


--
-- Name: game_consoles game_consoles_product_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.game_consoles
    ADD CONSTRAINT game_consoles_product_id_fkey FOREIGN KEY (product_id) REFERENCES public.hardware(product_id) ON DELETE CASCADE;


--
-- Name: game_images game_images_game_provider_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.game_images
    ADD CONSTRAINT game_images_game_provider_id_fkey FOREIGN KEY (game_provider_id) REFERENCES public.game_providers(id) ON DELETE CASCADE;


--
-- Name: game_images game_images_video_game_source_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.game_images
    ADD CONSTRAINT game_images_video_game_source_id_fkey FOREIGN KEY (video_game_source_id) REFERENCES public.video_game_sources(id) ON DELETE SET NULL;


--
-- Name: game_providers game_providers_video_game_source_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.game_providers
    ADD CONSTRAINT game_providers_video_game_source_id_fkey FOREIGN KEY (video_game_source_id) REFERENCES public.video_game_sources(id) ON DELETE SET NULL;


--
-- Name: game_videos game_videos_game_provider_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.game_videos
    ADD CONSTRAINT game_videos_game_provider_id_fkey FOREIGN KEY (game_provider_id) REFERENCES public.game_providers(id) ON DELETE CASCADE;


--
-- Name: game_videos game_videos_video_game_source_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.game_videos
    ADD CONSTRAINT game_videos_video_game_source_id_fkey FOREIGN KEY (video_game_source_id) REFERENCES public.video_game_sources(id) ON DELETE SET NULL;


--
-- Name: hardware hardware_product_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.hardware
    ADD CONSTRAINT hardware_product_id_fkey FOREIGN KEY (product_id) REFERENCES public.products(id) ON DELETE CASCADE;


--
-- Name: jurisdictions jurisdictions_country_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.jurisdictions
    ADD CONSTRAINT jurisdictions_country_id_fkey FOREIGN KEY (country_id) REFERENCES public.countries(id) ON DELETE CASCADE;


--
-- Name: offer_jurisdictions offer_jurisdictions_currency_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.offer_jurisdictions
    ADD CONSTRAINT offer_jurisdictions_currency_id_fkey FOREIGN KEY (currency_id) REFERENCES public.currencies(id);


--
-- Name: offer_jurisdictions offer_jurisdictions_jurisdiction_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.offer_jurisdictions
    ADD CONSTRAINT offer_jurisdictions_jurisdiction_id_fkey FOREIGN KEY (jurisdiction_id) REFERENCES public.jurisdictions(id) ON DELETE CASCADE;


--
-- Name: offer_jurisdictions offer_jurisdictions_offer_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.offer_jurisdictions
    ADD CONSTRAINT offer_jurisdictions_offer_id_fkey FOREIGN KEY (offer_id) REFERENCES public.offers(id) ON DELETE CASCADE;


--
-- Name: offers offers_retailer_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.offers
    ADD CONSTRAINT offers_retailer_id_fkey FOREIGN KEY (retailer_id) REFERENCES public.retailers(id) ON DELETE CASCADE;


--
-- Name: offers offers_sellable_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.offers
    ADD CONSTRAINT offers_sellable_id_fkey FOREIGN KEY (sellable_id) REFERENCES public.sellables(id) ON DELETE CASCADE;


--
-- Name: prices prices_offer_jurisdiction_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE public.prices
    ADD CONSTRAINT prices_offer_jurisdiction_id_fkey FOREIGN KEY (offer_jurisdiction_id) REFERENCES public.offer_jurisdictions(id) ON DELETE CASCADE;


--
-- Name: prices prices_provider_item_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE public.prices
    ADD CONSTRAINT prices_provider_item_id_fkey FOREIGN KEY (provider_item_id) REFERENCES public.provider_items(id) ON DELETE SET NULL;


--
-- Name: provider_ingest_runs provider_ingest_runs_provider_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.provider_ingest_runs
    ADD CONSTRAINT provider_ingest_runs_provider_id_fkey FOREIGN KEY (provider_id) REFERENCES public.providers(id) ON DELETE CASCADE;


--
-- Name: provider_items provider_items_provider_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.provider_items
    ADD CONSTRAINT provider_items_provider_id_fkey FOREIGN KEY (provider_id) REFERENCES public.providers(id) ON DELETE CASCADE;


--
-- Name: provider_media_links provider_media_links_provider_item_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.provider_media_links
    ADD CONSTRAINT provider_media_links_provider_item_id_fkey FOREIGN KEY (provider_item_id) REFERENCES public.provider_items(id) ON DELETE CASCADE;


--
-- Name: provider_media_links provider_media_links_video_game_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.provider_media_links
    ADD CONSTRAINT provider_media_links_video_game_id_fkey FOREIGN KEY (video_game_id) REFERENCES public.video_games(id) ON DELETE SET NULL;


--
-- Name: provider_offers provider_offers_offer_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.provider_offers
    ADD CONSTRAINT provider_offers_offer_id_fkey FOREIGN KEY (offer_id) REFERENCES public.offers(id) ON DELETE SET NULL;


--
-- Name: provider_offers provider_offers_provider_item_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.provider_offers
    ADD CONSTRAINT provider_offers_provider_item_id_fkey FOREIGN KEY (provider_item_id) REFERENCES public.provider_items(id) ON DELETE CASCADE;


--
-- Name: retailer_providers retailer_providers_provider_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.retailer_providers
    ADD CONSTRAINT retailer_providers_provider_id_fkey FOREIGN KEY (provider_id) REFERENCES public.providers(id) ON DELETE CASCADE;


--
-- Name: retailer_providers retailer_providers_retailer_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.retailer_providers
    ADD CONSTRAINT retailer_providers_retailer_id_fkey FOREIGN KEY (retailer_id) REFERENCES public.retailers(id) ON DELETE CASCADE;


--
-- Name: sellables sellables_console_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.sellables
    ADD CONSTRAINT sellables_console_id_fkey FOREIGN KEY (console_id) REFERENCES public.game_consoles(id) ON DELETE CASCADE;


--
-- Name: sellables sellables_software_title_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.sellables
    ADD CONSTRAINT sellables_software_title_id_fkey FOREIGN KEY (software_title_id) REFERENCES public.video_game_titles(id) ON DELETE CASCADE;


--
-- Name: software software_product_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.software
    ADD CONSTRAINT software_product_id_fkey FOREIGN KEY (product_id) REFERENCES public.products(id) ON DELETE CASCADE;


--
-- Name: tax_rules tax_rules_jurisdiction_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.tax_rules
    ADD CONSTRAINT tax_rules_jurisdiction_id_fkey FOREIGN KEY (jurisdiction_id) REFERENCES public.jurisdictions(id) ON DELETE CASCADE;


--
-- Name: video_game_ratings_by_locale video_game_ratings_by_locale_video_game_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.video_game_ratings_by_locale
    ADD CONSTRAINT video_game_ratings_by_locale_video_game_id_fkey FOREIGN KEY (video_game_id) REFERENCES public.video_games(id) ON DELETE CASCADE;


--
-- Name: video_game_sources video_game_sources_video_game_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.video_game_sources
    ADD CONSTRAINT video_game_sources_video_game_id_fkey FOREIGN KEY (video_game_id) REFERENCES public.video_games(id) ON DELETE SET NULL;


--
-- Name: video_game_title_sources video_game_title_sources_video_game_title_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.video_game_title_sources
    ADD CONSTRAINT video_game_title_sources_video_game_title_id_fkey FOREIGN KEY (video_game_title_id) REFERENCES public.video_game_titles(id) ON DELETE CASCADE;


--
-- Name: video_game_titles video_game_titles_product_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.video_game_titles
    ADD CONSTRAINT video_game_titles_product_id_fkey FOREIGN KEY (product_id) REFERENCES public.software(product_id) ON DELETE CASCADE;


--
-- Name: video_game_titles video_game_titles_video_game_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.video_game_titles
    ADD CONSTRAINT video_game_titles_video_game_id_fkey FOREIGN KEY (video_game_id) REFERENCES public.video_games(id);


--
-- Name: video_games video_games_platform_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.video_games
    ADD CONSTRAINT video_games_platform_id_fkey FOREIGN KEY (platform_id) REFERENCES public.platforms(id);


--
-- PostgreSQL database dump complete
--

\unrestrict txxX0DnC5F08EaNy11azVW8xrVVz5exT1yJxznWXgLqRBigBinicqIZoMsnhKHW

