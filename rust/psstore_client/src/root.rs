use std::{ collections::HashMap, sync::Arc, time::Duration };
use std::fs;
use std::io::Write;
use std::path::{ Path, PathBuf };
use std::hash::{ Hash, Hasher };
// (no need for UNIX_EPOCH; using metadata.modified().elapsed())
use governor::{ Quota, RateLimiter, state::keyed::DashMapStateStore, clock::DefaultClock };
use reqwest::Client;
use std::net::{ ToSocketAddrs, SocketAddr };
use hickory_resolver::{
    TokioAsyncResolver,
    config::{ ResolverConfig, ResolverOpts, LookupIpStrategy },
};
use thiserror::Error;
use tokio::sync::Mutex;
use dotenv::dotenv;
use reqwest::header::{ HeaderMap, HeaderName, HeaderValue, CONTENT_TYPE };
// backoff not currently used; simple retry implemented
use serde_json::Value;
use serde::{ Serialize, Deserialize };
use tracing::{ info, warn, error, debug };
use std::time::{ Instant, SystemTime, UNIX_EPOCH };

#[derive(Clone, Debug)]
pub struct PageTask {
    pub locale: String,
    pub page: u32,
    pub size: u32,
}

#[derive(Clone, Debug)]
pub struct ProviderItemIn {
    pub provider_key: String,
    pub external_item_id: String,
    pub external_sku: Option<String>,
    pub title: Option<String>,
}

#[derive(Clone, Debug)]
pub struct OfferIn {
    pub sellable_id: i64,
    pub retailer_id: i64,
    pub sku: Option<String>,
}

#[derive(Clone, Debug)]
pub struct OfferJurisdictionIn {
    pub offer_temp_key: String,
    pub jurisdiction_id: i64,
    pub currency_id: i64,
}

#[derive(Clone, Debug)]
pub struct PricePointIn {
    pub offer_jurisdiction_id: i64,
    pub provider_item_id: Option<i64>,
    pub recorded_at: chrono::DateTime<chrono::Utc>,
    pub amount_minor: i64,
    pub tax_inclusive: bool,
    pub fx_minor_per_unit: Option<i64>,
    pub btc_sats_per_unit: Option<i64>,
}

pub fn get_env(key: &str) -> String {
    std::env::var(key).unwrap_or_else(|err| panic!("Missing env. key={key}, error={err}"))
}

#[derive(Clone, Debug)]
pub struct PsConfig {
    pub base_url: String,
    pub bearer: Option<String>,
    pub locales: Vec<String>,
    pub rps: u32,
    pub extra_headers: HashMap<String, String>,
    pub retry_attempts: u32,
    pub retry_base_delay_ms: u64,
    pub cookie: Option<String>,
    // IPv6/Proxy opts
    pub ipv6_only: bool,
    pub proxy: Option<String>,
}

impl Default for PsConfig {
    fn default() -> Self {
        dotenv().ok();
        let regions_raw = get_env("PS_STORE_REGIONS");
        let regions: Vec<String> = regions_raw
            .split(|c: char| (c == ',' || c == ' '))
            .filter(|s| !s.is_empty())
            .map(|s| s.trim().to_lowercase())
            .collect();
        let bearer = std::env
            ::var("PS_STORE_BEARER")
            .ok()
            .filter(|s| !s.is_empty())
            .or_else(||
                std::env
                    ::var("PS_STORE_API_KEY")
                    .ok()
                    .filter(|s| !s.is_empty())
            );
        let rps = std::env
            ::var("PS_RPS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(3);
        let retry_attempts = std::env
            ::var("PS_RETRY_ATTEMPTS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(5);
        let retry_base_delay_ms = std::env
            ::var("PS_RETRY_BASE_DELAY_MS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(2000);

        // Optional cookie bootstrap (Akamai/_abck/bm values captured from browser)
        let cookie = std::env
            ::var("PS_STORE_COOKIE_FILE")
            .ok()
            .and_then(|p|
                std::fs
                    ::read_to_string(p)
                    .ok()
                    .map(|s| s.trim().to_string())
            )
            .or_else(||
                std::env
                    ::var("PS_STORE_COOKIE")
                    .ok()
                    .map(|s| s.trim().to_string())
            )
            .filter(|s| !s.is_empty());

        // IPv6-by-default:
        // - We default to IPv6-only for PS Store HTTP to avoid v4/v6 split-brain issues.
        // - You can explicitly disable with PS_IPV6_ONLY=0 (or false/no/off) OR PS_DISABLE_IPV6=1.
        // - You can explicitly force-enable with PS_IPV6_ONLY=1.
        let disable_ipv6 = std::env::var("PS_DISABLE_IPV6")
            .ok()
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true") || v.eq_ignore_ascii_case("yes"))
            .unwrap_or(false);
        let ipv6_only = if disable_ipv6 {
            false
        } else {
            match std::env::var("PS_IPV6_ONLY").ok().as_deref() {
                None => true,
                Some(v) if v == "1" || v.eq_ignore_ascii_case("true") || v.eq_ignore_ascii_case("yes") || v.eq_ignore_ascii_case("on") => true,
                Some(v) if v == "0" || v.eq_ignore_ascii_case("false") || v.eq_ignore_ascii_case("no") || v.eq_ignore_ascii_case("off") => false,
                Some(_) => true,
            }
        };
        Self {
            base_url: "https://web.np.playstation.com/api/graphql/v1/".into(),
            bearer,
            locales: regions,
            rps,
            extra_headers: HashMap::new(),
            retry_attempts,
            retry_base_delay_ms,
            cookie,
            ipv6_only,
            proxy: std::env::var("PS_PROXY").ok(),
        }
    }
}

/// Normalize a locale string into "ll-CC" form used for headers and hash map keys.
fn normalize_locale_key(s: &str) -> String {
    let t = s.replace('_', "-");
    if let Some((ll, cc)) = t.split_once('-') {
        format!("{}-{}", ll.to_ascii_lowercase(), cc.to_ascii_uppercase())
    } else {
        // If country part missing, default to lower-case language only
        t.to_ascii_lowercase()
    }
}

#[derive(Error, Debug)]
pub enum PsError {
    #[error("http {status}: {body}")] Http {
        status: u16,
        body: String,
    },
    #[error("network: {0}")] Net(#[from] reqwest::Error),
    #[error("json: {0}")] Json(#[from] serde_json::Error),
    #[error("other: {0}")] Other(String),
}

#[derive(Clone)]
pub struct PsStoreClient {
    http: Client,
    cfg: Arc<PsConfig>,
    limiter: Arc<RateLimiter<String, DashMapStateStore<String>, DefaultClock>>,
    #[allow(dead_code)]
    etags: Arc<Mutex<HashMap<String, String>>>,
    #[allow(dead_code)]
    resolver_v6: Option<TokioAsyncResolver>,
}

impl PsStoreClient {
    /// Detect known PS Store Elasticsearch shard failures (GraphQL error code 3165954 or message contains "all shards failed").
    fn is_es_shard_failure(v: &Value) -> bool {
        v.get("errors")
            .and_then(|e| e.as_array())
            .map(|errs| {
                errs.iter().any(|err| {
                    let msg = err
                        .get("message")
                        .and_then(|m| m.as_str())
                        .unwrap_or("")
                        .to_lowercase();
                    let code_i64 = err
                        .get("extensions")
                        .and_then(|ex| ex.get("code"))
                        .and_then(|c| c.as_i64());
                    msg.contains("all shards failed") || code_i64 == Some(3165954)
                })
            })
            .unwrap_or(false)
    }
    // --- Dynamic persisted hash loader -------------------------------------------------------
    // We keep a static fallback map (identical to the previous hard-coded version) but prefer
    // hashes parsed from the authoritative Postman collection file at runtime so drift is
    // eliminated. If the file cannot be read/parsed we quietly fall back to the static map.
    fn static_hash_fallback(op: &str) -> &'static str {
        match op {
            "categoryGridRetrieve" =>
                "9845afc0dbaab4965f6563fffc703f588c8e76792000e8610843b8d3ee9c4c09",
            "metGetProductById" =>
                "a128042177bd93dd831164103d53b73ef790d56f51dae647064cb8f9d9fc9d1a",
            "metGetConceptById" =>
                "cc90404ac049d935afbd9968aef523da2b6723abfb9d586e5f77ebf7c5289006",
            "metGetConceptByProductIdQuery" =>
                "0a4c9f3693b3604df1c8341fdc3e481f42eeecf961a996baaa65e65a657a6433",
            "metGetPricingDataByConceptId" =>
                "abcb311ea830e679fe2b697a27f755764535d825b24510ab1239a4ca3092bd09",
            "wcaProductStarRatingRetrieve" =>
                "cedd370c39e89da20efa7b2e55710e88cb6e6843cc2f8203f7e73ba4751e7253",
            "wcaConceptStarRatingRetrieve" =>
                "e12dc5cef72296a437b4d71e0b130010bf3707ab981b585ba00d1d5773ce2092",
            "metGetAddOnsByTitleId" =>
                "e98d01ff5c1854409a405a5f79b5a9bcd36a5c0679fb33f4e18113c157d4d916",
            "featuresRetrieve" =>
                "010870e8b9269c5bcf06b60190edbf5229310d8fae5b86515ad73f05bd11c4d1",
            _ => "",
        }
    }
    fn dynamic_hash_map() -> &'static std::sync::OnceLock<HashMap<String, String>> {
        static HASHES: std::sync::OnceLock<HashMap<String, String>> = std::sync::OnceLock::new();
        &HASHES
    }
    fn ensure_hashes_loaded() {
        if Self::dynamic_hash_map().get().is_some() {
            return;
        }
        // First, try dedicated override file with per-locale mappings
        if
            let Ok(text) = std::fs
                ::read_to_string("psstore_client/hashes.json")
                .or_else(|_| std::fs::read_to_string("hashes.json"))
        {
            if let Ok(v) = serde_json::from_str::<serde_json::Value>(&text) {
                let mut map = HashMap::new();
                if let Some(obj) = v.as_object() {
                    for (locale_key, ops_val) in obj {
                        let norm_locale = normalize_locale_key(locale_key);
                        if let Some(ops_obj) = ops_val.as_object() {
                            for (op_name, hash_val) in ops_obj {
                                // Support two schemas:
                                // 1) direct string: "operationName": "<sha256>"
                                // 2) object wrapper: "operationName": { "sha256Hash": "<sha256>" }
                                let extracted: Option<String> = if let Some(s) = hash_val.as_str() {
                                    Some(s.to_string())
                                } else if let Some(inner_obj) = hash_val.as_object() {
                                    inner_obj
                                        .get("sha256Hash")
                                        .and_then(|x| x.as_str())
                                        .map(|s| s.to_string())
                                } else {
                                    None
                                };
                                if let Some(hash) = extracted {
                                    let trimmed = hash.trim();
                                    if
                                        !trimmed.is_empty() &&
                                        trimmed.chars().all(|c| c.is_ascii_hexdigit())
                                    {
                                        map.insert(
                                            format!("{}::{}", norm_locale, op_name),
                                            trimmed.to_string()
                                        );
                                    } else {
                                        warn!(locale=%norm_locale, op=%op_name, value=%trimmed, "ps dynamic hash loader: invalid hash format (ignored)");
                                    }
                                }
                            }
                        }
                    }
                }
                let count = map.len();
                Self::dynamic_hash_map().set(map).ok();
                info!(count=%count, "ps dynamic hash loader initialized from hashes.json");
                return;
            } else {
                warn!(
                    "ps dynamic hash loader: hashes.json parse failed; will try Postman collection or fallback"
                );
            }
        }

        // Next, try the Postman collection export (variables + items)
        let candidates = [
            "psstore_client/psstore_api_collection.json",
            "psstore_api_collection.json",
        ];
        for path in candidates.iter() {
            if let Ok(text) = std::fs::read_to_string(path) {
                match serde_json::from_str::<serde_json::Value>(&text) {
                    Ok(v) => {
                        let mut map = HashMap::new();
                        if let Some(vars) = v.get("variable").and_then(|x| x.as_array()) {
                            for var in vars {
                                if
                                    let (Some(k), Some(val)) = (
                                        var.get("key").and_then(|x| x.as_str()),
                                        var.get("value").and_then(|x| x.as_str()),
                                    )
                                {
                                    map.insert(format!("__var::{k}"), val.to_string());
                                }
                            }
                        }
                        fn walk(items: &Vec<serde_json::Value>, map: &mut HashMap<String, String>) {
                            for it in items {
                                if let Some(sub) = it.get("item").and_then(|x| x.as_array()) {
                                    walk(sub, map);
                                }
                                if let Some(req) = it.get("request") {
                                    if let Some(url) = req.get("url") {
                                        if
                                            let Some(qs) = url
                                                .get("query")
                                                .and_then(|x| x.as_array())
                                        {
                                            let mut op_name: Option<String> = None;
                                            let mut ext_raw: Option<String> = None;
                                            for q in qs {
                                                if
                                                    q.get("key").and_then(|x| x.as_str()) ==
                                                    Some("operationName")
                                                {
                                                    op_name = q
                                                        .get("value")
                                                        .and_then(|x| x.as_str())
                                                        .map(|s| s.to_string());
                                                } else if
                                                    q.get("key").and_then(|x| x.as_str()) ==
                                                    Some("extensions")
                                                {
                                                    ext_raw = q
                                                        .get("value")
                                                        .and_then(|x| x.as_str())
                                                        .map(|s| s.to_string());
                                                }
                                            }
                                            if let (Some(op), Some(ext)) = (op_name, ext_raw) {
                                                if let Some(pos) = ext.find("sha256Hash\":\"") {
                                                    let start = pos + "sha256Hash\":\"".len();
                                                    if let Some(end_rel) = ext[start..].find('"') {
                                                        let raw_hash = &ext[start..start + end_rel];
                                                        let resolved = if
                                                            raw_hash.starts_with("{{") &&
                                                            raw_hash.ends_with("}}")
                                                        {
                                                            let key =
                                                                &raw_hash[2..raw_hash.len() - 2];
                                                            map.get(&format!("__var::{key}"))
                                                                .cloned()
                                                                .unwrap_or_else(||
                                                                    raw_hash.to_string()
                                                                )
                                                        } else {
                                                            raw_hash.to_string()
                                                        };
                                                        if
                                                            !resolved.is_empty() &&
                                                            resolved
                                                                .chars()
                                                                .all(|c| c.is_ascii_hexdigit())
                                                        {
                                                            map.insert(op.clone(), resolved);
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        if let Some(items) = v.get("item").and_then(|x| x.as_array()) {
                            walk(items, &mut map);
                        }
                        map.retain(|k, _| !k.starts_with("__var::"));
                        let count = map.len();
                        Self::dynamic_hash_map().set(map).ok();
                        info!(path=%path, count=%count, "ps dynamic hash loader initialized");
                        return;
                    }
                    Err(e) => {
                        warn!(path=%path, error=?e, "ps dynamic hash loader parse failed; will try next path or fallback");
                    }
                }
            }
        }
        Self::dynamic_hash_map().set(HashMap::new()).ok();
        warn!("ps dynamic hash loader using static fallback map (file not found or invalid)");
    }
    fn persisted_hash_for(op: &str, locale: &str) -> String {
        // Priority 0: Dedicated override for categoryGridRetrieve via PSSTORE_SHA256
        if op == "categoryGridRetrieve" {
            if let Ok(hash) = std::env::var("PSSTORE_SHA256") {
                let trimmed = hash.trim();
                if !trimmed.is_empty() {
                    return trimmed.to_string();
                }
            }
        }

        // Priority 1: Environment variable override PS_HASH_<OPERATION>
        // e.g., PS_HASH_metGetProductById=<new-hash>
        if let Ok(hash) = std::env::var(&format!("PS_HASH_{}", op)) {
            let trimmed = hash.trim();
            if !trimmed.is_empty() {
                return trimmed.to_string();
            }
        }

        Self::ensure_hashes_loaded();

        // Priority 2: Dynamic hash map — prefer per-locale entry if present, else global op entry
        if let Some(map) = Self::dynamic_hash_map().get() {
            let loc_key = normalize_locale_key(locale);
            if let Some(h) = map.get(&format!("{}::{}", loc_key, op)) {
                let trimmed = h.trim();
                if !trimmed.is_empty() {
                    return trimmed.to_string();
                }
            }
            if let Some(h) = map.get(op) {
                let trimmed = h.trim();
                if !trimmed.is_empty() {
                    return trimmed.to_string();
                }
            }
        }

        // Legacy fallback: global PS_HASH override for categoryGridRetrieve only
        if op == "categoryGridRetrieve" {
            if let Ok(hash) = std::env::var("PS_HASH") {
                let trimmed = hash.trim();
                if !trimmed.is_empty() {
                    return trimmed.to_string();
                }
            }
        }

        // Priority 3: Static fallback map
        Self::static_hash_fallback(op).to_string()
    }

    /// Record a hash observation (missing or suspected mismatch) to a JSONL file for later curation.
    fn record_hash_observation(
        &self,
        op: &str,
        locale: &str,
        sha256: &str,
        source: &str,
        note: &str
    ) {
        use std::fs::{ OpenOptions, create_dir_all };
        use std::io::Write;
        let dir = std::path::Path::new("psstore_client");
        let _ = create_dir_all(dir);
        let path = dir.join("hashes.observed.json");
        #[derive(serde::Serialize)]
        struct Obs<'a> {
            op: &'a str,
            locale: &'a str,
            sha256: &'a str,
            source: &'a str,
            note: &'a str,
            ts: String,
        }
        let obs = Obs { op, locale, sha256, source, note, ts: chrono::Utc::now().to_rfc3339() };
        match OpenOptions::new().create(true).append(true).open(&path) {
            Ok(mut f) => {
                if let Ok(line) = serde_json::to_string(&obs) {
                    let _ = writeln!(f, "{}", line);
                }
            }
            Err(e) => {
                warn!(?e, path=%path.display(), "failed to append hash observation");
            }
        }
    }
    pub fn new(cfg: PsConfig) -> Self {
        // Build default headers to mirror the successful Postman capture as closely as possible
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        headers.insert(HeaderName::from_static("accept"), HeaderValue::from_static("*/*"));
        headers.insert(
            HeaderName::from_static("accept-encoding"),
            HeaderValue::from_static("gzip, deflate, br")
        );
        headers.insert(
            HeaderName::from_static("connection"),
            HeaderValue::from_static("keep-alive")
        );
        headers.insert(
            HeaderName::from_static("cache-control"),
            HeaderValue::from_static("no-cache")
        );

        // Prefer a realistic browser UA (configurable via PS_STORE_UA)
        let ua = std::env
            ::var("PS_STORE_UA")
            .unwrap_or_else(|_|
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0 Safari/537.36".to_string()
            );
        headers.insert(
            HeaderName::from_static("user-agent"),
            HeaderValue::from_str(&ua).unwrap_or_else(|_| HeaderValue::from_static("Mozilla/5.0"))
        );

        // IMPORTANT:
        // We intentionally set the "Cookie" header manually here to avoid using a persistent
        // cookie jar, which can cause issues with cookie reuse across requests.
        if let Some(c) = &cfg.cookie {
             headers.insert(
                HeaderName::from_static("cookie"),
                HeaderValue::from_str(c).expect("valid cookie value"),
            );
        }

        // Extra headers
        for (k, v) in &cfg.extra_headers {
            let name = HeaderName::from_bytes(k.as_bytes()).expect("valid header name");
            let val = HeaderValue::from_str(v).expect("valid header value");
            headers.insert(name, val);
        }

        let mut builder = Client::builder()
            .default_headers(headers)
            .gzip(true)
            .brotli(true)
            // .deflate(true) // not exposed; gzip/brotli are sufficient
            // Reduce connection churn; we do a lot of DB work between requests.
            .pool_idle_timeout(Duration::from_secs(300))
            .pool_max_idle_per_host(20)
            .tcp_keepalive(Duration::from_secs(60));

        if let Some(px) = &cfg.proxy {
            if !px.trim().is_empty() {
                let proxy = reqwest::Proxy::all(px).expect("invalid PS_PROXY");
                builder = builder.proxy(proxy);
            }
        }

        // If ipv6_only is requested and no proxy is set, override DNS resolution for the
        // base host to AAAA addresses only. This preserves the original hostname in the URL
        // (so TLS SNI remains correct) while removing any IPv4 fallback (no Happy Eyeballs).
        if
            cfg.ipv6_only &&
            cfg.proxy
                .as_deref()
                .map(|s| s.is_empty())
                .unwrap_or(true)
        {
            if let Ok(base) = reqwest::Url::parse(&cfg.base_url) {
                if let Some(host) = base.host_str() {
                    // Resolve host:443 and keep only IPv6 addresses
                    let v6_addrs: Vec<SocketAddr> = (host, 443)
                        .to_socket_addrs()
                        .ok()
                        .into_iter()
                        .flatten()
                        .filter(|sa| sa.is_ipv6())
                        .collect();
                    if !v6_addrs.is_empty() {
                        // reqwest::ClientBuilder::resolve requires a 'static domain string.
                        // Support the default PS host; otherwise skip the override.
                        const PS_HOST: &str = "web.np.playstation.com";
                        if host == PS_HOST {
                            let mut b2 = builder;
                            for addr in v6_addrs {
                                b2 = b2.resolve(PS_HOST, addr);
                            }
                            builder = b2;
                        } else {
                            warn!(host=%host, "ipv6_only host override skipped (non-default host requires static mapping)");
                        }
                    } else {
                        warn!(host=%host, "ipv6_only requested but no AAAA addresses resolved; proceeding without override");
                    }
                }
            }
        }

        let http = builder.build().expect("failed to build reqwest client");

        let limiter = RateLimiter::keyed(
            Quota::per_second(std::num::NonZeroU32::new(cfg.rps.max(3)).unwrap())
        );

        let resolver_v6 = if cfg.ipv6_only {
            let mut opts = ResolverOpts::default();
            opts.ip_strategy = LookupIpStrategy::Ipv6Only;
            Some(TokioAsyncResolver::tokio(ResolverConfig::default(), opts))
        } else {
            None
        };

        Self {
            http,
            cfg: Arc::new(cfg),
            limiter: Arc::new(limiter),
            etags: Arc::new(Mutex::new(HashMap::new())),
            resolver_v6,
        }
    }

    /// Normalize arbitrary locale strings to the ll-CC form used in headers and hash maps.
    /// Examples: "en-us" → "en-US", "en_US" → "en-US", "de-de" → "de-DE".
    fn normalize_locale_for_hash(locale: &str) -> String {
        normalize_locale_key(locale)
    }
    /// Fetch one product by product_id and normalize fields into PsProductDetail
    pub async fn get_product_by_id(
        &self,
        locale: &str,
        product_id: &str
    ) -> Result<PsProductDetail, PsError> {
        let v = self.product_detail_raw(locale, product_id).await?;
        // Adjust pointers to match current payload
        let prod = v
            .pointer("/data/metGetProductById")
            .and_then(|x| x.as_object())
            .ok_or_else(|| PsError::Other("metGetProductById: missing data".into()))?;

        let name = prod
            .get("name")
            .and_then(|x| x.as_str())
            .map(|s| s.to_string());
        // Prefer PS LONG description if present; fall back to common description fields
        let description = prod
            .get("longDesc")
            .or_else(|| prod.get("description"))
            .and_then(|x| x.as_str())
            .map(|s| s.to_string());
        let release_date = prod
            .get("releaseDate")
            .or_else(|| prod.get("release_date"))
            .and_then(|x| x.as_str())
            .map(|s| s.to_string());

        let mut genres: Vec<String> = Vec::new();
        if let Some(val) = prod.get("productGenres").or_else(|| prod.get("genres")) {
            collect_genre_strings(val, &mut genres);
        }
        genres.sort();
        genres.dedup();

        let mut images: Vec<String> = Vec::new();
        let mut videos: Vec<String> = Vec::new();
        if let Some(arr) = prod.get("media").and_then(|m| m.as_array()) {
            for m in arr {
                if let Some(u) = m.get("url").and_then(|x| x.as_str()) {
                    let is_img = m
                        .get("__typename")
                        .and_then(|t| t.as_str())
                        .map(|s| s.eq_ignore_ascii_case("IMAGE"))
                        .unwrap_or_else(|| {
                            m.get("type")
                                .and_then(|t| t.as_str())
                                .map(|s| s.eq_ignore_ascii_case("IMAGE"))
                                .unwrap_or(false)
                        });
                    if is_img {
                        images.push(u.to_string());
                    } else {
                        videos.push(u.to_string());
                    }
                }
            }
        }

        // Price fields vary; reuse module helper to parse minor units
        let (base_minor, disc_minor, _is_free) = parse_price_minor(prod.get("price"));
        let (avg_opt, cnt_opt) = match self.product_star_rating(locale, product_id).await? {
            Some((a, c)) => (Some(a), Some(c)),
            None => (None, None),
        };

        Ok(PsProductDetail {
            product_id: product_id.to_string(),
            name,
            description,
            release_date,
            genres,
            images,
            videos,
            price_minor: base_minor,
            discount_minor: disc_minor,
            average_rating: avg_opt,
            rating_count: cnt_opt,
        })
    }

    pub fn url(&self, path: &str) -> String {
        if path.starts_with("http") {
            path.to_string()
        } else {
            format!("{}/{}", self.cfg.base_url.trim_end_matches('/'), path.trim_start_matches('/'))
        }
    }

    /// Call a GraphQL persisted operation by name with variables and known sha256 hash.
    /// Locale header controls results; defaults to first configured locale if `locale` is None.
    pub async fn op_get(
        &self,
        operation_name: &str,
        variables: &Value,
        locale: Option<&str>
    ) -> Result<Value, PsError> {
        let url = self.url("op");
        let vars_string = variables.to_string();
        let vars_encoded = urlencoding::encode(&vars_string);
        // Resolve hash priority via persisted_hash_for, which accounts for per-operation overrides
        let expected_default = Self::static_hash_fallback(operation_name).to_string();
        // locale_use is defined below; compute effective_sha after that

        // Basic metrics counters
        use std::sync::atomic::{ AtomicU64, Ordering };
        static TOTAL_REQUESTS: AtomicU64 = AtomicU64::new(0);
        static HASH_MISMATCHES: AtomicU64 = AtomicU64::new(0);
        let total_after = TOTAL_REQUESTS.fetch_add(1, Ordering::Relaxed) + 1;
        let mut mismatch_total = HASH_MISMATCHES.load(Ordering::Relaxed);
        // We'll check for mismatch AFTER computing effective_sha

        let locale_use = locale
            .or_else(|| self.cfg.locales.get(0).map(|s| s.as_str()))
            .unwrap_or("en-us");

        // Compute effective sha now that locale_use is known
        let effective_sha = Self::persisted_hash_for(operation_name, locale_use);
        let effective_sha_trimmed = effective_sha.trim();
        if
            !expected_default.is_empty() &&
            !effective_sha_trimmed.is_empty() &&
            effective_sha_trimmed != expected_default
        {
            mismatch_total = HASH_MISMATCHES.fetch_add(1, Ordering::Relaxed) + 1;
            warn!(op=%operation_name, effective_hash=%effective_sha_trimmed, expected_hash=%expected_default, mismatch_total=%mismatch_total, "ps op_get hash override in effect");
            // Persist an observation to help capture and curate correct hashes per locale/op.
            self.record_hash_observation(
                operation_name,
                &locale_use,
                effective_sha_trimmed,
                "override",
                "effective hash differs from static default"
            );
        }
        info!(op=%operation_name, effective_hash=%effective_sha_trimmed, expected_hash=%expected_default, total_requests=%total_after, mismatch_total=%mismatch_total, "ps op_get hash metrics");

        let extensions_opt = if !effective_sha_trimmed.is_empty() {
            Some(
                format!("{{\"persistedQuery\":{{\"version\":1,\"sha256Hash\":\"{}\"}}}}", effective_sha_trimmed)
            )
        } else {
            None
        };

        let http = self.http.clone();
        let limiter = self.limiter.clone();
        let req_url = if let Some(ext) = &extensions_opt {
            format!(
                "{}?operationName={}&variables={}&extensions={}",
                url,
                operation_name,
                vars_encoded,
                urlencoding::encode(ext)
            )
        } else {
            format!("{}?operationName={}&variables={}", url, operation_name, vars_encoded)
        };

        // Simple on-disk cache for GraphQL GET responses (opt-in via PS_CACHE_DIR)
        // Keyed by (operation, locale, sha256, variables). TTL controlled by PS_CACHE_TTL_SECS (default 7200s).
        fn cache_key_to_path(
            base: &Path,
            op: &str,
            locale: &str,
            sha: &str,
            vars: &str
        ) -> PathBuf {
            // compute a stable u64 hash for compact filenames
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            op.hash(&mut hasher);
            locale.hash(&mut hasher);
            sha.hash(&mut hasher);
            vars.hash(&mut hasher);
            let h: u64 = hasher.finish();
            let mut p = base.to_path_buf();
            p.push(op);
            p.push(locale);
            fs::create_dir_all(&p).ok();
            p.push(format!("{:016x}.json", h));
            p
        }

        if let Ok(base_dir) = std::env::var("PS_CACHE_DIR") {
            let base = Path::new(&base_dir);
            let ttl_secs: u64 = std::env
                ::var("PS_CACHE_TTL_SECS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(7200);
            let cache_path = cache_key_to_path(
                base,
                operation_name,
                &locale_use,
                effective_sha_trimmed,
                &vars_string
            );
            if let Ok(meta) = fs::metadata(&cache_path) {
                if let Ok(modified) = meta.modified() {
                    if let Ok(elapsed) = modified.elapsed() {
                        if elapsed.as_secs() <= ttl_secs {
                            if let Ok(body) = fs::read_to_string(&cache_path) {
                                if let Ok(v) = serde_json::from_str::<Value>(&body) {
                                    // Do NOT serve cached error payloads; force network retry on GraphQL errors
                                    if v.get("errors").is_some() {
                                        warn!(op=%operation_name, cache="bypass", path=%cache_path.display(), "ps op_get cached payload contains GraphQL errors; ignoring cache");
                                    } else {
                                        info!(op=%operation_name, cache="hit", path=%cache_path.display(), "ps op_get served from cache");
                                        if let Ok(pretty) = serde_json::to_string_pretty(&v) {
                                            println!(
                                                "[psstore op={} cache-hit] {}",
                                                operation_name,
                                                pretty
                                            );
                                        } else {
                                            println!("[psstore op={} cache-hit] <non-json>", operation_name);
                                        }
                                        return Ok(v);
                                    }
                                }
                            }
                        }
                    }
                }
            }

            // Perform request with retries; on success, write cache
            let mut attempt = 0u32;
            let max_attempts = self.cfg.retry_attempts.max(1);
            let mut delay = Duration::from_millis(self.cfg.retry_base_delay_ms.max(1));
            // Canonicalize locale header to ll-CC
            let key = {
                let s = locale_use.replace('_', "-");
                if let Some((ll, cc)) = s.split_once('-') {
                    format!("{}-{}", ll.to_ascii_lowercase(), cc.to_ascii_uppercase())
                } else {
                    s.to_ascii_lowercase()
                }
            };

            loop {
                attempt += 1;

                let _ = limiter.until_key_ready(&key).await;
                let t0 = Instant::now();
                let req_id = format!(
                    "{:x}-{}",
                    SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .map(|d| d.as_millis())
                        .unwrap_or(0),
                    attempt
                );
                info!(op=%operation_name, locale=%key, req_id=%req_id, url=%req_url, sha256=%effective_sha_trimmed, "ps op_get request");

                // Identification headers similar to the web app
                let accept_lang = format!("{};q=0.9", key);
                let origin = "https://store.playstation.com";
                let referer = format!(
                    "https://store.playstation.com/{}/pages/latest",
                    key.to_lowercase()
                );
                let ua = std::env
                    ::var("PS_STORE_UA")
                    .unwrap_or_else(|_|
                        "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0 Safari/537.36".to_string()
                    );

                let resp = match
                    http
                        .get(&req_url)
                        .header("x-psn-store-locale-override", &key)
                        .header("x-apollo-operation-name", operation_name)
                        .header("apollo-require-preflight", "true")
                        .header(CONTENT_TYPE, HeaderValue::from_static("application/json"))
                        .header(reqwest::header::ACCEPT, HeaderValue::from_static("application/json"))
                        .header(reqwest::header::ACCEPT_LANGUAGE, accept_lang.clone())
                        .header(reqwest::header::ORIGIN, origin)
                        .header(reqwest::header::REFERER, referer.clone())
                        .header(reqwest::header::USER_AGENT, ua.clone())
                        .header("X-PSN-Store-Front", key.to_lowercase())
                        .send().await
                {
                    Ok(r) => r,
                    Err(e) => {
                        warn!(attempt, error=?e, "ps op_get network error");
                        if attempt >= max_attempts {
                            return Err(PsError::Net(e));
                        }
                        tokio::time::sleep(delay).await;
                        delay = delay.saturating_mul(2);
                        continue;
                    }
                };

                let status = resp.status();
                let body = match resp.text().await {
                    Ok(b) => b,
                    Err(e) => {
                        warn!(attempt, error=?e, "ps op_get body read error");
                        if attempt >= max_attempts {
                            return Err(PsError::Net(e));
                        }
                        tokio::time::sleep(delay).await;
                        delay = delay.saturating_mul(2);
                        continue;
                    }
                };

                let elapsed = t0.elapsed().as_millis();
                info!(op=%operation_name, locale=%key, req_id=%req_id, status=%status.as_u16(), body_len=body.len(), elapsed_ms=%elapsed, "ps op_get response");
                if std::env::var("PS_TRACE_BODIES").ok().as_deref() == Some("1") {
                    let max = std::env
                        ::var("PS_TRACE_BODY_LEN")
                        .ok()
                        .and_then(|s| s.parse().ok())
                        .unwrap_or(512usize);
                    let sample = &body[..body.len().min(max)];
                    debug!(op=%operation_name, req_id=%req_id, sample=%sample, "ps op_get body sample");
                }

                if !status.is_success() {
                    if status.as_u16() >= 500 {
                        warn!(status=%status.as_u16(), "ps op_get server error, will retry if attempts remain");
                        if attempt >= max_attempts {
                            // Record observation for non-success
                            self.record_hash_observation(
                                operation_name,
                                &key,
                                effective_sha_trimmed,
                                if effective_sha_trimmed.is_empty() {
                                    "none"
                                } else {
                                    "override"
                                },
                                "server error at cache path"
                            );
                            return Err(PsError::Http { status: status.as_u16(), body });
                        }
                        tokio::time::sleep(delay).await;
                        delay = delay.saturating_mul(2);
                        continue;
                    } else {
                        let status_u16 = status.as_u16();
                        let sample_body = body.get(..200).unwrap_or(&body);
                        // NOTE: Many GraphQL client-side errors are expected/handled by callers (e.g. missing ratings).
                        // Keep auth-related issues loud, but avoid polluting logs with ERROR for non-fatal 4xx.
                        if status_u16 == 401 || status_u16 == 403 {
                            error!(status=%status_u16, op=%operation_name, sample_body=%sample_body, "ps op_get auth/forbidden");
                        } else if status_u16 == 400 && sample_body.contains("Unknown operation named") {
                            warn!(status=%status_u16, op=%operation_name, sample_body=%sample_body, "ps op_get client error (expected/handled)");
                        } else {
                            warn!(status=%status_u16, op=%operation_name, sample_body=%sample_body, "ps op_get client error");
                        }
                        self.record_hash_observation(
                            operation_name,
                            &key,
                            effective_sha_trimmed,
                            if effective_sha_trimmed.is_empty() {
                                "none"
                            } else {
                                "override"
                            },
                            "client error at cache path"
                        );
                        return Err(PsError::Http { status: status.as_u16(), body });
                    }
                }

                let v: Value = match serde_json::from_str(&body) {
                    Ok(v) => v,
                    Err(e) => {
                        return Err(PsError::Json(e));
                    }
                };

                // If GraphQL errors indicate persisted query mismatch, don't cache and bubble up immediately
                if let Some(errs) = v.get("errors").and_then(|e| e.as_array()) {
                    let mut persisted_query_issue = false;
                    let mut retryable = false;
                    for err in errs {
                        let msg = err
                            .get("message")
                            .and_then(|m| m.as_str())
                            .unwrap_or("");
                        let code_i64 = err
                            .get("extensions")
                            .and_then(|ex| ex.get("code"))
                            .and_then(|c| c.as_i64());
                        warn!(op=%operation_name, code_i64=?code_i64, msg=%msg, sample_body=%body.get(..200).unwrap_or(&body), "ps op_get graphql error");
                        if
                            msg.to_lowercase().contains("persisted query not found") ||
                            msg.to_lowercase().contains("persistedquerynotfound") ||
                            msg.to_lowercase().contains("unsupported persisted query")
                        {
                            persisted_query_issue = true;
                        }
                        if
                            msg.to_lowercase().contains("all shards failed") ||
                            code_i64 == Some(3165954)
                        {
                            retryable = true;
                        }
                    }
                    if persisted_query_issue {
                        return Err(
                            PsError::Other(
                                "psstore persisted query not found or unsupported; refresh sha256Hash".to_string()
                            )
                        );
                    }
                    if retryable {
                        warn!(attempt, op=%operation_name, "ps op_get graphql transient error; retrying");
                        if attempt >= max_attempts {
                            return Err(PsError::Other(format!("graphql errors: {:?}", errs)));
                        }
                        tokio::time::sleep(delay).await;
                        delay = delay.saturating_mul(2);
                        continue;
                    }
                }

                // Successful response without retryable errors — write cache best-effort
                if let Some(parent) = cache_path.parent() {
                    let _ = fs::create_dir_all(parent);
                }
                if let Ok(mut f) = fs::File::create(&cache_path) {
                    let _ = f.write_all(body.as_bytes());
                }
                info!(op=%operation_name, cache="miss-write", path=%cache_path.display(), "ps op_get cached response");
                // Avoid dumping full responses by default; they are large and will drown CLI output.
                if std::env::var("PS_PRINT_FULL_JSON").ok().as_deref() == Some("1") {
                    if let Ok(pretty) = serde_json::to_string_pretty(&v) {
                        println!("[psstore op={} cache-write] {}", operation_name, pretty);
                    } else {
                        println!("[psstore op={} cache-write] <non-json>", operation_name);
                    }
                }
                return Ok(v);
            }
        }

        // Default path: no cache directory configured
        let mut attempt = 0u32;
        let max_attempts = self.cfg.retry_attempts.max(1);
        let mut delay = Duration::from_millis(self.cfg.retry_base_delay_ms.max(1));
        let key = {
            let s = locale_use.replace('_', "-");
            if let Some((ll, cc)) = s.split_once('-') {
                format!("{}-{}", ll.to_ascii_lowercase(), cc.to_ascii_uppercase())
            } else {
                s.to_ascii_lowercase()
            }
        };

        loop {
            attempt += 1;

            // rate-limit per locale
            let _ = limiter.until_key_ready(&key).await;
            let t0 = Instant::now();
            let req_id = format!(
                "{:x}-{}",
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map(|d| d.as_millis())
                    .unwrap_or(0),
                attempt
            );
            info!(op=%operation_name, locale=%key, req_id=%req_id, url=%req_url, sha256=%effective_sha_trimmed, "ps op_get request");

            // 🔴 This is the `Err(e) => { ... }` branch you asked for
            // Identification headers similar to the web app
            let accept_lang = format!("{};q=0.9", key);
            let origin = "https://store.playstation.com";
            let referer = format!(
                "https://store.playstation.com/{}/pages/latest",
                key.to_lowercase()
            );
            let ua = std::env
                ::var("PS_STORE_UA")
                .unwrap_or_else(|_|
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0 Safari/537.36".to_string()
                );

            let resp = match
                http
                    .get(&req_url)
                    .header("x-psn-store-locale-override", &key)
                    .header("x-apollo-operation-name", operation_name)
                    .header("apollo-require-preflight", "true")
                    .header(CONTENT_TYPE, HeaderValue::from_static("application/json"))
                    .header(reqwest::header::ACCEPT, HeaderValue::from_static("application/json"))
                    .header(reqwest::header::ACCEPT_LANGUAGE, accept_lang.clone())
                    .header(reqwest::header::ORIGIN, origin)
                    .header(reqwest::header::REFERER, referer.clone())
                    .header(reqwest::header::USER_AGENT, ua.clone())
                    .header("X-PSN-Store-Front", key.to_lowercase())
                    .send().await
            {
                Ok(r) => r,
                Err(e) => {
                    // Network-level error (DNS, timeout, connection reset, etc.)
                    warn!(attempt, error=?e, "ps op_get network error");

                    // If we've exhausted retries, bubble it up as PsError::Net
                    if attempt >= max_attempts {
                        return Err(PsError::Net(e));
                    }

                    // Otherwise back off and retry
                    tokio::time::sleep(delay).await;
                    delay = delay.saturating_mul(2);
                    continue;
                }
            };

            let status = resp.status();
            let body = match resp.text().await {
                Ok(b) => b,
                Err(e) => {
                    warn!(attempt, error=?e, "ps op_get body read error");
                    if attempt >= max_attempts {
                        return Err(PsError::Net(e));
                    }
                    tokio::time::sleep(delay).await;
                    delay = delay.saturating_mul(2);
                    continue;
                }
            };

            let elapsed = t0.elapsed().as_millis();
            info!(op=%operation_name, locale=%key, req_id=%req_id, status=%status.as_u16(), body_len=body.len(), elapsed_ms=%elapsed, "ps op_get response");
            if std::env::var("PS_TRACE_BODIES").ok().as_deref() == Some("1") {
                let max = std::env
                    ::var("PS_TRACE_BODY_LEN")
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(512usize);
                let sample = &body[..body.len().min(max)];
                debug!(op=%operation_name, req_id=%req_id, sample=%sample, "ps op_get body sample");
            }

            if !status.is_success() {
                // Retry 5xx as transient, fail fast on 4xx
                if status.as_u16() >= 500 {
                    warn!(
                            status=%status.as_u16(),
                            "ps op_get server error, will retry if attempts remain"
                        );
                    if attempt >= max_attempts {
                        self.record_hash_observation(
                            operation_name,
                            &key,
                            effective_sha_trimmed,
                            if effective_sha_trimmed.is_empty() {
                                "none"
                            } else {
                                "override"
                            },
                            "server error"
                        );
                        return Err(PsError::Http {
                            status: status.as_u16(),
                            body,
                        });
                    }
                    tokio::time::sleep(delay).await;
                    delay = delay.saturating_mul(2);
                    continue;
                } else {
                    let status_u16 = status.as_u16();
                    let sample_body = body.get(..200).unwrap_or(&body);
                    // Mirror the cache-path behavior: many 4xx are expected/handled by the caller.
                    if status_u16 == 401 || status_u16 == 403 {
                        error!(status=%status_u16, op=%operation_name, sample_body=%sample_body, "ps op_get auth/forbidden");
                    } else if status_u16 == 400 && sample_body.contains("Unknown operation named") {
                        warn!(status=%status_u16, op=%operation_name, sample_body=%sample_body, "ps op_get client error (expected/handled)");
                    } else {
                        warn!(status=%status_u16, op=%operation_name, sample_body=%sample_body, "ps op_get client error");
                    }
                    self.record_hash_observation(
                        operation_name,
                        &key,
                        effective_sha_trimmed,
                        if effective_sha_trimmed.is_empty() {
                            "none"
                        } else {
                            "override"
                        },
                        "client error"
                    );
                    return Err(PsError::Http {
                        status: status.as_u16(),
                        body,
                    });
                }
            }

            let v: Value = serde_json::from_str(&body).map_err(PsError::Json)?;

            // Full body dumps are opt-in (they are huge and make CLI runs unusable).
            if std::env::var("PS_PRINT_FULL_JSON").ok().as_deref() == Some("1") {
                if let Ok(pretty) = serde_json::to_string_pretty(&v) {
                    println!("[psstore op={}] {}", operation_name, pretty);
                } else {
                    println!("[psstore op={}] <non-json>", operation_name);
                }
            }

            // GraphQL-level transient errors (e.g. ES "all shards failed")
            if let Some(errs) = v.get("errors").and_then(|e| e.as_array()) {

                // Default: not retryable unless matched below
                let mut retryable = false;
                let mut persisted_query_issue = false;

                for err in errs {
                    let msg = err
                        .get("message")
                        .and_then(|m| m.as_str())
                        .unwrap_or("");
                    let code_i64 = err
                        .get("extensions")
                        .and_then(|ex| ex.get("code"))
                        .and_then(|c| c.as_i64());
                    let code_str = err
                        .get("extensions")
                        .and_then(|ex| ex.get("code"))
                        .and_then(|c| c.as_str());

                    // Log details for easier diagnosis
                    warn!(op=%operation_name, req_id=%req_id, code_i64=?code_i64, code_str=?code_str, msg=%msg, sample_body=%body.get(..200).unwrap_or(&body), "ps op_get graphql error");

                    // Known transient: backing search failure
                    if msg.to_lowercase().contains("all shards failed") || code_i64 == Some(3165954) {
                        retryable = true;
                    }

                    // Non-retryable: persisted query cache miss or version mismatch
                    if
                        msg.to_lowercase().contains("persistedquerynotfound") ||
                        msg.to_lowercase().contains("persisted query not found") ||
                        msg.to_lowercase().contains("unsupported persisted query")
                    {
                        persisted_query_issue = true;
                    }
                }

                if persisted_query_issue {
                    // Persist explicit observation for persisted query errors
                    self.record_hash_observation(
                        operation_name,
                        &key,
                        effective_sha_trimmed,
                        if effective_sha_trimmed.is_empty() {
                            "none"
                        } else {
                            "override"
                        },
                        "persisted query issue"
                    );
                    // Bubble up immediately; caller should refresh sha256 hash or switch to POST with full query text.
                    return Err(
                        PsError::Other(
                            "psstore persisted query not found or unsupported; refresh sha256Hash".to_string()
                        )
                    );
                }

                if retryable {
                    warn!(attempt, op=%operation_name, "ps op_get graphql transient error; retrying");
                    if attempt >= max_attempts {
                        return Err(PsError::Other(format!("graphql errors: {:?}", errs)));
                    }
                    tokio::time::sleep(delay).await;
                    delay = delay.saturating_mul(2);
                    continue;
                }
            }

            return Ok(v);
        }
    }
    /// Convenience: categoryGridRetrieve → returns best-effort parsed product summaries
    pub async fn category_grid_retrieve(
        &self,
        locale: &str,
        category_id: &str,
        size: u32,
        offset: u32
    ) -> Result<Vec<PsProductSummary>, PsError> {
        let vars =
            serde_json::json!({
                "id": category_id,
                "pageArgs": { "size": size, "offset": offset },
                "sortBy": Value::Null,
                "filterBy": [],
                "facetOptions": []
            });
        let v = self.op_get("categoryGridRetrieve", &vars, Some(locale)).await?;
        let items = extract_product_summaries(&v);
        if v.get("errors").is_some() && items.is_empty() {
            // Bubble up so callers can apply fallback strategies (size/sort/no-sort) instead of
            // silently interpreting upstream failures as a real empty page.
            if Self::is_es_shard_failure(&v) {
                return Err(PsError::Other("psstore elasticsearch shard failure".into()));
            }
            return Err(PsError::Other("graphql errors with empty results".into()));
        }
        Ok(items)
    }

    /// categoryGridRetrieve with explicit sortBy
    /// sort_name examples observed: "releaseDate", "name", etc.
    pub async fn category_grid_retrieve_sorted(
        &self,
        locale: &str,
        category_id: &str,
        size: u32,
        offset: u32,
        sort_name: &str,
        is_ascending: bool
    ) -> Result<Vec<PsProductSummary>, PsError> {
        let vars =
            serde_json::json!({
                "id": category_id,
                "pageArgs": { "size": size, "offset": offset },
                "sortBy": { "name": sort_name, "isAscending": is_ascending },
                "filterBy": [],
                "facetOptions": []
            });
        let v = self.op_get("categoryGridRetrieve", &vars, Some(locale)).await?;
        let items = extract_product_summaries(&v);
        if v.get("errors").is_some() && items.is_empty() {
            if Self::is_es_shard_failure(&v) {
                return Err(PsError::Other("psstore elasticsearch shard failure".into()));
            }
            return Err(PsError::Other("graphql errors with empty results".into()));
        }
        Ok(items)
    }

    /// Paginate categoryGridRetrieve sorted descending by release date until reaching cutoff year.
    /// Stops when it encounters an item with a release date year < cutoff_year or max_pages exhausted.
    /// Returns aggregated product summaries (without guaranteeing uniqueness).
    pub async fn category_grid_retrieve_desc_until_year(
        &self,
        locale: &str,
        category_id: &str,
        cutoff_year: i32,
        page_size: u32,
        max_pages: u32
    ) -> Result<Vec<PsProductSummary>, PsError> {
        let mut out: Vec<PsProductSummary> = Vec::new();
        let mut page: u32 = 0;
        'pages: while page < max_pages {
            let offset = page * page_size;
            // Try object form of sortBy (name + isAscending expected by API); fall back to a string form if needed by retry.
            let vars_obj =
                serde_json::json!({
                    "id": category_id,
                    "pageArgs": { "size": page_size, "offset": offset },
                    "sortBy": {"name":"releaseDate","isAscending": false},
                    "filterBy": [],
                    "facetOptions": []
                });
            let mut v = match self.op_get("categoryGridRetrieve", &vars_obj, Some(locale)).await {
                Ok(val) => val,
                Err(_) => {
                    // Fallback: simple string sortBy (implementation guess) — continue even if error again.
                    let vars_simple =
                        serde_json::json!({
                            "id": category_id,
                            "pageArgs": { "size": page_size, "offset": offset },
                            "sortBy": "releaseDate:desc",
                            "filterBy": [],
                            "facetOptions": []
                        });
                    self.op_get("categoryGridRetrieve", &vars_simple, Some(locale)).await?
                }
            };

            // If Sony's backend is returning shard failures for releaseDate sorting, try once with no sort.
            // This is a best-effort resiliency hack: if the retry returns real items, keep going.
            if Self::is_es_shard_failure(&v) {
                warn!(page=%page, offset=%offset, "ps categoryGridRetrieve shard failure on releaseDate sort; retrying without sortBy");
                let vars_nosort =
                    serde_json::json!({
                        "id": category_id,
                        "pageArgs": { "size": page_size, "offset": offset },
                        "sortBy": Value::Null,
                        "filterBy": [],
                        "facetOptions": []
                    });
                match self.op_get("categoryGridRetrieve", &vars_nosort, Some(locale)).await {
                    Ok(v2) => {
                        let retry_items = extract_product_summaries_with_date(&v2);
                        if !retry_items.is_empty() {
                            v = v2;
                        }
                    }
                    Err(err) => {
                        debug!(page=%page, offset=%offset, error=?err, "ps categoryGridRetrieve retry without sortBy failed");
                    }
                }
            }
            if let Some(errs) = v.get("errors").and_then(|e| e.as_array()) {
                if let Some(first) = errs.get(0) {
                    error!(page=%page, error=?first, "ps category_grid_retrieve_desc_until_year returned errors");
                }
            }
            let page_items = extract_product_summaries_with_date(&v);
            if page_items.is_empty() {
                break;
            }
            for item in &page_items {
                if let Some(year) = item.release_year {
                    if year < cutoff_year {
                        break 'pages;
                    }
                }
                out.push(PsProductSummary {
                    product_id: item.product_id.clone(),
                    concept_id: item.concept_id.clone(),
                    name: item.name.clone(),
                    release_date: None,
                    base_price_minor: None,
                    discounted_price_minor: None,
                    is_free: None,
                    media_urls: Vec::new(),
                    media_image_urls: Vec::new(),
                    media_video_urls: Vec::new(),
                    media_images: Vec::new(),
                    media_videos: Vec::new(),
                    genres: Vec::new(),
                    average_rating: None,
                    rating_count: None,
                });
            }
            if (page_items.len() as u32) < page_size {
                break;
            } // no more pages
            page += 1;
        }
        Ok(out)
    }

    /// Build variables for categoryGridRetrieve requests in a typed way and handle pagination.
    pub fn category_request(
        &self,
        category_id: impl Into<String>,
        size: u32,
        offset: u32,
        sort_name: Option<&str>,
        is_ascending: Option<bool>,
        filter_by: Option<Vec<String>>,
        facet_options: Option<Vec<String>>
    ) -> CategoryRequest {
        CategoryRequest {
            category_id: category_id.into(),
            size,
            offset,
            sort_name: sort_name.map(|s| s.to_string()),
            is_ascending,
            filter_by: filter_by.unwrap_or_default(),
            facet_options: facet_options.unwrap_or_default(),
        }
    }

    fn vars_for_category_request(req: &CategoryRequest) -> Value {
        let mut vars =
            serde_json::json!({
                "id": req.category_id.clone(),
                "pageArgs": { "size": req.size, "offset": req.offset },
                "filterBy": req.filter_by.clone(),
                "facetOptions": req.facet_options.clone(),
            });
        if let Some(name) = &req.sort_name {
            let asc = req.is_ascending.unwrap_or(false);
            vars["sortBy"] = serde_json::json!({"name": name, "isAscending": asc});
        } else {
            vars["sortBy"] = Value::Null;
        }
        vars
    }

    pub async fn category_grid_raw(
        &self,
        locale: &str,
        req: &CategoryRequest
    ) -> Result<Value, PsError> {
        let vars = Self::vars_for_category_request(req);
        self.op_get("categoryGridRetrieve", &vars, Some(locale)).await
    }

    pub async fn get_category_page(
        &self,
        locale: &str,
        req: &CategoryRequest
    ) -> Result<Vec<PsProductSummary>, PsError> {
        let v = self.category_grid_raw(locale, req).await?;
        Ok(extract_product_summaries(&v))
    }

    /// Fetch product star rating (average + count) via wcaProductStarRatingRetrieve.
    pub async fn product_star_rating(
        &self,
        locale: &str,
        product_id: &str
    ) -> Result<Option<(f32, i64)>, PsError> {
        // Preferred path: derive concept id and call wcaConceptStarRatingRetrive to avoid
        // deprecated product-level star rating queries. Fall back to the legacy product op
        // only if the concept flow fails so ingestion remains resilient if Sony reshuffles
        // response shapes again.
        match self.concept_id_for_product(locale, product_id).await {
            Ok(Some(concept_id)) =>
                match self.concept_star_rating(locale, &concept_id).await {
                    Ok(Some(rating)) => {
                        return Ok(Some(rating));
                    }
                    Ok(None) => {
                        debug!(
                        %locale,
                        %product_id,
                        %concept_id,
                        "concept star rating missing; falling back to product op"
                    );
                    }
                    Err(err) => {
                        warn!(
                        %locale,
                        %product_id,
                        %concept_id,
                        error=?err,
                        "concept star rating failed; falling back to product op"
                    );
                    }
                }
            Ok(None) => {
                debug!(%locale, %product_id, "concept id not found; falling back to product op");
            }
            Err(err) => {
                warn!(%locale, %product_id, error=?err, "concept lookup failed; falling back");
            }
        }

        let vars = serde_json::json!({ "productId": product_id });
        let v = match self.op_get("wcaProductStarRatingRetrieve", &vars, Some(locale)).await {
            Ok(val) => val,
            Err(PsError::Http { status, body }) if (400..500).contains(&status) => {
                let sample: String = body.chars().take(200).collect();
                warn!(
                    %locale,
                    %product_id,
                    status,
                    sample_body=%sample,
                    "ps product star rating request returned client error; treating as missing"
                );
                return Ok(None);
            }
            Err(err) => {
                return Err(err);
            }
        };
        if let Some(obj) = v.get("data").and_then(|d| d.get("wcaProductStarRatingRetrieve")) {
            if let Some(metrics) = rating_metrics_from_star_obj(obj) {
                return Ok(Some(metrics));
            }
        }
        Ok(None)
    }

    /// Fetch concept star rating (preferred) via wcaConceptStarRatingRetrive.
    pub async fn concept_star_rating(
        &self,
        locale: &str,
        concept_id: &str
    ) -> Result<Option<(f32, i64)>, PsError> {
        let vars = serde_json::json!({ "conceptId": concept_id });
        let v = self.op_get("wcaConceptStarRatingRetrieve", &vars, Some(locale)).await?;
        let candidates = [
            v
                .get("data")
                .and_then(|d| d.get("conceptRetrieve"))
                .and_then(|c| c.get("defaultProduct"))
                .and_then(|p| p.get("starRating")),
            v
                .get("data")
                .and_then(|d| d.get("conceptRetrieve"))
                .and_then(|c| c.get("starRating")),
            v
                .get("data")
                .and_then(|d| d.get("concept"))
                .and_then(|c| c.get("defaultProduct"))
                .and_then(|p| p.get("starRating")),
            v
                .get("data")
                .and_then(|d| d.get("concept"))
                .and_then(|c| c.get("starRating")),
        ];
        for obj in candidates.into_iter().flatten() {
            if let Some(metrics) = rating_metrics_from_star_obj(obj) {
                return Ok(Some(metrics));
            }
        }
        Ok(None)
    }

    /// Resolve the first concept id backing a product id via metGetConceptByProductIdQuery.
    pub async fn concept_id_for_product(
        &self,
        locale: &str,
        product_id: &str
    ) -> Result<Option<String>, PsError> {
        let v = self.concept_by_product_id_raw(locale, product_id).await?;
        Ok(extract_first_concept_id(&v))
    }

    /// Fetch raw product metadata via metGetProductById. Returns the GraphQL payload without
    /// additional parsing so callers can decide which fields to hydrate.
    pub async fn product_detail_raw(
        &self,
        locale: &str,
        product_id: &str
    ) -> Result<Value, PsError> {
        // Unified hash handling: rely on global PS_HASH if set; otherwise omit persistedQuery
        let _hash = "";
        let vars = serde_json::json!({ "productId": product_id });
        self.op_get("metGetProductById", &vars, Some(locale)).await
    }

    /// Fetch concept info by product id via metGetConceptByProductIdQuery. Returns raw payload.
    pub async fn concept_by_product_id_raw(
        &self,
        locale: &str,
        product_id: &str
    ) -> Result<Value, PsError> {
        // Unified hash handling: rely on global PS_HASH if set; otherwise omit persistedQuery
        let _hash = "";
        let vars = serde_json::json!({ "productId": product_id });
        self.op_get("metGetConceptByProductIdQuery", &vars, Some(locale)).await
    }

    /// Fetch pricing by concept id via metGetPricingDataByConceptId. Returns raw payload.
    pub async fn concept_pricing_raw(
        &self,
        locale: &str,
        concept_id: &str
    ) -> Result<Value, PsError> {
        // Unified hash handling: rely on global PS_HASH if set; otherwise omit persistedQuery
        let _hash = "";
        let vars = serde_json::json!({ "conceptId": concept_id });
        self.op_get("metGetPricingDataByConceptId", &vars, Some(locale)).await
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PsProductSummary {
    pub product_id: Option<String>,
    pub concept_id: Option<String>,
    pub name: Option<String>,
    pub release_date: Option<String>,
    pub base_price_minor: Option<i64>,
    pub discounted_price_minor: Option<i64>,
    pub is_free: Option<bool>,
    pub media_urls: Vec<String>,
    pub media_image_urls: Vec<String>,
    pub media_video_urls: Vec<String>,
    pub media_images: Vec<PsMedia>,
    pub media_videos: Vec<PsMedia>,
    pub genres: Vec<String>, // genre keys or display names
    pub average_rating: Option<f32>, // enriched later
    pub rating_count: Option<i64>, // enriched later
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PsMedia {
    pub typename: Option<String>,
    pub media_type: Option<String>,
    pub role: Option<String>,
    pub url: Option<String>,
}

/// Normalized product detail suitable for ingestion and UI
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PsProductDetail {
    pub product_id: String,
    pub name: Option<String>,
    pub description: Option<String>,
    pub release_date: Option<String>,
    pub genres: Vec<String>,
    pub images: Vec<String>,
    pub videos: Vec<String>,
    pub price_minor: Option<i64>,
    pub discount_minor: Option<i64>,
    pub average_rating: Option<f32>,
    pub rating_count: Option<i64>,
}

#[derive(Debug, Clone)]
pub struct GenreFacetValue {
    pub key: String,
    pub display_name: String,
    pub count: i64,
}

fn extract_product_summaries(v: &Value) -> Vec<PsProductSummary> {
    // Prefer explicit categoryGridRetrieve paths, supporting `products`, `results`, `grid.results`, and `concepts` shapes.
    let mut arr_ref: Option<&Vec<Value>> = None;
    if let Some(data) = v.get("data") {
        if let Some(cat) = data.get("categoryGridRetrieve") {
            // Newer shape: direct products array
            if let Some(arr) = cat.get("products").and_then(|r| r.as_array()) {
                arr_ref = Some(arr);
            }
            // Some locales/schemas expose `concepts`
            if arr_ref.is_none() {
                if let Some(arr) = cat.get("concepts").and_then(|r| r.as_array()) {
                    arr_ref = Some(arr);
                }
            }
            // Legacy/alt shapes: results or grid.results
            if arr_ref.is_none() {
                if let Some(arr) = cat.get("results").and_then(|r| r.as_array()) {
                    arr_ref = Some(arr);
                }
            }
            if arr_ref.is_none() {
                if let Some(grid) = cat.get("grid") {
                    if let Some(arr) = grid.get("results").and_then(|r| r.as_array()) {
                        arr_ref = Some(arr);
                    }
                    if arr_ref.is_none() {
                        if let Some(arr) = grid.get("concepts").and_then(|r| r.as_array()) {
                            arr_ref = Some(arr);
                        }
                    }
                }
            }
        }
        // Fallback: scan any child object for products/results/concepts arrays
        if arr_ref.is_none() {
            if let Some(obj) = data.as_object() {
                for (_k, val) in obj {
                    if let Some(arr) = val.get("products").and_then(|r| r.as_array()) {
                        arr_ref = Some(arr);
                        break;
                    }
                    if let Some(arr) = val.get("concepts").and_then(|r| r.as_array()) {
                        arr_ref = Some(arr);
                        break;
                    }
                    if let Some(arr) = val.get("results").and_then(|r| r.as_array()) {
                        arr_ref = Some(arr);
                        break;
                    }
                    if let Some(grid) = val.get("grid") {
                        if let Some(arr) = grid.get("results").and_then(|r| r.as_array()) {
                            arr_ref = Some(arr);
                            break;
                        }
                        if let Some(arr) = grid.get("concepts").and_then(|r| r.as_array()) {
                            arr_ref = Some(arr);
                            break;
                        }
                    }
                }
            }
        }
    }
    let mut out = Vec::new();
    if let Some(items) = arr_ref {
        for it in items {
            let product_id = it
                .get("id")
                .or_else(|| it.get("productId"))
                .and_then(|x| x.as_str())
                .map(|s| s.to_string());
            let concept_id = it
                .get("conceptId")
                .or_else(|| it.get("conceptID"))
                .and_then(|x| x.as_str())
                .map(|s| s.to_string());
            let name = it
                .get("name")
                .and_then(|x| x.as_str())
                .map(|s| s.to_string())
                .or_else(||
                    it
                        .get("displayName")
                        .and_then(|x| x.as_str())
                        .map(|s| s.to_string())
                );
            let release_date = it
                .get("releaseDate")
                .and_then(|d| d.as_str())
                .map(|s| s.to_string())
                .or_else(||
                    it
                        .get("release_date")
                        .and_then(|d| d.as_str())
                        .map(|s| s.to_string())
                )
                .or_else(||
                    it
                        .get("firstReleaseDate")
                        .and_then(|d| d.as_str())
                        .map(|s| s.to_string())
                )
                .or_else(||
                    it
                        .get("releaseDateStr")
                        .and_then(|d| d.as_str())
                        .map(|s| s.to_string())
                );
            let (base_price_minor, discounted_price_minor, is_free) = parse_price_minor(
                it.get("price")
            );
            if std::env::var("PS_LOG_ZERO_PRICES").ok().as_deref() == Some("1") {
                if let Some(0) = base_price_minor {
                    warn!(?it, "ps price parsed zero base");
                }
                if let Some(0) = discounted_price_minor {
                    warn!(?it, "ps price parsed zero discount");
                }
            }
            let mut media_urls: Vec<String> = Vec::new();
            let mut media_image_urls: Vec<String> = Vec::new();
            let mut media_video_urls: Vec<String> = Vec::new();
            let mut media_images: Vec<PsMedia> = Vec::new();
            let mut media_videos: Vec<PsMedia> = Vec::new();
            let mut seen_urls: std::collections::HashSet<String> = std::collections::HashSet::new();
            if let Some(arr) = it.get("media").and_then(|m| m.as_array()) {
                for x in arr {
                    let url_opt = x
                        .get("url")
                        .and_then(|u| u.as_str())
                        .map(|s| s.to_string());
                    let mtype_opt = x
                        .get("type")
                        .and_then(|t| t.as_str())
                        .map(|s| s.to_string());
                    let role_opt = x
                        .get("role")
                        .and_then(|r| r.as_str())
                        .map(|s| s.to_string());
                    // Some feeds expose "__typename", others might expose "__typeof"; prefer either.
                    let typename_opt = x
                        .get("__typename")
                        .or_else(|| x.get("__typeof"))
                        .and_then(|r| r.as_str())
                        .map(|s| s.to_string());
                    let media_obj = PsMedia {
                        typename: typename_opt.clone(),
                        media_type: mtype_opt.clone(),
                        role: role_opt.clone(),
                        url: url_opt.clone(),
                    };
                    if let Some(url) = &url_opt {
                        // Deduplicate by URL only. If name/role/etc. are same but URLs differ, they are distinct.
                        if !seen_urls.contains(url) {
                            seen_urls.insert(url.clone());
                            media_urls.push(url.clone());
                        } else {
                            // URL already seen; skip adding duplicates across all buckets
                            continue;
                        }
                    }
                    // Classification rule: ANY media that is not explicitly an IMAGE is a VIDEO.
                    // Consider both __typename/__typeof and the generic "type" field. Compare case-insensitively.
                    let is_image = match
                        (
                            typename_opt.as_deref().map(|s| s.eq_ignore_ascii_case("IMAGE")),
                            mtype_opt.as_deref().map(|s| s.eq_ignore_ascii_case("IMAGE")),
                        )
                    {
                        (Some(true), _) | (_, Some(true)) => true,
                        _ => false,
                    };
                    if is_image {
                        if let Some(url) = &url_opt {
                            media_image_urls.push(url.clone());
                        }
                        media_images.push(media_obj);
                    } else {
                        if let Some(url) = &url_opt {
                            media_video_urls.push(url.clone());
                        }
                        media_videos.push(media_obj);
                    }
                }
            }
            // Attempt genre extraction (robust):
            // 1) Known keys containing genres at product-level
            // 2) Recursively scan any sub-object whose key contains "genre"
            let mut genres: Vec<String> = Vec::new();
            // Known direct keys: handle strings, arrays of strings, arrays of objects with displayName/key
            for key in ["productGenres", "genres", "genre", "productGenre"].iter() {
                if let Some(val) = it.get(*key) {
                    collect_genre_strings(val, &mut genres);
                }
            }
            // Recursive scan: any field whose key contains "genre" (case-insensitive)
            if let Some(obj) = it.as_object() {
                scan_object_for_genres(obj, &mut genres);
            }
            // Dedupe
            if !genres.is_empty() {
                genres.sort();
                genres.dedup();
            }
            out.push(PsProductSummary {
                product_id,
                concept_id,
                name,
                release_date,
                base_price_minor,
                discounted_price_minor,
                is_free,
                media_urls,
                media_image_urls,
                media_video_urls,
                media_images,
                media_videos,
                genres,
                average_rating: None,
                rating_count: None,
            });
        }
    }
    out
}

fn rating_metrics_from_star_obj(obj: &Value) -> Option<(f32, i64)> {
    let avg = obj.get("averageRating").and_then(|x| x.as_f64());
    let cnt = obj
        .get("ratingCount")
        .or_else(|| obj.get("totalRatingsCount"))
        .and_then(|x| x.as_i64());
    match (avg, cnt) {
        (Some(a), Some(c)) => Some((a as f32, c)),
        _ => None,
    }
}

fn extract_first_concept_id(value: &Value) -> Option<String> {
    let root = value.get("data")?.get("metGetConceptByProductIdQuery")?;

    if let Some(arr) = root.get("concepts").and_then(|c| c.as_array()) {
        for item in arr {
            if let Some(id) = item.get("id").and_then(|x| x.as_str()) {
                return Some(id.to_string());
            }
            if let Some(node) = item.get("node") {
                if let Some(id) = node.get("id").and_then(|x| x.as_str()) {
                    return Some(id.to_string());
                }
            }
        }
    }

    if
        let Some(edges) = root
            .get("concepts")
            .and_then(|c| c.get("edges"))
            .and_then(|e| e.as_array())
    {
        for edge in edges {
            if let Some(node) = edge.get("node") {
                if let Some(id) = node.get("id").and_then(|x| x.as_str()) {
                    return Some(id.to_string());
                }
            }
        }
    }

    None
}

fn collect_genre_strings(v: &Value, out: &mut Vec<String>) {
    match v {
        Value::String(s) => {
            if !s.is_empty() {
                out.push(s.to_string());
            }
        }
        Value::Array(arr) => {
            for el in arr {
                collect_genre_strings(el, out);
            }
        }
        Value::Object(o) => {
            // Common fields within genre entries
            for key in ["displayName", "name", "key", "genre"].iter() {
                if let Some(Value::String(s)) = o.get(*key) {
                    if !s.is_empty() {
                        out.push(s.to_string());
                    }
                }
            }
            // Also consider nested arrays/objects
            for (_k, vv) in o {
                collect_genre_strings(vv, out);
            }
        }
        _ => {}
    }
}

fn scan_object_for_genres(obj: &serde_json::Map<String, Value>, out: &mut Vec<String>) {
    for (k, v) in obj.iter() {
        let kl = k.to_lowercase();
        if kl.contains("genre") {
            collect_genre_strings(v, out);
        }
        // Recurse into nested containers as well to catch deeper occurrences
        match v {
            Value::Object(nested) => scan_object_for_genres(nested, out),
            Value::Array(arr) => {
                for el in arr {
                    if let Value::Object(n) = el {
                        scan_object_for_genres(n, out);
                    }
                }
            }
            _ => {}
        }
    }
}

/// Extract the Genre facet (productGenres) from a categoryGridRetrieve response.
/// JSON path: data.categoryGridRetrieve.facetOptions[] where name=="productGenres" → values[]
pub fn extract_genre_facet(v: &Value) -> Vec<GenreFacetValue> {
    let mut out = Vec::new();
    let Some(data) = v.get("data") else {
        return out;
    };
    let Some(cat) = data.get("categoryGridRetrieve") else {
        return out;
    };
    let Some(facets) = cat.get("facetOptions").and_then(|x| x.as_array()) else {
        return out;
    };
    for facet in facets {
        if facet.get("name").and_then(|n| n.as_str()) == Some("productGenres") {
            if let Some(vals) = facet.get("values").and_then(|x| x.as_array()) {
                for val in vals {
                    if
                        let (Some(key), Some(dn)) = (
                            val.get("key").and_then(|x| x.as_str()),
                            val.get("displayName").and_then(|x| x.as_str()),
                        )
                    {
                        let count = val
                            .get("count")
                            .and_then(|c| c.as_i64())
                            .unwrap_or(0);
                        out.push(GenreFacetValue {
                            key: key.to_string(),
                            display_name: dn.to_string(),
                            count,
                        });
                    }
                }
            }
        }
    }
    out
}

#[derive(Debug, Clone)]
struct PsProductSummaryWithDate {
    product_id: Option<String>,
    concept_id: Option<String>,
    name: Option<String>,
    release_year: Option<i32>,
}

// Enrichment moved to PsStoreClient impl for correct receiver type
impl PsStoreClient {
    /// Enrich already-fetched product summaries with detail + rating + genres.
    /// Concurrency bounded by PS_ENRICH_CONCURRENCY (default 6).
    pub async fn enrich_products(&self, locale: &str, items: &mut [PsProductSummary]) {
        use futures::Future;
        use futures::stream::{ FuturesUnordered, StreamExt };
        use std::pin::Pin;
        use tokio::sync::Semaphore;
        let sem = std::sync::Arc::new(
            Semaphore::new(
                std::env
                    ::var("PS_ENRICH_CONCURRENCY")
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(6)
            )
        );
        let mut futs: FuturesUnordered<
            Pin<Box<dyn Future<Output = (usize, Option<(f32, i64)>, Value)> + Send>>
        > = FuturesUnordered::new();
        for (idx, it) in items.iter().enumerate() {
            if let Some(pid) = &it.product_id {
                let pid_c = pid.clone();
                let self_c = self.clone();
                let locale_c = locale.to_string();
                let sem_c = sem.clone();
                let f: Pin<
                    Box<dyn Future<Output = (usize, Option<(f32, i64)>, Value)> + Send>
                > = Box::pin(async move {
                    let _permit = sem_c.acquire().await.ok();
                    let rating = self_c.product_star_rating(&locale_c, &pid_c).await.ok().flatten();
                    let detail = self_c
                        .product_detail_raw(&locale_c, &pid_c).await
                        .unwrap_or(Value::Null);
                    (idx, rating, detail)
                });
                futs.push(f);
            }
        }
        let mut details: Vec<Option<Value>> = vec![None; items.len()];
        while let Some((idx, rating_opt, detail)) = futs.next().await {
            if idx < items.len() {
                if let Some((avg, cnt)) = rating_opt {
                    items[idx].average_rating = Some(avg);
                    items[idx].rating_count = Some(cnt);
                }
                details[idx] = Some(detail);
            }
        }
        for (idx, it) in items.iter_mut().enumerate() {
            if let Some(detail) = &details[idx] {
                if it.release_date.is_none() {
                    let rd = detail
                        .get("data")
                        .and_then(|d| d.get("metGetProductById"))
                        .and_then(|m| m.get("releaseDate"))
                        .and_then(|r| r.as_str())
                        .map(|s| s.to_string())
                        .or_else(||
                            detail
                                .get("data")
                                .and_then(|d| d.get("metGetProductById"))
                                .and_then(|m| m.get("release_date"))
                                .and_then(|r| r.as_str())
                                .map(|s| s.to_string())
                        );
                    if rd.is_some() {
                        it.release_date = rd;
                    }
                }
                let mut g: Vec<String> = Vec::new();
                if let Some(prod) = detail.get("data").and_then(|d| d.get("metGetProductById")) {
                    for key in ["productGenres", "genres", "genre"].iter() {
                        if let Some(val) = prod.get(*key) {
                            collect_genre_strings(val, &mut g);
                        }
                    }
                }
                g.sort();
                g.dedup();
                if !g.is_empty() {
                    it.genres = g;
                }
            }
        }
    }
}

fn extract_product_summaries_with_date(v: &Value) -> Vec<PsProductSummaryWithDate> {
    fn first_items_array(v: &Value) -> Option<&Vec<Value>> {
        let data = v.get("data")?;
        if let Some(obj) = data.as_object() {
            for (_k, val) in obj {
                if let Some(arr) = val.get("products").and_then(|r| r.as_array()) {
                    return Some(arr);
                }
                if let Some(arr) = val.get("results").and_then(|r| r.as_array()) {
                    return Some(arr);
                }
                if let Some(grid) = val.get("grid") {
                    if let Some(arr) = grid.get("results").and_then(|r| r.as_array()) {
                        return Some(arr);
                    }
                }
            }
        }
        None
    }
    let mut out = Vec::new();
    if let Some(items) = first_items_array(v) {
        for it in items {
            let product_id = it
                .get("id")
                .or_else(|| it.get("productId"))
                .and_then(|x| x.as_str())
                .map(|s| s.to_string());
            let concept_id = it
                .get("conceptId")
                .or_else(|| it.get("conceptID"))
                .and_then(|x| x.as_str())
                .map(|s| s.to_string());
            let name = it
                .get("name")
                .and_then(|x| x.as_str())
                .map(|s| s.to_string())
                .or_else(||
                    it
                        .get("displayName")
                        .and_then(|x| x.as_str())
                        .map(|s| s.to_string())
                );
            // Try various date fields; store only year for cutoff logic.
            let year = it
                .get("releaseDate")
                .and_then(|d| d.as_str())
                .and_then(parse_year)
                .or_else(||
                    it
                        .get("release_date")
                        .and_then(|d| d.as_str())
                        .and_then(parse_year)
                )
                .or_else(||
                    it
                        .get("firstReleaseDate")
                        .and_then(|d| d.as_str())
                        .and_then(parse_year)
                )
                .or_else(||
                    it
                        .get("releaseDateStr")
                        .and_then(|d| d.as_str())
                        .and_then(parse_year)
                );
            out.push(PsProductSummaryWithDate { product_id, concept_id, name, release_year: year });
        }
    }
    out
}

fn parse_price_minor(price_val: Option<&Value>) -> (Option<i64>, Option<i64>, Option<bool>) {
    let Some(p) = price_val else {
        return (None, None, None);
    };
    let is_free = p.get("isFree").and_then(|f| f.as_bool());
    let base = p
        .get("basePrice")
        .and_then(|b| b.as_str())
        .and_then(|s| parse_money_to_minor(s));
    let discounted = p
        .get("discountedPrice")
        .and_then(|b| b.as_str())
        .and_then(|s| parse_money_to_minor(s));
    (base, discounted, is_free)
}

fn parse_money_to_minor(s: &str) -> Option<i64> {
    // Normalize: replace comma with dot, strip all except digits and single dot
    let mut normalized = s.replace(',', ".");
    normalized.retain(|c| (c.is_ascii_digit() || c == '.'));
    if normalized.is_empty() {
        return None;
    }
    // If multiple dots, keep first two parts only
    let mut parts = normalized.splitn(2, '.');
    let int_part = parts.next().unwrap_or("");
    let frac_part = parts.next();
    if let Some(frac) = frac_part {
        let mut frac_norm = frac
            .chars()
            .filter(|c| c.is_ascii_digit())
            .collect::<String>();
        if frac_norm.len() == 0 {
            frac_norm.push('0');
            frac_norm.push('0');
        } else if frac_norm.len() == 1 {
            frac_norm.push('0');
        } else if frac_norm.len() > 2 {
            frac_norm.truncate(2);
        }
        let total = format!("{}{}", int_part, frac_norm);
        total.parse::<i64>().ok()
    } else {
        int_part
            .parse::<i64>()
            .ok()
            .map(|v| v * 100)
    }
}

fn parse_year(s: &str) -> Option<i32> {
    // Accept YYYY or full date formats (YYYY-MM-DD / ISO8601)
    if s.len() >= 4 {
        let year_part = &s[0..4];
        if let Ok(y) = year_part.parse::<i32>() {
            return Some(y);
        }
    }
    None
}

#[derive(Debug, Clone)]
pub struct CategoryRequest {
    pub category_id: String,
    pub size: u32,
    pub offset: u32,
    pub sort_name: Option<String>,
    pub is_ascending: Option<bool>,
    pub filter_by: Vec<String>,
    pub facet_options: Vec<String>,
}

impl CategoryRequest {
    pub fn next_page(&self) -> Self {
        let mut n = self.clone();
        n.offset = n.offset.saturating_add(n.size);
        n
    }
}
