use std::{ collections::HashMap, sync::Arc, time::Duration };
use governor::{ Quota, RateLimiter, state::keyed::DashMapStateStore, clock::DefaultClock };
use reqwest::{ header, Client };
use thiserror::Error;
use tokio::sync::Mutex;
use backoff::{ ExponentialBackoff, future::retry };

#[derive(Clone, Debug)]
pub struct PageTask {
    pub locale: String, // e.g. "en-us"
    pub page: u32,
    pub size: u32,
}

#[derive(Clone, Debug)]
pub struct ProviderItemIn {
    pub provider_key: String, // "psstore"
    pub external_item_id: String, // conceptId/productId
    pub external_sku: Option<String>,
    pub title: Option<String>,
}

#[derive(Clone, Debug)]
pub struct OfferIn {
    pub sellable_id: i64, // resolved/created earlier (software/hardware)
    pub retailer_id: i64, // "playstation"
    pub sku: Option<String>,
}

#[derive(Clone, Debug)]
pub struct OfferJurisdictionIn {
    pub offer_temp_key: String, // temp key like "{sellable}:{retailer}:{sku?}"
    pub jurisdiction_id: i64,
    pub currency_id: i64,
}

#[derive(Clone, Debug)]
pub struct PsConfig {
    pub base_url: String,
    pub bearer: Option<String>,
    pub locales: Vec<String>, // e.g. ["en-us", "en-gb"]
    pub rps: u32,
    pub extra_headers: HashMap<String, String>,
}

impl Default for PsConfig {
    fn default() -> Self {
        Self {
            base_url: "https://web.np.playstation.com/api/graphql/v1/".into(),
            bearer: None,
            locales: vec!["en-us".into()],
            rps: 3,
            extra_headers: HashMap::new(),
        }
    }
}
impl PsConfig {
    pub fn basic_config() -> PsConfig {
        let mut config = PsConfig {
            base_url: "https://web.np.playstation.com/api/graphql/v1/",
            bearer: None,
            country_w_language: vec![
                "en-us",
                "en-gb",
                "fr-fr",
                "de-de",
                "es-es",
                "it-it",
                "ja-jp",
                "en-au",
                "ca-en",
                "pt-br",
                "es-mx",
                "ru-ru",
                "ar-sa",
                "en-za",
                "en-ch",
                "fr-ch",
                "it-ch",
                "de-ch",
                "zh-tw",
                "zh-cn",
                "ko-kr",
                "nl-nl",
                "sv-se",
                "da-dk",
                "no-no",
                "fi-fi",
                "pl-pl",
                "pt-pt",
                "tr-tr"
            ],
            rps: 3_u32,
            extra_headers: None,
        };
        return config;
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
    etags: Arc<Mutex<HashMap<String, String>>>,
}

impl PsStoreClient {
    pub fn new(cfg: PsConfig) -> Self {
        let mut headers = header::HeaderMap::new();
        headers.insert(header::ACCEPT, header::HeaderValue::from_static("application/json"));
        headers.insert(
            header::USER_AGENT,
            header::HeaderValue::from_static(
                " Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/"
            )
        );
        if let Some(b) = &cfg.bearer {
            headers.insert(header::AUTHORIZATION, format!("Bearer {b}").parse().unwrap());
        }
        if let Some(loc) = cfg.locales.get(0) {
            headers.insert(header::ACCEPT_LANGUAGE, loc.parse().unwrap());
        }
        for (k, v) in &cfg.extra_headers {
            headers.insert(
                header::HeaderName::from_bytes(k.as_bytes()).unwrap(),
                header::HeaderValue::from_str(v).unwrap()
            );
        }
        let http = Client::builder()
            .default_headers(headers)
            .gzip(true)
            .brotli(true)
            .deflate(true)
            .pool_idle_timeout(Duration::from_secs(50))
            .pool_max_idle_per_host(70)
            .tcp_keepalive(Duration::from_secs(60))
            .build()
            .unwrap();
        let limiter = RateLimiter::keyed(
            Quota::per_second(std::num::NonZeroU32::new(cfg.rps.max(3)).unwrap())
        );
        Self {
            http,
            cfg: Arc::new(cfg),
            limiter: Arc::new(limiter),
            etags: Arc::new(Mutex::new(HashMap::new())),
        }
    }
    fn url(&self, path: &str) -> String {
        if path.starts_with("http") {
            path.to_string()
        } else {
            format!("{}/{}", self.cfg.base_url.trim_end_matches('/'), path.trim_start_matches('/'))
        }
    }
}
