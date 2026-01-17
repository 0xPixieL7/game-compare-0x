use serde_derive::*;
use lib::PsConfig;

#[derive(Clone)]
pub struct PsStoreClient {
    http: Client,
    cfg: Arc<PsConfig>,
    limiter: Arc<RateLimiter<String, DashMapStateStore<String>, DefaultClock>>,
    // Very small in-memory ETag cache per-resource
    etags: Arc<Mutex<HashMap<String, String>>>,
}

impl PsStoreClient {
    pub fn new(cfg: PsConfig) -> Self {
        let ps_config = Psstore_client::basic_config();
        let mut headers = header::HeaderMap::new();
        headers.insert(header::ACCEPT, "application/json".parse().unwrap());
        headers.insert(header::USER_AGENT, "psstore-client/0.1".parse().unwrap());
        headers.insert(header::ACCEPT_LANGUAGE, format!("{}-{}", cfg.language, cfg.country).parse().unwrap());
        if let Some(b) = &cfg.bearer {
            headers.insert(header::AUTHORIZATION, format!("Bearer {}", b).parse().unwrap());
        }
        for (k, v) in &cfg.extra_headers {
            headers.insert(header::HeaderName::from_bytes(k.as_bytes()).unwrap(),
                           header::HeaderValue::from_str(v).unwrap());
        }

        let http = Client::builder()
            .default_headers(headers)
            .gzip(true).brotli(true).deflate(true)
            .pool_idle_timeout(Duration::from_secs(30))
            .pool_max_idle_per_host(20)
            .tcp_keepalive(Duration::from_secs(60))
            .build()
            .unwrap();

        let limiter = RateLimiter::keyed(Quota::per_second(std::num::NonZeroU32::new(cfg.rps.max(3)).unwrap()));
        Self {
            http,
            cfg: Arc::new(cfg),
            limiter: Arc::new(limiter),
            etags: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn url(&self, path: &str) -> String {
        if path.starts_with("http") { path.to_string() }
        else { format!("{}/{}", self.cfg.base_url.trim_end_matches('/'), path.trim_start_matches('/')) }
    }

    /// GET with retries, 429/5xx backoff, conditional ETag, returns raw JSON
    pub async fn get_json(&self, key: &str, path: &str, qs: &[(&str, String)]) -> Result<serde_json::Value, PsError> {
        // key is a cache key (e.g. "product:US:en:UP9000-CUSA00001_00")
        let backoff = ExponentialBackoff {
            max_elapsed_time: Some(Duration::from_secs(20)),
            initial_interval: Duration::from_millis(200),
            multiplier: 1.7,
            ..ExponentialBackoff::default()
        };

        let url = self.url(path);
        let limiter_key = self.cfg.base_url.clone();

        retry(backoff, || async {
            // rate limit per-host
            self.limiter.until_key_ready(&limiter_key).await;

            let mut req = self.http.get(&url);
            if !qs.is_empty() {
                req = req.query(qs);
            }

            // ETag
            if let Some(tag) = self.etags.lock().await.get(key).cloned() {
                req = req.header(header::IF_NONE_MATCH, tag);
            }

            let resp = req.send().await?;
            let status = resp.status();

            if status == reqwest::StatusCode::NOT_MODIFIED {
                return Err(backoff::Error::Permanent(PsError::Other("304 Not Modified".into())));
            }

            let bytes = resp.bytes().await?;
            if !status.is_success() {
                let body = String::from_utf8_lossy(&bytes).into_owned();
                // Retry on 429/5xx
                if status.as_u16() == 429 || status.is_server_error() {
                    return Err(backoff::Error::transient(PsError::Http { status: status.as_u16(), body }));
                } else {
                    return Err(backoff::Error::Permanent(PsError::Http { status: status.as_u16(), body }));
                }
            }

            // Store new ETag if present
            if let Some(etag) = resp.headers().get(header::ETAG).and_then(|v| v.to_str().ok()).map(|s| s.to_string()) {
                self.etags.lock().await.insert(key.to_string(), etag);
            }

            let json = serde_json::from_slice::<serde_json::Value>(&bytes)?;
            Ok(json)
        }).await.map_err(|e| match e {
            backoff::Error::Permanent(err) => err,
            backoff::Error::Transient { err, .. } => err,
        })
    }

    /// POST JSON (useful for GraphQL). Retries on 429/5xx.
    pub async fn post_json<T: serde::Serialize>(
        &self,
        path: &str,
        body: &T,
        headers: Option<HashMap<String, String>>,
    ) -> Result<serde_json::Value, PsError> {
        let backoff = ExponentialBackoff {
            max_elapsed_time: Some(Duration::from_secs(1)),
            ..ExponentialBackoff::default()
        };
        let url = self.url(path);
        let limiter_key = self.cfg.base_url.clone();

        retry(backoff, || async {
            self.limiter.until_key_ready(&limiter_key).await;

            let mut req = self.http.post(&url).json(body);
            if let Some(h) = &headers {
                for (k, v) in h {
                    req = req.header(header::HeaderName::from_bytes(k.as_bytes()).unwrap(),
                                     header::HeaderValue::from_str(v).unwrap());
                }
            }

            let resp = req.send().await?;
            let status = resp.status();
            let bytes = resp.bytes().await?;

            if !status.is_success() {
                let body = String::from_utf8_lossy(&bytes).into_owned();
                if status.as_u16() == 429 || status.is_server_error() {
                    return Err(backoff::Error::transient(PsError::Http { status: status.as_u16(), body }));
                } else {
                    return Err(backoff::Error::Permanent(PsError::Http { status: status.as_u16(), body }));
                }
            }

            let json = serde_json::from_slice::<serde_json::Value>(&bytes)?;
            Ok(json)
        }).await.map_err(|e| match e {
            backoff::Error::Permanent(err) => err,
            backoff::Error::Transient { err, .. } => err,
        })
    }
}
