// API server implementation using actix-web

use crate::api::{auth, middleware, routes};
use crate::database_ops::db::Db;
use actix_web::{web, App, HttpServer};
use anyhow::{Context, Result};
use std::env;

pub struct ApiServer {
    pub host: String,
    pub port: u16,
    pub api_secret: String,
    pub allowed_origins: String,
    pub laravel_url: String,
}

impl ApiServer {
    /// Create server from environment variables
    pub fn from_env() -> Result<Self> {
        crate::util::env::init_env();

        let host = env::var("API_HOST").unwrap_or_else(|_| "0.0.0.0".to_string());
        let port = env::var("API_PORT")
            .unwrap_or_else(|_| "8080".to_string())
            .parse()
            .context("Invalid API_PORT")?;

        let api_secret =
            env::var("API_SECRET").context("API_SECRET environment variable is required")?;

        let allowed_origins = env::var("ALLOWED_ORIGINS")
            .unwrap_or_else(|_| "http://localhost:3000,http://localhost:8000".to_string());

        let laravel_url = env::var("LARAVEL_API_URL")
            .unwrap_or_else(|_| "http://localhost:8000".to_string());

        Ok(Self {
            host,
            port,
            api_secret,
            allowed_origins,
            laravel_url,
        })
    }

    /// Notify Laravel that the API server is running
    async fn notify_laravel(&self) {
        let client = reqwest::Client::new();
        let url = format!("{}/api/rust/status", self.laravel_url);
        let payload = serde_json::json!({
            "status": "online",
            "port": self.port,
            "host": self.host,
            "timestamp": chrono::Utc::now().to_rfc3339()
        });

        match client
            .post(&url)
            .header("Authorization", format!("Bearer {}", self.api_secret))
            .json(&payload)
            .send()
            .await
        {
            Ok(resp) => {
                if resp.status().is_success() {
                    tracing::info!("Successfully notified Laravel at {}", url);
                } else {
                    tracing::warn!(
                        "Failed to notify Laravel at {}: Status {}",
                        url,
                        resp.status()
                    );
                }
            }
            Err(e) => {
                tracing::warn!("Failed to notify Laravel at {}: {}", url, e);
            }
        }
    }

    /// Start the HTTP server
    pub async fn run(self, db: Db) -> Result<()> {
        let bind_addr = format!("{}:{}", self.host, self.port);

        tracing::info!(
            host = %self.host,
            port = %self.port,
            "Starting i-miss-rust API server"
        );

        // Notify Laravel asynchronously
        self.notify_laravel().await;

        let db_data = web::Data::new(db);
        let api_secret = self.api_secret.clone();
        let allowed_origins = self.allowed_origins.clone();

        HttpServer::new(move || {
            let (logger, compress) = middleware::setup_middleware();
            let cors = middleware::setup_cors(&allowed_origins);
            let auth = auth::Auth::new(api_secret.clone());

            App::new()
                .app_data(db_data.clone())
                .wrap(logger)
                .wrap(compress)
                .wrap(cors)
                .wrap(auth)
                .configure(routes::configure_routes)
        })
        .bind(&bind_addr)
        .with_context(|| format!("Failed to bind to {}", bind_addr))?
        .run()
        .await
        .context("HTTP server error")?;

        Ok(())
    }
}
