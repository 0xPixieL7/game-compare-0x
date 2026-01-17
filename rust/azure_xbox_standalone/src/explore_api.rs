/*!
 * Xbox Store API Explorer
 * Tests different API endpoints to discover what's available
 */

use anyhow::{Context, Result};
use serde::Deserialize;
use serde_json::json;
use std::env;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[derive(Debug, Deserialize)]
struct TokenResponse {
    #[serde(default)]
    access_token: String,
    #[serde(default)]
    token_type: String,
    #[serde(default)]
    expires_in: Option<u64>,
    // error fields (when request fails)
    #[serde(default)]
    error: String,
    #[serde(default)]
    error_description: String,
    #[serde(default)]
    error_codes: Option<Vec<i64>>,
    #[serde(default)]
    timestamp: String,
    #[serde(default)]
    trace_id: String,
    #[serde(default)]
    correlation_id: String,
}

#[derive(Debug, Deserialize)]
struct DeviceCodeResponse {
    device_code: String,
    user_code: String,
    verification_uri: String,
    expires_in: u64,
    interval: Option<u64>,
    message: Option<String>,
}

async fn get_token() -> Result<Option<String>> {
    let auth_mode = env::var("AZURE_AUTH_MODE").unwrap_or_else(|_| "secret".to_string());

    // Allow forcing no auth. Many catalog endpoints are publicly callable.
    if auth_mode == "none" {
        return Ok(None);
    }

    // Prefer a pre-supplied token if provided.
    if let Ok(t) = env::var("AZURE_ACCESS_TOKEN") {
        if !t.trim().is_empty() {
            return Ok(Some(t));
        }
    }

    let tenant_id = env::var("AZURE_TENANT_ID").context("Missing env var AZURE_TENANT_ID")?;
    let client_id = env::var("AZURE_CLIENT_ID").context("Missing env var AZURE_CLIENT_ID")?;
    let scope = env::var("AZURE_SCOPE").unwrap_or_else(|_| "openid profile offline_access".to_string());

    // DEVICE CODE FLOW (recommended for personal Microsoft accounts)
    if auth_mode == "device_code" {
        let device_code_url = format!(
            "https://login.microsoftonline.com/{}/oauth2/v2.0/devicecode",
            tenant_id
        );
        let token_url = format!(
            "https://login.microsoftonline.com/{}/oauth2/v2.0/token",
            tenant_id
        );

        let client = reqwest::Client::new();
        let dc_resp = client
            .post(&device_code_url)
            .form(&[("client_id", client_id.as_str()), ("scope", scope.as_str())])
            .send()
            .await?;

        let dc_status = dc_resp.status();
        let dc_text = dc_resp.text().await.unwrap_or_default();
        if !dc_status.is_success() {
            anyhow::bail!("Device code request failed HTTP={} body_prefix={:?}", dc_status, dc_text.chars().take(800).collect::<String>());
        }

        let dc: DeviceCodeResponse = serde_json::from_str(&dc_text).context("Failed to parse device code response")?;
        if let Some(m) = &dc.message {
            println!("{}", m);
        } else {
            println!(
                "To sign in, visit {} and enter code {}",
                dc.verification_uri, dc.user_code
            );
        }

        let interval = dc.interval.unwrap_or(5);
        let deadline = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::from_secs(0))
            .as_secs()
            + dc.expires_in;

        loop {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or(Duration::from_secs(0))
                .as_secs();
            if now >= deadline {
                anyhow::bail!("Device code expired before authorization completed");
            }

            let tok_resp = client
                .post(&token_url)
                .form(&[
                    ("grant_type", "urn:ietf:params:oauth:grant-type:device_code"),
                    ("client_id", client_id.as_str()),
                    ("device_code", dc.device_code.as_str()),
                ])
                .send()
                .await?;

            let status = tok_resp.status();
            let text = tok_resp.text().await.unwrap_or_default();
            let parsed: Result<TokenResponse, _> = serde_json::from_str(&text);
            let tr = match parsed {
                Ok(v) => v,
                Err(e) => {
                    anyhow::bail!(
                        "Token endpoint returned non-JSON. HTTP={} parse_err={} body_prefix={:?}",
                        status,
                        e,
                        text.chars().take(800).collect::<String>()
                    );
                }
            };

            if status.is_success() && !tr.access_token.is_empty() {
                return Ok(Some(tr.access_token));
            }

            // In device code flow, AAD returns these as OAuth errors while waiting.
            if tr.error == "authorization_pending" {
                tokio::time::sleep(Duration::from_secs(interval)).await;
                continue;
            }
            if tr.error == "slow_down" {
                tokio::time::sleep(Duration::from_secs(interval + 5)).await;
                continue;
            }

            let body_prefix = text.chars().take(1200).collect::<String>();
            let debug = json!({
                "http_status": status.as_u16(),
                "error": tr.error,
                "error_description": tr.error_description,
                "error_codes": tr.error_codes,
                "timestamp": tr.timestamp,
                "trace_id": tr.trace_id,
                "correlation_id": tr.correlation_id,
                "body_prefix": body_prefix,
                "hint": "If this is a personal Microsoft account, use AZURE_TENANT_ID=consumers and a known public client_id (e.g. Azure CLI 04b07795-8ddb-461a-bbee-02f9e1bf7b46)."
            });
            anyhow::bail!("Failed to get access token (device_code): {}", debug);
        }
    }

    // CLIENT CREDENTIALS (app-only)
    let client_secret = env::var("AZURE_CLIENT_SECRET")
        .context("Missing env var AZURE_CLIENT_SECRET (required for AZURE_AUTH_MODE=secret)")?;

    let url = format!(
        "https://login.microsoftonline.com/{}/oauth2/v2.0/token",
        tenant_id
    );

    // Default to the prior scope if user didn't set one.
    let scope = if scope.trim().is_empty() {
        "https://onestore.microsoft.com/.default".to_string()
    } else {
        scope
    };

    let params = [
        ("client_id", client_id.as_str()),
        ("client_secret", client_secret.as_str()),
        ("grant_type", "client_credentials"),
        ("scope", scope.as_str()),
    ];

    let client = reqwest::Client::new();
    let resp = client.post(&url).form(&params).send().await?;
    let status = resp.status();
    let text = resp.text().await.unwrap_or_default();

    let parsed: Result<TokenResponse, _> = serde_json::from_str(&text);
    let tr = match parsed {
        Ok(v) => v,
        Err(e) => {
            let prefix = text.chars().take(800).collect::<String>();
            anyhow::bail!(
                "Token endpoint returned non-JSON. HTTP={} parse_err={} body_prefix={:?}",
                status,
                e,
                prefix
            );
        }
    };

    if !status.is_success() || tr.access_token.is_empty() {
        let body_prefix = text.chars().take(1200).collect::<String>();
        let debug = json!({
            "http_status": status.as_u16(),
            "error": tr.error,
            "error_description": tr.error_description,
            "error_codes": tr.error_codes,
            "timestamp": tr.timestamp,
            "trace_id": tr.trace_id,
            "correlation_id": tr.correlation_id,
            "body_prefix": body_prefix,
            "hint": "For client_credentials, AZURE_TENANT_ID must be the tenant where the app is registered and admin consent must be granted for the requested scope."
        });
        anyhow::bail!("Failed to get access token (client_credentials): {}", debug);
    }

    Ok(Some(tr.access_token))
}

async fn test_endpoint(client: &reqwest::Client, token: &str, name: &str, url: &str) {
    println!("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("🧪 Testing: {}", name);
    println!("🔗 URL: {}", url);
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    let mut req = client.get(url).header("MS-CV", "1");
    if !token.trim().is_empty() {
        req = req.bearer_auth(token);
    }

    let response = req.send().await;

    match response {
        Ok(resp) => {
            let status = resp.status();
            println!("📊 Status: {}", status);

            if status.is_success() {
                if let Ok(body) = resp.text().await {
                    println!("✅ SUCCESS!");
                    println!("\n📄 Response preview (first 1000 chars):");
                    println!("{}", body.chars().take(1000).collect::<String>());

                    // Save to file
                    let filename = format!("api_response_{}.json", name.replace(" ", "_").to_lowercase());
                    if let Err(e) = std::fs::write(&filename, &body) {
                        println!("⚠️  Could not save response: {}", e);
                    } else {
                        println!("\n💾 Full response saved to: {}", filename);
                    }
                }
            } else {
                let error_body = resp.text().await.unwrap_or_default();
                println!("❌ FAILED");
                println!("Error: {}", error_body);
            }
        }
        Err(e) => {
            println!("❌ Request failed: {}", e);
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();

    println!("\n🔍 Xbox Store API Explorer");
    println!("═══════════════════════════════════════════════════════════\n");

    println!("🔑 Authenticating...");
    let token_opt = get_token().await.context("Failed to get access token")?;
    let token = token_opt.unwrap_or_else(|| "".to_string());
    if token.is_empty() {
        println!("⚠️  No bearer token; running requests without Authorization header.\n");
    } else {
        println!("✅ Authenticated!\n");
    }

    let client = reqwest::Client::new();
    let market = "US";

    println!("Testing various API endpoints...\n");

    // Test 1: Known working endpoint (single product)
    test_endpoint(
        &client,
        &token,
        "Single Product Query",
        &format!(
            "https://displaycatalog.mp.microsoft.com/v7.0/products?bigIds=9NKX70BBCDRN&market={}&languages=en-us",
            market
        ),
    ).await;

    // Test 2: Try search endpoint
    test_endpoint(
        &client,
        &token,
        "Search Endpoint",
        &format!(
            "https://displaycatalog.mp.microsoft.com/v7.0/products/search?query=forza&market={}&languages=en-us",
            market
        ),
    ).await;

    // Test 3: Try recommendations endpoint
    test_endpoint(
        &client,
        &token,
        "Recommendations",
        &format!(
            "https://displaycatalog.mp.microsoft.com/v7.0/products/recommendations?market={}&languages=en-us",
            market
        ),
    ).await;

    // Test 4: Try categories endpoint
    test_endpoint(
        &client,
        &token,
        "Categories",
        &format!(
            "https://displaycatalog.mp.microsoft.com/v7.0/categories?market={}&languages=en-us",
            market
        ),
    ).await;

    // Test 5: Try featured endpoint
    test_endpoint(
        &client,
        &token,
        "Featured",
        &format!(
            "https://displaycatalog.mp.microsoft.com/v7.0/products/featured?market={}&languages=en-us",
            market
        ),
    ).await;

    // Test 6: Try collections endpoint
    test_endpoint(
        &client,
        &token,
        "Collections",
        &format!(
            "https://displaycatalog.mp.microsoft.com/v7.0/collections?market={}&languages=en-us",
            market
        ),
    ).await;

    // Test 7: Try purchase API base
    test_endpoint(
        &client,
        &token,
        "Purchase API Base",
        "https://purchase.mp.microsoft.com/v7.0/",
    ).await;

    // Test 8: Try collections API base
    test_endpoint(
        &client,
        &token,
        "Collections API Base",
        "https://collections.mp.microsoft.com/v7.0/",
    ).await;

    // Test 9: Try browse with filter
    test_endpoint(
        &client,
        &token,
        "Browse with Filter",
        &format!(
            "https://displaycatalog.mp.microsoft.com/v7.0/browse?market={}&languages=en-us&filter=productType:Game",
            market
        ),
    ).await;

    // Test 10: Try query endpoint
    test_endpoint(
        &client,
        &token,
        "Query Endpoint",
        &format!(
            "https://displaycatalog.mp.microsoft.com/v7.0/query?market={}&languages=en-us",
            market
        ),
    ).await;

    println!("\n\n═══════════════════════════════════════════════════════════");
    println!("✅ API Exploration Complete!");
    println!("Check the api_response_*.json files for successful responses");
    println!("═══════════════════════════════════════════════════════════\n");

    Ok(())
}
