/*!
 * Xbox Store Price Checker
 * Standalone binary to query Xbox Store prices across multiple SKU regions
 */

use anyhow::{Context, Result};
use serde::Deserialize;
use serde_json::Value;
use std::collections::HashSet;
use std::env;

#[derive(Debug, Deserialize)]
struct TokenResponse {
    access_token: String,
    expires_in: i64,
}

async fn get_token() -> Result<String> {
    let tenant_id = env::var("AZURE_TENANT_ID")?;
    let client_id = env::var("AZURE_CLIENT_ID")?;
    let client_secret = env::var("AZURE_CLIENT_SECRET")?;

    let url = format!(
        "https://login.microsoftonline.com/{}/oauth2/v2.0/token",
        tenant_id
    );

    let params = [
        ("client_id", client_id.as_str()),
        ("client_secret", client_secret.as_str()),
        ("grant_type", "client_credentials"),
        ("scope", "https://onestore.microsoft.com/.default"),
    ];

    let client = reqwest::Client::new();
    let response = client.post(&url).form(&params).send().await?;

    if !response.status().is_success() {
        anyhow::bail!("Token request failed: {}", response.status());
    }

    let token_data: TokenResponse = response.json().await?;
    Ok(token_data.access_token)
}

async fn query_product(
    token: &str,
    product_id: &str,
    market: &str,
) -> Result<Value> {
    let url = format!(
        "https://displaycatalog.mp.microsoft.com/v7.0/products?bigIds={}&market={}&languages=en-us",
        product_id, market
    );

    let client = reqwest::Client::new();
    let response = client
        .get(&url)
        .bearer_auth(token)
        .header("MS-CV", "1")
        .send()
        .await?;

    if !response.status().is_success() {
        anyhow::bail!("API request failed: {}", response.status());
    }

    let result: Value = response.json().await?;
    Ok(result)
}

fn display_prices(result: &Value, game_name: &str, market: &str) {
    if let Some(products) = result["Products"].as_array() {
        if products.is_empty() {
            println!("  ⚠️  Not available in {} market", market);
            return;
        }

        for product in products {
            let title = product["ProductTitle"]
                .as_str()
                .unwrap_or(game_name);

            if let Some(skus) = product["DisplaySkuAvailabilities"].as_array() {
                for sku in skus {
                    if let Some(availabilities) = sku["Availabilities"].as_array() {
                        for availability in availabilities {
                            if let Some(price_obj) =
                                availability["OrderManagementData"]["Price"].as_object()
                            {
                                let list_price = price_obj
                                    .get("ListPrice")
                                    .and_then(|v| v.as_f64())
                                    .unwrap_or(0.0);

                                let msrp = price_obj
                                    .get("MSRP")
                                    .and_then(|v| v.as_f64())
                                    .unwrap_or(0.0);

                                let currency = price_obj
                                    .get("CurrencyCode")
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("???");

                                println!("  ✅ {}", title);
                                println!("  💰 {}: {:.2} {}", market, list_price, currency);

                                if msrp > 0.0 && (msrp - list_price).abs() > 0.01 {
                                    let discount =
                                        ((msrp - list_price) / msrp * 100.0).round();
                                    println!(
                                        "  🎉 {}% OFF (was {:.2} {})",
                                        discount, msrp, currency
                                    );
                                }
                                return;
                            }
                        }
                    }
                }
            }

            println!("  📦 {} (Price not available)", title);
        }
    } else {
        println!("  ⚠️  No data for {} market", market);
    }
}

fn env_bool(name: &str, default: bool) -> bool {
    match std::env::var(name) {
        Ok(v) => {
            let v = v.to_ascii_lowercase();
            matches!(v.as_str(), "1" | "true" | "yes" | "y" | "on")
        }
        Err(_) => default,
    }
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(default)
}

fn extract_product_ids_recursive(v: &Value, out: &mut HashSet<String>) {
    match v {
        Value::Object(map) => {
            for (k, val) in map {
                // Common shapes: {"productId":"..."} or {"ProductId":"..."}
                if (k.eq_ignore_ascii_case("productid") || k.eq_ignore_ascii_case("bigid"))
                    && val.is_string()
                {
                    if let Some(s) = val.as_str() {
                        // Most MS Store product IDs are 12 chars (e.g., 9NKX70BBCDRN)
                        // but keep this tolerant.
                        let s = s.trim();
                        if (10..=20).contains(&s.len()) {
                            out.insert(s.to_string());
                        }
                    }
                }
                extract_product_ids_recursive(val, out);
            }
        }
        Value::Array(arr) => {
            for item in arr {
                extract_product_ids_recursive(item, out);
            }
        }
        _ => {}
    }
}

async fn discover_product_ids_from_storeedge(
    market: &str,
    query: &str,
    max_pages: usize,
) -> Result<Vec<String>> {
    // StoreEdgeFD is the endpoint family used by Microsoft Store clients.
    // This is not an official public “list all games” API, but it is the
    // most practical way to discover product IDs at scale.
    //
    // NOTE: Parameters are inferred from observed client traffic. You may
    // need to tweak `deviceFamily` / `appversion` if Microsoft changes it.
    let base = "https://storeedgefd.dsx.mp.microsoft.com/v9.0/pages/search";

    let client = reqwest::Client::new();
    let mut all: HashSet<String> = HashSet::new();

    // StoreEdge often uses an opaque continuation token in responses.
    // We implement a tolerant loop:
    //  - Request page 0 without continuation
    //  - If response contains a continuationToken-like field, reuse it
    //  - Otherwise, stop after first page
    let mut continuation: Option<String> = None;

    for page in 0..max_pages {
        let mut req = client.get(base).query(&[
            ("market", market),
            ("locale", "en-us"),
            ("deviceFamily", "Windows.Xbox"),
            ("appversion", "22301.1401.0.0"),
            ("query", query),
        ]);

        if let Some(c) = &continuation {
            req = req.query(&[("continuationToken", c.as_str())]);
        }

        let resp = req
            .header("User-Agent", "Mozilla/5.0 (compatible; price-checker/1.0)")
            .send()
            .await?;

        if !resp.status().is_success() {
            anyhow::bail!("StoreEdge search failed: {}", resp.status());
        }

        let body: Value = resp.json().await?;

        // Extract any product ids we can find.
        extract_product_ids_recursive(&body, &mut all);

        // Try to locate a continuation token.
        // Observed fields can vary; keep it tolerant.
        let next = body
            .get("ContinuationToken")
            .and_then(|v| v.as_str())
            .or_else(|| body.get("continuationToken").and_then(|v| v.as_str()))
            .or_else(|| body.get("Continuation").and_then(|v| v.as_str()))
            .map(|s| s.to_string());

        if next.is_some() {
            continuation = next;
        } else {
            // If no continuation is present, we assume a single page.
            if page == 0 {
                break;
            }
        }
    }

    let mut v: Vec<String> = all.into_iter().collect();
    v.sort();
    Ok(v)
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();

    println!("\n🎮 Xbox Store Price Checker");
    println!("═══════════════════════════════════════════════════════════\n");

    println!("🔑 Authenticating...");
    let token = get_token().await.context("Failed to get access token")?;
    println!("✅ Authenticated!\n");

    // Optional catalog discovery:
    //   CATALOG_QUERY="" CATALOG_MARKET=US CATALOG_PAGES=5 CATALOG_MODE=1 cargo run
    // This will print discovered ProductIds and then exit.
    if env_bool("CATALOG_MODE", false) {
        let market = env::var("CATALOG_MARKET").unwrap_or_else(|_| "US".to_string());
        let query = env::var("CATALOG_QUERY").unwrap_or_else(|_| "".to_string());
        let pages = env_usize("CATALOG_PAGES", 3);

        println!("📚 Catalog discovery (StoreEdgeFD)");
        println!("   market={} query={:?} pages={}", market, query, pages);

        let ids = discover_product_ids_from_storeedge(&market, &query, pages)
            .await
            .context("Failed to discover product IDs")?;

        println!("\nDiscovered {} product IDs:", ids.len());
        for id in ids {
            println!("{}", id);
        }

        println!("\n✅ Done (catalog mode). Set CATALOG_MODE=0 to run price checks.");
        return Ok(());
    }

    // Product catalog - add your own product IDs here
    let products = vec![
        ("9NKX70BBCDRN", "Forza Horizon 5"),
        ("9P2N57MC619K", "Minecraft"),
        ("9N8G2FQ76HVC", "Halo Infinite"),
    ];

    // Markets to query
    let markets = vec!["US", "GB", "JP", "AU"];

    for (product_id, game_name) in &products {
        println!("═══════════════════════════════════════════════════════════");
        println!("🎮 {}", game_name);
        println!("═══════════════════════════════════════════════════════════\n");

        for market in &markets {
            println!("📍 {}:", market);

            match query_product(&token, product_id, market).await {
                Ok(result) => display_prices(&result, game_name, market),
                Err(e) => println!("  ❌ Error: {}", e),
            }
            println!();
        }
    }

    println!("═══════════════════════════════════════════════════════════");
    println!("\n✅ Done! To add more games, edit the products vector in src/xbox_store_prices.rs");

    Ok(())
}
