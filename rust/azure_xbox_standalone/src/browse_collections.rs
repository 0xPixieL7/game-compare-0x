/*!
 * Xbox Store Collections Browser
 * Discovers games using Microsoft Store collection endpoints (TopFree, TopPaid, etc.)
 * This solves the catalog browsing problem we had with the Display Catalog API
 */

use anyhow::{Context, Result};
use serde_json::Value;
use std::env;
use std::time::Duration;

fn log_reqwest_error(label: &str, url: &str, e: &reqwest::Error) {
    println!("❌ {label}: {e}");
    println!("   url: {url}");
    println!("   is_builder: {}", e.is_builder());
    println!("   is_request: {}", e.is_request());
    println!("   is_status:  {}", e.is_status());
    println!("   is_body:    {}", e.is_body());
    println!("   is_connect: {}", e.is_connect());
    println!("   is_timeout: {}", e.is_timeout());

    // Print the full causal chain.
    let mut i = 0usize;
    let mut cur: Option<&(dyn std::error::Error + 'static)> = Some(e);
    while let Some(err) = cur {
        println!("   cause[{i}]: {err}");
        cur = err.source();
        i += 1;
        if i > 10 {
            println!("   (cause chain truncated)");
            break;
        }
    }
}

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

async fn get_token() -> Result<String> {
    if let Ok(v) = std::env::var("XBL3_AUTH") {
        let v = v.trim().to_string();
        if v.is_empty() {
            anyhow::bail!("XBL3_AUTH was set but empty");
        }
        return Ok(v);
    }

    let uhs = std::env::var("XSTS_UHS")
        .map(|s| s.trim().to_string())
        .unwrap_or_default();
    let xsts = std::env::var("XSTS_TOKEN")
        .map(|s| s.trim().to_string())
        .unwrap_or_default();

    if uhs.is_empty() || xsts.is_empty() {
        anyhow::bail!(
            "Missing XSTS token env. Set either XBL3_AUTH or both XSTS_UHS and XSTS_TOKEN."
        );
    }

    Ok(format!("XBL3.0 x={};{}", uhs, xsts))
}

async fn browse_collection(
    token: &str,
    collection: &str,
    market: &str,
    count: u32,
) -> Result<Value> {
    // Device family differs across Store surfaces. Default to Android as requested.
    // Override at runtime with DEVICE_FAMILY=Windows.Xbox (or others) if needed.
    let device_family = env_or("DEVICE_FAMILY", "Android");
    let locale = env_or("LOCALE", "en-US");

    // Try each endpoint in order. If an endpoint fails due to transport/DNS issues,
    // continue to the next endpoint instead of bailing immediately.
    let attempts: Vec<(&str, String)> = vec![
        (
            "displaycatalog v7 collections",
            format!(
                "https://displaycatalog.mp.microsoft.com/v7.0/products/collections/Computed/{}?market={}&languages={}&itemType=Game&deviceFamily={}&count={}",
                collection,
                market,
                locale.to_lowercase(),
                device_family,
                count
            ),
        ),
        (
            "displaycatalog v8 collections",
            format!(
                "https://displaycatalog.mp.microsoft.com/v8.0/products/collections/Computed/{}?market={}&languages={}&itemType=Game&deviceFamily={}&count={}",
                collection,
                market,
                locale.to_lowercase(),
                device_family,
                count
            ),
        ),
        (
            "storeedgefd v9 page (fallback)",
            format!(
                "https://storeedgefd.dsx.mp.microsoft.com/v9.0/pages/pdp?market={}&locale={}&deviceFamily={}&itemType=Game",
                market,
                locale,
                device_family
            ),
        ),
    ];

    let client = reqwest::Client::builder()
        // Some Microsoft endpoints are flaky over HTTP/2 from non-browser clients.
        .http1_only()
        .timeout(Duration::from_secs(30))
        .connect_timeout(Duration::from_secs(10))
        // Keepalive improves reliability on some networks.
        .tcp_keepalive(Duration::from_secs(30))
        // Use a common browser-ish user agent to behave more like Microsoft Store clients.
        .user_agent("Mozilla/5.0 (Linux; Android 13) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36")
        .build()?;

    let mut last_err: Option<anyhow::Error> = None;

    for (label, url) in attempts {
        println!("🔗 Trying {label}: {url}");

        let resp = client
            .get(&url)
            .header(reqwest::header::AUTHORIZATION, token)
            .header(reqwest::header::ACCEPT, "application/json")
            .header(reqwest::header::ACCEPT_LANGUAGE, "en-US,en;q=0.9")
            .header(reqwest::header::ORIGIN, "https://www.xbox.com")
            .header(reqwest::header::REFERER, "https://www.xbox.com/")
            .header(reqwest::header::CACHE_CONTROL, "no-cache")
            .header(reqwest::header::PRAGMA, "no-cache")
            .header("MS-CV", "1")
            .send()
            .await;

        let mut resp = match resp {
            Ok(r) => r,
            Err(e) => {
                log_reqwest_error(&format!("request send failed ({label})"), &url, &e);
                println!("   hint: transport/DNS failure. Trying next endpoint...");
                last_err = Some(anyhow::anyhow!("{label}: {e}"));
                continue;
            }
        };

        if resp.status().is_success() {
            let result: Value = resp.json().await?;
            return Ok(result);
        }

        // Non-2xx: capture as much detail as possible and continue.
        let status = resp.status();
        let headers = resp.headers().clone();
        let body = resp.text().await.unwrap_or_default();
        println!("❌ {label} failed: {status}");
        println!("   headers: {:?}", headers);
        println!(
            "   body_prefix: {:?}",
            body.chars().take(200).collect::<String>()
        );

        last_err = Some(anyhow::anyhow!(
            "{label} failed: {status} body_prefix={:?}",
            body.chars().take(200).collect::<String>()
        ));

        // Continue to next endpoint pattern.
        // NOTE: resp has been consumed by .text() above.
        drop(resp);
    }

    Err(last_err.unwrap_or_else(|| anyhow::anyhow!("All collection endpoints failed")))
}

fn extract_product_ids(result: &Value) -> Vec<String> {
    let mut product_ids = Vec::new();

    // Try different response structures

    // Structure 1: Products array
    if let Some(products) = result.get("Products").and_then(|p| p.as_array()) {
        for product in products {
            if let Some(id) = product.get("ProductId").and_then(|i| i.as_str()) {
                product_ids.push(id.to_string());
            }
        }
    }

    // Structure 2: Items array
    if let Some(items) = result.get("Items").and_then(|i| i.as_array()) {
        for item in items {
            if let Some(id) = item.get("ProductId").and_then(|i| i.as_str()) {
                product_ids.push(id.to_string());
            }
        }
    }

    // Structure 3: Nested in Payload
    if let Some(payload) = result.get("Payload") {
        if let Some(results) = payload.get("Results").and_then(|r| r.as_array()) {
            for res in results {
                if let Some(id) = res.get("ProductId").and_then(|i| i.as_str()) {
                    product_ids.push(id.to_string());
                }
            }
        }
    }

    product_ids
}

fn display_results(result: &Value, limit: usize) {
    let product_ids = extract_product_ids(result);

    println!("\n📊 Found {} products", product_ids.len());
    println!("\n📋 Product IDs (first {}):", limit.min(product_ids.len()));

    for (i, id) in product_ids.iter().take(limit).enumerate() {
        println!("{}. {}", i + 1, id);
    }

    if product_ids.len() > limit {
        println!("\n... and {} more", product_ids.len() - limit);
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();

    let args: Vec<String> = env::args().collect();

    // Collection types to try: TopFree, TopPaid, New, Deal, BestRated, MostPlayed, ComingSoon
    let collection = args.get(1).map(|s| s.as_str()).unwrap_or("TopFree");
    let market = args.get(2).map(|s| s.as_str()).unwrap_or("US");
    let count: u32 = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(100);

    println!("\n🎮 Xbox Store Collections Browser");
    println!("═══════════════════════════════════════════════════════════");
    println!("Collection: {}", collection);
    println!("Market: {}", market);
    println!("Count: {}\n", count);
    println!("DeviceFamily: {}", env_or("DEVICE_FAMILY", "Android"));
    println!("Locale: {}\n", env_or("LOCALE", "en-US"));

    println!("🔑 Authenticating...");
    let token = get_token()
        .await
        .context("Failed to build XBL3 auth header. Set XBL3_AUTH or XSTS_UHS+XSTS_TOKEN")?;
    println!("✅ Authenticated (XBL3.0 header ready)!\n");

    println!("🔍 Querying collection API...\n");

    match browse_collection(&token, collection, market, count).await {
        Ok(result) => {
            display_results(&result, 20);

            // Save full response for analysis
            let filename = format!("collection_{}_{}_{}.json", collection.to_lowercase(), market, count);
            println!("\n💾 Saving full API response to {}...", filename);
            std::fs::write(
                &filename,
                serde_json::to_string_pretty(&result)?
            )?;
            println!("✅ Saved!");
        }
        Err(e) => {
            println!("\n❌ Failed to query collection API: {}", e);
            println!("\nNote: The collections API may require different authentication or parameters.");
            println!("Try running the find_products binary with search to discover product IDs instead.");
        }
    }

    println!("\n═══════════════════════════════════════════════════════════");
    println!("Available collections to try:");
    println!("  • TopFree    - Top free games");
    println!("  • TopPaid    - Top paid games");
    println!("  • New        - New releases");
    println!("  • Deal       - Current deals");
    println!("  • BestRated  - Highest rated");
    println!("  • MostPlayed - Most popular");
    println!("  • ComingSoon - Upcoming games");
    println!("\nUsage: cargo run --bin browse_collections -- <collection> <market> <count>");
    println!("Example: cargo run --bin browse_collections -- TopFree US 100\n");

    Ok(())
}
