/*!
 * Xbox Store New Releases Browser
 * Fetches games from Xbox Store sorted by release date
 */

use anyhow::{Context, Result};
use serde::Deserialize;
use serde_json::Value;
use psstore_client::get_env;
use std::env;

#[derive(Debug, Deserialize)]
struct TokenResponse {
    access_token: String,
}

async fn get_token() -> Result<String> {
    let tenant_id = get_env("AZURE_TENANT_ID")?;
    let client_id = get_env("AZURE_CLIENT_ID")?;
    let client_secret = get_env("AZURE_CLIENT_SECRET")?;

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
    let token_data: TokenResponse = response.json().await?;
    Ok(token_data.access_token)
}

async fn browse_new_releases(
    token: &str,
    market: &str,
    skip: u32,
    top: u32,
) -> Result<Value> {
    // Try multiple API endpoints to find games

    // Endpoint 1: Try collections/new endpoint
    let url = format!(
        "https://displaycatalog.mp.microsoft.com/v7.0/products/collections/newGames?market={}&languages=en-us&skipItems={}&top={}",
        market, skip, top
    );

    println!("🔗 Trying: {}", url);

    let client = reqwest::Client::new();
    let response = client
        .get(&url)
        .bearer_auth(token)
        .header("MS-CV", "1")
        .send()
        .await?;

    if response.status().is_success() {
        let result: Value = response.json().await?;
        return Ok(result);
    }

    println!("❌ Collections endpoint failed: {}", response.status());

    // Endpoint 2: Try browse with category filter
    let url2 = format!(
        "https://displaycatalog.mp.microsoft.com/v7.0/products/browse?market={}&languages=en-us&skipItems={}&top={}&categoryId=Games&orderBy=releaseDate&sortOrder=desc",
        market, skip, top
    );

    println!("🔗 Trying: {}", url2);

    let response2 = client
        .get(&url2)
        .bearer_auth(token)
        .header("MS-CV", "1")
        .send()
        .await?;

    if response2.status().is_success() {
        let result: Value = response2.json().await?;
        return Ok(result);
    }

    println!("❌ Browse endpoint failed: {}", response2.status());

    // Endpoint 3: Try direct products query with filters
    let url3 = format!(
        "https://displaycatalog.mp.microsoft.com/v7.0/products?market={}&languages=en-us&skipItems={}&top={}&ProductFamilyNames=Games",
        market, skip, top
    );

    println!("🔗 Trying: {}", url3);

    let response3 = client
        .get(&url3)
        .bearer_auth(token)
        .header("MS-CV", "1")
        .send()
        .await?;

    if !response3.status().is_success() {
        let status = response3.status();
        let error_text = response3.text().await.unwrap_or_default();
        anyhow::bail!("All API endpoints failed. Last error: {} - {}", status, error_text);
    }

    let result: Value = response3.json().await?;
    Ok(result)
}

fn extract_and_display_products(result: &Value, limit: usize) {
    let products = result["Products"]
        .as_array()
        .or_else(|| result["Items"].as_array());

    if let Some(products) = products {
        println!("\n📊 Found {} products\n", products.len());

        for (i, product) in products.iter().take(limit).enumerate() {
            // Try different fields for title
            let title = product["LocalizedProperties"][0]["ProductTitle"]
                .as_str()
                .or_else(|| product["ProductTitle"].as_str())
                .unwrap_or("Unknown Title");

            let product_id = product["ProductId"]
                .as_str()
                .unwrap_or("Unknown ID");

            // Try to get release date
            let release_date = product["MarketProperties"][0]["OriginalReleaseDate"]
                .as_str()
                .or_else(|| product["ReleaseDate"].as_str())
                .or_else(|| product["OriginalReleaseDate"].as_str())
                .unwrap_or("Unknown");

            // Get product type/category
            let product_type = product["ProductType"]
                .as_str()
                .unwrap_or("Unknown");

            println!("{}. {}", i + 1, title);
            println!("   ID: {}", product_id);
            println!("   Type: {}", product_type);
            println!("   Release: {}", release_date);

            // Try to get price
            if let Some(skus) = product["DisplaySkuAvailabilities"].as_array() {
                if let Some(sku) = skus.first() {
                    if let Some(availabilities) = sku["Availabilities"].as_array() {
                        if let Some(availability) = availabilities.first() {
                            if let Some(price_obj) = availability["OrderManagementData"]["Price"].as_object() {
                                let price = price_obj.get("ListPrice").and_then(|v| v.as_f64()).unwrap_or(0.0);
                                let currency = price_obj.get("CurrencyCode").and_then(|v| v.as_str()).unwrap_or("USD");
                                println!("   Price: {:.2} {}", price, currency);
                            }
                        }
                    }
                }
            }

            println!();
        }
    } else {
        println!("\n❌ No products found in response");
        println!("\nAPI Response structure:");
        println!("{}", serde_json::to_string_pretty(&result).unwrap_or_default().chars().take(500).collect::<String>());
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();

    let args: Vec<String> = env::args().collect();
    let market = args.get(1).map(|s| s.as_str()).unwrap_or("US");
    let limit: usize = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(20);

    println!("\n🎮 Xbox Store New Releases Browser");
    println!("═══════════════════════════════════════════════════════════");
    println!("Market: {}", market);
    println!("Limit: {} games\n", limit);

    println!("🔑 Authenticating...");
    let token = get_token().await.context("Failed to get access token")?;
    println!("✅ Authenticated!\n");

    println!("🔍 Querying Xbox Store API...\n");

    match browse_new_releases(&token, market, 0, limit as u32).await {
        Ok(result) => {
            extract_and_display_products(&result, limit);

            // Save full response for analysis
            println!("\n💾 Saving full API response to api_response.json...");
            std::fs::write(
                "api_response.json",
                serde_json::to_string_pretty(&result)?
            )?;
            println!("✅ Saved! Check api_response.json for full details");
        }
        Err(e) => {
            println!("\n❌ Failed to query API: {}", e);
            println!("\nNote: The Xbox Display Catalog API may not support browsing all games.");
            println!("The API is primarily designed for querying specific product IDs.");
            println!("\nAlternative: Use the Microsoft Store website to browse new releases:");
            println!("https://www.microsoft.com/en-us/store/new/games/xbox");
        }
    }

    Ok(())
}
