/*!
 * Xbox Store Product Search
 * Search for products by name and get their IDs
 */

use anyhow::{Context, Result};
use serde::Deserialize;
use serde_json::Value;
use std::env;
use psstore_client::get_env;

#[derive(Debug, Deserialize)]
struct TokenResponse {
    access_token: String,
}



async fn get_token() -> Result<String> {
    let tenant_id = get_env("AZURE_TENANT_ID");
    let client_id = get_env("AZURE_CLIENT_ID");
    let client_secret = get_env("AZURE_CLIENT_SECRET");

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

async fn search_products(token: &str, query: &str, market: &str) -> Result<Value> {
    // Use the search endpoint
    let url = format!(
        "https://www.microsoft.com/store/games/xbox/search/{}?market={}&form=games",
        urlencoding::encode(query), market
    );

    let client = reqwest::Client::new();
    let response = client
        .get(&url)
        .bearer_auth(token)
        .header("MS-CV", "1")
        .send()
        .await?;

    if !response.status().is_success() {
        anyhow::bail!("Search failed: {}", response.status());
    }

    let result: Value = response.json().await?;
    Ok(result)
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();

    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        println!("\nUsage: cargo run --bin find_products -- <search term>");
        println!("Example: cargo run --bin find_products -- \"FIFA 25\"\n");
        return Ok(());
    }

    let search_term = args[1..].join(" ");

    println!("\n🔍 Xbox Store Product Search");
    println!("═══════════════════════════════════════════════════════════");
    println!("Searching for: {}\n", search_term);

    println!("🔑 Authenticating...");
    let token = get_token().await.context("Failed to get access token")?;

    println!("🔎 Searching US market...\n");

    match search_products(&token, &search_term, "US").await {
        Ok(result) => {
            if let Some(products) = result["Products"].as_array() {
                if products.is_empty() {
                    println!("❌ No products found for '{}'", search_term);
                } else {
                    println!("Found {} result(s):\n", products.len());

                    for (i, product) in products.iter().take(10).enumerate() {
                        let title = product["LocalizedProperties"][0]["ProductTitle"]
                            .as_str()
                            .or_else(|| product["ProductTitle"].as_str())
                            .unwrap_or("Unknown Title");

                        let product_id = product["ProductId"]
                            .as_str()
                            .unwrap_or("Unknown ID");

                        let product_type = product["ProductType"]
                            .as_str()
                            .unwrap_or("Unknown");

                        println!("{}. {}", i + 1, title);
                        println!("   Product ID: {}", product_id);
                        println!("   Type: {}", product_type);

                        if let Some(price_obj) = product["DisplaySkuAvailabilities"][0]["Availabilities"][0]["OrderManagementData"]["Price"].as_object() {
                            if let Some(price) = price_obj.get("ListPrice").and_then(|v| v.as_f64()) {
                                let currency = price_obj.get("CurrencyCode").and_then(|v| v.as_str()).unwrap_or("USD");
                                println!("   Price: {:.2} {}", price, currency);
                            }
                        }
                        println!();
                    }
                }
            } else {
                println!("❌ Unexpected API response format");
            }
        }
        Err(e) => {
            println!("❌ Search failed: {}", e.to_string());
            println!("\nNote: The search endpoint may not be available with current API access.");
            println!("Try browsing https://www.microsoft.com/store/games/xbox instead.");
        }
        
    }

    Ok(())
}
