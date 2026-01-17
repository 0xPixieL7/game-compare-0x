use anyhow::{anyhow, Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{collections::HashMap, env, time::{SystemTime, UNIX_EPOCH}};

#[derive(Debug, Deserialize)]
struct TokenResponse {
    access_token: String,
}

#[derive(Debug, Serialize)]
struct Output {
    generated_at_epoch: u64,
    markets: Vec<MarketResult>,
}

#[derive(Debug, Serialize)]
struct MarketResult {
    market: String,
    categories: Vec<CategoryResult>,
    collections: Vec<CollectionResult>,
}

#[derive(Debug, Serialize)]
struct CategoryResult {
    id: String,
    name: String,
    total: Option<u64>,
    tops: HashMap<usize, Vec<String>>, // top_n -> product ids
}

#[derive(Debug, Serialize)]
struct CollectionResult {
    name: String,
    tops: HashMap<usize, Vec<String>>,
}

const TOP_SIZES: [usize; 3] = [20, 50, 100];
const DEFAULT_MARKETS: &str = "US,GB,DE,JP,AU";
const COLLECTIONS: &[&str] = &["TopPaid", "TopFree", "BestRated", "MostPlayed"];

async fn get_token() -> Result<String> {
    let tenant_id = get_env("AZURE_TENANT_ID").context("AZURE_TENANT_ID not set")?;
    let client_id = get_env("AZURE_CLIENT_ID").context("AZURE_CLIENT_ID not set")?;
    let client_secret = get_env("AZURE_CLIENT_SECRET").context("AZURE_CLIENT_SECRET not set")?;

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
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(anyhow!("Token request failed: {} — {}", status, body));
    }
    let token_data: TokenResponse = response.json().await?;
    Ok(token_data.access_token)
}

async fn fetch_categories(client: &reqwest::Client, token: &str, market: &str) -> Result<Value> {
    // Try a few category endpoints; return first success
    let urls = vec![
        format!(
            "https://displaycatalog.mp.microsoft.com/v7.0/categories?market={}&languages=en-us&categoryType=Games&deviceFamily=Windows.Xbox",
            market
        ),
        format!(
            "https://displaycatalog.mp.microsoft.com/v7.0/categories?market={}&languages=en-us&deviceFamily=Windows.Xbox",
            market
        ),
    ];

    for url in urls {
        let resp = client
            .get(&url)
            .bearer_auth(token)
            .header("MS-CV", "1")
            .send()
            .await?;

        if resp.status().is_success() {
            return Ok(resp.json().await?);
        }
    }

    Err(anyhow!("All category endpoints failed for market {}", market))
}

#[derive(Debug)]
struct CategoryMeta {
    id: String,
    name: String,
}

fn extract_categories(payload: &Value) -> Vec<CategoryMeta> {
    let mut categories = Vec::new();

    if let Some(items) = payload.get("Categories").and_then(|v| v.as_array()) {
        for item in items {
            if let Some(id) = item.get("Id").and_then(|v| v.as_str()) {
                let name = item
                    .get("Name")
                    .and_then(|v| v.as_str())
                    .or_else(|| {
                        item.get("LocalizedProperties").and_then(|lp| lp
                            .as_array()
                            .and_then(|arr| arr.first())
                            .and_then(|first| first.get("Name").and_then(|v| v.as_str())))
                    })
                    .unwrap_or(id)
                    .to_string();
                categories.push(CategoryMeta {
                    id: id.to_string(),
                    name,
                });
            }
        }
    }

    if categories.is_empty() {
        if let Some(items) = payload.get("Items").and_then(|v| v.as_array()) {
            for item in items {
                if let Some(id) = item.get("CategoryId").and_then(|v| v.as_str()) {
                    let name = item
                        .get("Name")
                        .and_then(|v| v.as_str())
                        .unwrap_or(id)
                        .to_string();
                    categories.push(CategoryMeta {
                        id: id.to_string(),
                        name,
                    });
                }
            }
        }
    }

    categories
}

async fn fetch_category_top(
    client: &reqwest::Client,
    token: &str,
    market: &str,
    category_id: &str,
    top: usize,
) -> Result<Value> {
    // Use products endpoint filtered by category. orderBy=rank attempts chart-like ordering
    let url = format!(
        "https://displaycatalog.mp.microsoft.com/v7.0/products?market={}&languages=en-us&categoryId={}&deviceFamily=Windows.Xbox&productFamilyNames=Games&orderBy=rank&top={}&skipItems=0",
        market, category_id, top
    );

    let resp = client
        .get(&url)
        .bearer_auth(token)
        .header("MS-CV", "1")
        .send()
        .await?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(anyhow!(
            "Category {} ({}): request failed {} — {}",
            category_id,
            market,
            status,
            body
        ));
    }

    Ok(resp.json().await?)
}

async fn fetch_collection(
    client: &reqwest::Client,
    token: &str,
    market: &str,
    collection: &str,
    top: usize,
) -> Result<Value> {
    // Use v7.0 collections first, then v8.0 as fallback
    let url1 = format!(
        "https://displaycatalog.mp.microsoft.com/v7.0/products/collections/{}?market={}&languages=en-us&count={}&deviceFamily=Windows.Xbox",
        collection, market, top
    );

    let resp1 = client
        .get(&url1)
        .bearer_auth(token)
        .header("MS-CV", "1")
        .send()
        .await?;

    if resp1.status().is_success() {
        return Ok(resp1.json().await?);
    }

    let url2 = format!(
        "https://displaycatalog.mp.microsoft.com/v8.0/products/collections/Computed/{}?market={}&languages=en-us&itemType=Game&deviceFamily=Windows.Xbox&count={}",
        collection, market, top
    );

    let resp2 = client
        .get(&url2)
        .bearer_auth(token)
        .header("MS-CV", "1")
        .send()
        .await?;

    if !resp2.status().is_success() {
        let status = resp2.status();
        let body = resp2.text().await.unwrap_or_default();
        return Err(anyhow!(
            "Collection {} ({}): request failed {} — {}",
            collection,
            market,
            status,
            body
        ));
    }

    Ok(resp2.json().await?)
}

fn extract_product_ids(payload: &Value) -> Vec<String> {
    let mut ids = Vec::new();

    if let Some(products) = payload.get("Products").and_then(|v| v.as_array()) {
        for product in products {
            if let Some(id) = product.get("ProductId").and_then(|v| v.as_str()) {
                ids.push(id.to_string());
            }
        }
    }

    if let Some(items) = payload.get("Items").and_then(|v| v.as_array()) {
        for item in items {
            if let Some(id) = item.get("ProductId").and_then(|v| v.as_str()) {
                ids.push(id.to_string());
            }
        }
    }

    if let Some(payload) = payload.get("Payload") {
        if let Some(results) = payload.get("Results").and_then(|v| v.as_array()) {
            for res in results {
                if let Some(id) = res.get("ProductId").and_then(|v| v.as_str()) {
                    ids.push(id.to_string());
                }
            }
        }
    }

    ids
}

fn extract_total_count(payload: &Value) -> Option<u64> {
    payload
        .get("PagingInfo")
        .and_then(|v| v.get("TotalRecords"))
        .and_then(|v| v.as_u64())
        .or_else(|| payload.get("TotalResultCount").and_then(|v| v.as_u64()))
        .or_else(|| payload.get("totalItems").and_then(|v| v.as_u64()))
}

fn parse_markets() -> Vec<String> {
    env::var("XBOX_MARKETS")
        .unwrap_or_else(|_| DEFAULT_MARKETS.to_string())
        .split(',')
        .filter_map(|s| {
            let trimmed = s.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed.to_uppercase())
            }
        })
        .collect()
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();

    let markets = parse_markets();
    if markets.is_empty() {
        return Err(anyhow!("No markets configured. Set XBOX_MARKETS or use default list."));
    }

    println!("\n🎮 Xbox Store Genre & Charts Harvester");
    println!("═══════════════════════════════════════════════════════════");
    println!("Markets: {:?}", markets);
    println!("Top sizes: {:?}\n", TOP_SIZES);

    println!("🔑 Authenticating...");
    let token = get_token().await?;
    println!("✅ Authenticated!\n");

    let client = reqwest::Client::new();
    let mut market_results = Vec::new();

    for market in markets {
        println!("🌍 Processing market: {}", market);
        let categories_payload = match fetch_categories(&client, &token, &market).await {
            Ok(data) => data,
            Err(err) => {
                eprintln!("⚠️  Skipping categories for {}: {}", market, err);
                Value::Null
            }
        };

        let categories_meta = extract_categories(&categories_payload);
        if categories_meta.is_empty() {
            eprintln!("⚠️  No categories discovered for {} — you may need to supply CategoryIds manually.", market);
        }

        let mut categories_out = Vec::new();

        for cat in categories_meta.iter() {
            println!("  • {} ({})", cat.name, cat.id);
            let mut tops = HashMap::new();
            let mut total: Option<u64> = None;

            for &top in TOP_SIZES.iter() {
                match fetch_category_top(&client, &token, &market, &cat.id, top).await {
                    Ok(resp) => {
                        let ids = extract_product_ids(&resp);
                        if total.is_none() {
                            total = extract_total_count(&resp);
                        }
                        tops.insert(top, ids);
                    }
                    Err(err) => {
                        eprintln!("    ⚠️  top {} failed for {} ({}): {}", top, cat.name, market, err);
                    }
                }
            }

            categories_out.push(CategoryResult {
                id: cat.id.clone(),
                name: cat.name.clone(),
                total,
                tops,
            });
        }

        let mut collection_results = Vec::new();
        for &collection in COLLECTIONS {
            let mut tops = HashMap::new();
            for &top in TOP_SIZES.iter() {
                match fetch_collection(&client, &token, &market, collection, top).await {
                    Ok(resp) => {
                        tops.insert(top, extract_product_ids(&resp));
                    }
                    Err(err) => {
                        eprintln!("⚠️  Collection {} top {} failed for {}: {}", collection, top, market, err);
                    }
                }
            }
            collection_results.push(CollectionResult {
                name: collection.to_string(),
                tops,
            });
        }

        market_results.push(MarketResult {
            market,
            categories: categories_out,
            collections: collection_results,
        });
    }

    let generated_at_epoch = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    let output = Output {
        generated_at_epoch,
        markets: market_results,
    };

    let filename = format!("genre_toplists_{}.json", generated_at_epoch);
    let json = serde_json::to_string_pretty(&output)?;
    std::fs::write(&filename, &json)?;

    println!("\n💾 Saved results to {}", filename);
    println!("   Contains: category ids, optional counts, and top {:?} product ids per category/collection.", TOP_SIZES);

    Ok(())
}
