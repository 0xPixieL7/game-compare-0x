use anyhow::{Context, Result};
use chrono::Utc;
use rayon::prelude::*;
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::fs::File;
use std::io::BufReader;
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

#[derive(Debug, Deserialize)]
struct GbGame {
    guid: String,
    name: String,
    description: Option<String>,
    deck: Option<String>,
    platforms: Option<Vec<GbPlatform>>,
}

#[derive(Debug, Deserialize)]
struct GbPlatform {
    name: String,
}

#[derive(Debug, Deserialize)]
struct NexardaGame {
    id: i64,
    name: String,
    slug: String,
    prices: HashMap<String, Value>,
}

#[derive(Debug)]
struct BatchProduct {
    name: String,
    slug: String,
    synopsis: Option<String>,
    external_ids: Value,
    metadata: Value,
}

#[derive(Debug)]
struct BatchTitle {
    product_slug: String,
    name: String,
    slug: String,
    providers: Value,
}

#[derive(Debug)]
struct BatchGame {
    title_slug: String,
    slug: String,
    provider: String,
    external_id: String,
    name: String,
    description: Option<String>,
    summary: Option<String>,
    media: Value,
    source_payload: Value,
    hypes: i32,
    follows: i32,
    popularity_score: f64,
}

#[derive(Debug)]
struct BatchPrice {
    game_provider: String,
    game_external_id: String,
    amount_minor: i64,
    currency: String,
    retailer: String,
    country_code: Option<String>,
    condition: Option<String>,
    sku: Option<String>,
    game_name: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Load .env first so RUST_LOG is picked up
    i_miss_rust::util::env::init_env();

    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();

    i_miss_rust::util::env::bootstrap_cli("ingest_v2");

    let db_url = i_miss_rust::util::env::db_url_prefer_session()
        .context("Database URL must be set in env")?;
    let db = i_miss_rust::database_ops::db::Db::connect(&db_url, 20).await?;

    info!("🚀 Starting Ingest V2 (Optimized UNNEST Edition)...");

    // 1. Giant Bomb
    if let Ok(file) = File::open("giant_bomb_games_detailed.json") {
        info!("📖 [Giant Bomb] Processing JSON Map...");
        let val: Value = serde_json::from_reader(BufReader::new(file))?;
        info!("📖 [Giant Bomb] JSON parsed successfully. Mapping items...");
        if let Some(map) = val.as_object() {
            let games: Vec<GbGame> = map
                .values()
                .par_bridge()
                .filter_map(|v| {
                    serde_json::from_value(v.clone())
                        .map_err(|e| {
                            warn!("Malformed GB line: {}", e);
                            e
                        })
                        .ok()
                })
                .collect();

            info!("📦 [Giant Bomb] Batching {} games...", games.len());
            let mut products = Vec::new();
            let mut titles = Vec::new();
            let mut games_batch = Vec::new();

            for game in games {
                let slug = slugify(&game.name);
                products.push(BatchProduct {
                    name: game.name.clone(),
                    slug: slug.clone(),
                    synopsis: game.description.clone().or(game.deck.clone()),
                    external_ids: json!({"giantbomb": game.guid}),
                    metadata: json!({
                        "gb_deck": game.deck,
                        "gb_platforms": game.platforms.as_ref().map(|p| p.iter().map(|pl| &pl.name).collect::<Vec<_>>())
                    }),
                });
                titles.push(BatchTitle {
                    product_slug: slug.clone(),
                    name: game.name.clone(),
                    slug: slug.clone(),
                    providers: json!(["giantbomb"]),
                });
                games_batch.push(BatchGame {
                    title_slug: slug.clone(),
                    slug: slug.clone(),
                    provider: "giantbomb".to_string(),
                    external_id: game.guid.clone(),
                    name: game.name.clone(),
                    description: game.description.clone().or(game.deck.clone()),
                    summary: game.deck.clone(),
                    media: json!({}), // Giant Bomb images could be added here
                    source_payload: json!({}), // Giant Bomb raw could be added here
                    hypes: 0,
                    follows: 0,
                    popularity_score: 0.0,
                });
            }
            info!("🔄 [Giant Bomb] Flushing batches to database...");
            flush_batches(&db, &products, &titles, &games_batch, &[]).await?;
            info!("✅ [Giant Bomb] Processing complete.");
        }
    } else {
        warn!("⚠️ [Giant Bomb] 'giant_bomb_games_detailed.json' not found. Skipping.");
    }

    // 2. Nexarda
    if let Ok(file) = File::open("nexarda_product_catalogue.json") {
        info!("📖 [Nexarda] Processing JSON...");
        let val: Value = serde_json::from_reader(BufReader::new(file))?;
        info!("📖 [Nexarda] JSON parsed successfully. Mapping items...");
        if let Some(games_val) = val.get("games").and_then(|v| v.as_array()) {
            let games: Vec<NexardaGame> = games_val
                .par_iter()
                .filter_map(|v| {
                    serde_json::from_value(v.clone())
                        .map_err(|e| {
                            warn!("Malformed Nexarda line: {}", e);
                            e
                        })
                        .ok()
                })
                .collect();

            info!("📦 [Nexarda] Batching {} games...", games.len());
            let mut products = Vec::new();
            let mut titles = Vec::new();
            let mut games_batch = Vec::new();
            let mut prices = Vec::new();

            for game in games {
                let slug = game.slug.trim_start_matches("/games/").to_string();
                products.push(BatchProduct {
                    name: game.name.clone(),
                    slug: slug.clone(),
                    synopsis: None,
                    external_ids: json!({"nexarda": game.id}),
                    metadata: json!({}),
                });
                titles.push(BatchTitle {
                    product_slug: slug.clone(),
                    name: game.name.clone(),
                    slug: slug.clone(),
                    providers: json!(["nexarda"]),
                });
                games_batch.push(BatchGame {
                    title_slug: slug.clone(),
                    slug: slug.clone(),
                    provider: "nexarda".to_string(),
                    external_id: game.id.to_string(),
                    name: game.name.clone(),
                    description: None,
                    summary: None,
                    media: json!({}),
                    source_payload: json!({}),
                    hypes: 0,
                    follows: 0,
                    popularity_score: 0.0,
                });

                for (currency, price_val) in game.prices {
                    if let Some(price_f) = extract_price_f64(&price_val) {
                        prices.push(BatchPrice {
                            game_provider: "nexarda".to_string(),
                            game_external_id: game.id.to_string(),
                            amount_minor: (price_f * 100.0).round() as i64,
                            currency: currency.to_uppercase(),
                            retailer: "Nexarda".to_string(),
                            country_code: None, // Global/Digital
                            condition: Some("digital".to_string()),
                            sku: None,
                            game_name: Some(game.name.clone()),
                        });
                    }
                }
            }
            info!("🔄 [Nexarda] Flushing batches to database...");
            flush_batches(&db, &products, &titles, &games_batch, &prices).await?;
            info!("✅ [Nexarda] Processing complete.");
        }
    } else {
        warn!("⚠️ [Nexarda] 'nexarda_product_catalogue.json' not found. Skipping.");
    }

    // 3. Price Guide
    if let Ok(file) = File::open("price-guide.csv") {
        info!("📖 [Price Guide] Processing CSV...");
        let mut rdr = csv::Reader::from_reader(file);
        let records: Vec<csv::StringRecord> = rdr
            .records()
            .filter_map(|r| r.map_err(|e| warn!("Malformed CSV line: {}", e)).ok())
            .collect();
        info!(
            "📖 [Price Guide] CSV parsed successfully. Processed {} records.",
            records.len()
        );

        let mut products = Vec::new();
        let mut titles = Vec::new();
        let mut games_batch = Vec::new();
        let mut prices = Vec::new();

        for record in records {
            let ext_id = record.get(0).unwrap_or("").to_string();
            let name = record.get(2).unwrap_or("");
            if name.is_empty() {
                continue;
            }
            let slug = slugify(name);
            let price_str = record
                .get(3)
                .unwrap_or("")
                .replace('$', "")
                .replace(',', "");
            let price_f = price_str.parse::<f64>().unwrap_or(0.0);

            products.push(BatchProduct {
                name: name.to_string(),
                slug: slug.clone(),
                synopsis: None,
                external_ids: json!({}),
                metadata: json!({}),
            });
            titles.push(BatchTitle {
                product_slug: slug.clone(),
                name: name.to_string(),
                slug: slug.clone(),
                providers: json!(["pricecharting"]),
            });
            games_batch.push(BatchGame {
                title_slug: slug.clone(),
                slug: slug.clone(),
                provider: "pricecharting".to_string(),
                external_id: ext_id.clone(),
                name: name.to_string(),
                description: None,
                summary: None,
                media: json!({}),
                source_payload: json!({}),
                hypes: 0,
                follows: 0,
                popularity_score: 0.0,
            });
            prices.push(BatchPrice {
                game_provider: "pricecharting".to_string(),
                game_external_id: ext_id,
                amount_minor: (price_f * 100.0).round() as i64,
                currency: "USD".to_string(),
                retailer: "PriceCharting".to_string(),
                country_code: Some("US".to_string()), // Assuming USD prices are US
                condition: Some("loose".to_string()), // Assuming column 3 is loose
                sku: None,
                game_name: Some(name.to_string()),
            });
        }
        info!("🔄 [Price Guide] Flushing batches to database...");
        flush_batches(&db, &products, &titles, &games_batch, &prices).await?;
        info!("✅ [Price Guide] Processing complete.");
    } else {
        warn!("⚠️ [Price Guide] 'price-guide.csv' not found. Skipping.");
    }

    info!("✅ Ingest V2 Complete!");
    Ok(())
}

async fn flush_batches(
    db: &i_miss_rust::database_ops::db::Db,
    products: &[BatchProduct],
    titles: &[BatchTitle],
    games: &[BatchGame],
    prices: &[BatchPrice],
) -> Result<()> {
    let now = Utc::now().naive_utc();
    info!(
        "🔄 Flushing batches: Products={}, Titles={}, Games={}, Prices={}",
        products.len(),
        titles.len(),
        games.len(),
        prices.len()
    );

    // 1. Products (Deduplicate by slug)
    // Keep the LAST occurrence if duplicates exist to reflect latest state
    let mut unique_products_map = HashMap::new();
    for p in products {
        unique_products_map.insert(p.slug.clone(), p);
    }
    let unique_products: Vec<_> = unique_products_map.into_values().collect();
    // Sort or keep order? HashMap shuffles order. Batch processing doesn't strictly need order, but deterministic behavior is nice.
    // We'll proceed with arbitrary order from HashMap.

    let product_chunks = unique_products.chunks(100);
    let product_chunks_len = product_chunks.len();
    info!(
        "📦 Flushing {} product chunks ({} unique items)...",
        product_chunks_len,
        unique_products.len()
    );
    for (i, chunk) in product_chunks.enumerate() {
        let names: Vec<_> = chunk.iter().map(|p| &p.name).collect();
        let slugs: Vec<_> = chunk.iter().map(|p| &p.slug).collect();
        let synopses: Vec<_> = chunk.iter().map(|p| &p.synopsis).collect();
        let ext_ids: Vec<_> = chunk.iter().map(|p| &p.external_ids).collect();
        let metadata: Vec<_> = chunk.iter().map(|p| &p.metadata).collect();

        match sqlx::query(
            r#"
            INSERT INTO products (type, name, slug, title, normalized_title, synopsis, category, external_ids, metadata, created_at, updated_at)
            SELECT 'video_game', name, slug, name, slug, synopsis, 'GAME', ext_id, meta, $6, $6
            FROM UNNEST($1::text[], $2::text[], $3::text[], $4::jsonb[], $5::jsonb[]) 
            AS t(name, slug, synopsis, ext_id, meta)
            ON CONFLICT (slug) DO UPDATE SET 
                external_ids = products.external_ids || EXCLUDED.external_ids,
                metadata = products.metadata || EXCLUDED.metadata,
                synopsis = COALESCE(products.synopsis, EXCLUDED.synopsis),
                updated_at = EXCLUDED.updated_at
            "#
        )
        .bind(&names as &[&String])
        .bind(&slugs as &[&String])
        .bind(&synopses as &[&Option<String>])
        .bind(&ext_ids as &[&Value])
        .bind(&metadata as &[&Value])
        .bind(now)
        .execute(&db.pool).await {
            Ok(_) => tracing::debug!("✅ Products chunk {}/{} flushed", i + 1, product_chunks_len),
            Err(e) => {
                tracing::error!("❌ Products chunk {} failed: {}", i + 1, e);
                return Err(e.into());
            }
        }
    }

    // 2. Titles (Deduplicate by slug)
    let mut unique_titles_map = HashMap::new();
    for t in titles {
        unique_titles_map.insert(t.slug.clone(), t);
    }
    let unique_titles: Vec<_> = unique_titles_map.into_values().collect();

    let title_chunks = unique_titles.chunks(100);
    let title_chunks_len = title_chunks.len();
    info!(
        "📦 Flushing {} title chunks ({} unique items)...",
        title_chunks_len,
        unique_titles.len()
    );
    for (i, chunk) in title_chunks.enumerate() {
        let p_slugs: Vec<_> = chunk.iter().map(|t| &t.product_slug).collect();
        let names: Vec<_> = chunk.iter().map(|t| &t.name).collect();
        let slugs: Vec<_> = chunk.iter().map(|t| &t.slug).collect();
        let providers: Vec<_> = chunk.iter().map(|t| &t.providers).collect();

        match sqlx::query(
            r#"
            INSERT INTO video_game_titles (product_id, name, normalized_title, slug, providers, created_at, updated_at)
            SELECT p.id, t.name, t.slug, t.slug, t.providers, $5, $5
            FROM UNNEST($1::text[], $2::text[], $3::text[], $4::json[]) AS t(p_slug, name, slug, providers)
            JOIN products p ON p.slug = t.p_slug
            ON CONFLICT (slug) DO UPDATE SET 
                providers = (video_game_titles.providers::jsonb || EXCLUDED.providers::jsonb)::json,
                updated_at = EXCLUDED.updated_at
            "#
        )
        .bind(&p_slugs as &[&String])
        .bind(&names as &[&String])
        .bind(&slugs as &[&String])
        .bind(&providers as &[&Value])
        .bind(now)
        .execute(&db.pool).await {
            Ok(_) => tracing::debug!("✅ Titles chunk {}/{} flushed", i + 1, title_chunks_len),
            Err(e) => {
                tracing::error!("❌ Titles chunk {} failed: {}", i + 1, e);
                return Err(e.into());
            }
        }
    }

    // 3. Games (Deduplicate by provider + external_id)
    let mut unique_games_map = HashMap::new();
    for g in games {
        let key = (g.provider.clone(), g.external_id.clone());
        unique_games_map.insert(key, g);
    }
    let unique_games: Vec<_> = unique_games_map.into_values().collect();

    let game_chunks = unique_games.chunks(100);
    let game_chunks_len = game_chunks.len();
    info!(
        "📦 Flushing {} game chunks ({} unique items)...",
        game_chunks_len,
        unique_games.len()
    );
    for (i, chunk) in game_chunks.enumerate() {
        let t_slugs: Vec<_> = chunk.iter().map(|g| &g.title_slug).collect();
        let slugs: Vec<_> = chunk.iter().map(|g| &g.slug).collect();
        let providers: Vec<_> = chunk.iter().map(|g| &g.provider).collect();
        let ext_ids: Vec<_> = chunk.iter().map(|g| &g.external_id).collect();
        let names: Vec<_> = chunk.iter().map(|g| &g.name).collect();
        let descs: Vec<_> = chunk.iter().map(|g| &g.description).collect();

        let summaries: Vec<_> = chunk.iter().map(|g| &g.summary).collect();
        let media: Vec<_> = chunk.iter().map(|g| &g.media).collect();
        let simple_payloads: Vec<_> = chunk.iter().map(|g| &g.source_payload).collect();
        let hypes: Vec<_> = chunk.iter().map(|g| g.hypes).collect();
        let follows: Vec<_> = chunk.iter().map(|g| g.follows).collect();
        let pop_scores: Vec<_> = chunk.iter().map(|g| g.popularity_score).collect();

        match sqlx::query(
            r#"
            INSERT INTO video_games (
                video_game_title_id, slug, provider, external_id, name, description, 
                summary, media, source_payload, hypes, follows, popularity_score,
                created_at, updated_at
            )
            SELECT 
                vgt.id, t.slug, t.provider, t.ext_id, t.name, t.description,
                t.summary, t.media, t.source_payload, t.hypes, t.follows, t.pop_score,
                $13, $13
            FROM UNNEST(
                $1::text[], $2::text[], $3::text[], $4::text[], $5::text[], $6::text[],
                $7::text[], $8::jsonb[], $9::jsonb[], $10::int[], $11::int[], $12::float8[]
            ) 
            AS t(t_slug, slug, provider, ext_id, name, description, summary, media, source_payload, hypes, follows, pop_score)
            JOIN video_game_titles vgt ON vgt.slug = t.t_slug
            ON CONFLICT (provider, external_id) DO UPDATE SET 
                name = EXCLUDED.name,
                description = COALESCE(video_games.description, EXCLUDED.description),
                summary = COALESCE(video_games.summary, EXCLUDED.summary),
                media = EXCLUDED.media,
                source_payload = EXCLUDED.source_payload,
                hypes = EXCLUDED.hypes,
                follows = EXCLUDED.follows,
                popularity_score = EXCLUDED.popularity_score,
                updated_at = EXCLUDED.updated_at
            "#
        )
        .bind(&t_slugs as &[&String])
        .bind(&slugs as &[&String])
        .bind(&providers as &[&String])
        .bind(&ext_ids as &[&String])
        .bind(&names as &[&String])
        .bind(&descs as &[&Option<String>])
        .bind(&summaries as &[&Option<String>])
        .bind(&media as &[&Value])
        .bind(&simple_payloads as &[&Value])
        .bind(&hypes as &[i32])
        .bind(&follows as &[i32])
        .bind(&pop_scores as &[f64])
        .bind(now)
        .execute(&db.pool).await {
            Ok(_) => tracing::debug!("✅ Games chunk {}/{} flushed", i + 1, game_chunks_len),
            Err(e) => {
                tracing::error!("❌ Games chunk {} failed: {}", i + 1, e);
                return Err(e.into());
            }
        }
    }

    // 4. Prices (Deduplicate by unique constraint)
    let mut unique_prices_map = HashMap::new();
    for p in prices {
        let key = (
            &p.game_provider,
            &p.game_external_id,
            &p.retailer,
            &p.currency,
            &p.country_code,
            &p.condition,
            &p.sku,
        );
        unique_prices_map.insert(key, p);
    }
    let unique_prices: Vec<_> = unique_prices_map.into_values().collect();

    let price_chunks = unique_prices.chunks(100);
    let price_chunks_len = price_chunks.len();

    info!(
        "📦 Flushing {} price chunks ({} unique items)...",
        price_chunks_len,
        unique_prices.len()
    );

    for (i, chunk) in price_chunks.enumerate() {
        let providers: Vec<_> = chunk.iter().map(|p| &p.game_provider).collect();
        let ext_ids: Vec<_> = chunk.iter().map(|p| &p.game_external_id).collect();
        let amounts: Vec<_> = chunk.iter().map(|p| p.amount_minor).collect();
        let currencies: Vec<_> = chunk.iter().map(|p| &p.currency).collect();
        let retailers: Vec<_> = chunk.iter().map(|p| &p.retailer).collect();
        let country_codes: Vec<_> = chunk.iter().map(|p| &p.country_code).collect();
        let conditions: Vec<_> = chunk.iter().map(|p| &p.condition).collect();
        let skus: Vec<_> = chunk.iter().map(|p| &p.sku).collect();

        // Create metadata with source info for cross-referencing orphans later
        let metadata_vec: Vec<_> = chunk
            .iter()
            .map(|p| {
                json!({
                    "source": {
                        "provider": p.game_provider,
                        "external_id": p.game_external_id,
                        "name": p.game_name
                    }
                })
            })
            .collect();

        match sqlx::query(
            r#"
            INSERT INTO video_game_prices (video_game_id, amount_minor, currency, retailer, country_code, condition, sku, recorded_at, created_at, updated_at, bucket, tax_inclusive, is_active, is_retail_buy, metadata)
            SELECT vg.id, t.amount, t.currency, t.retailer, t.country_code, t.condition, t.sku, $9, $9, $9, 'live', true, true, true, t.metadata
            FROM UNNEST($1::text[], $2::text[], $3::bigint[], $4::text[], $5::text[], $6::text[], $7::text[], $8::text[], $10::jsonb[]) 
            AS t(provider, ext_id, amount, currency, retailer, country_code, condition, sku, metadata)
            LEFT JOIN video_games vg ON vg.provider = t.provider AND vg.external_id = t.ext_id
            ON CONFLICT (video_game_id, retailer, currency, country_code, condition, sku) DO UPDATE SET 
                amount_minor = EXCLUDED.amount_minor,
                updated_at = EXCLUDED.updated_at,
                metadata = video_game_prices.metadata || EXCLUDED.metadata
            "#
        )
        .bind(&providers as &[&String])
        .bind(&ext_ids as &[&String])
        .bind(&amounts as &[i64])
        .bind(&currencies as &[&String])
        .bind(&retailers as &[&String])
        .bind(&country_codes as &[&Option<String>])
        .bind(&conditions as &[&Option<String>])
        .bind(&skus as &[&Option<String>])
        .bind(now)
        .bind(&metadata_vec as &[Value])
        .execute(&db.pool).await {
             Ok(_) => tracing::debug!("✅ Prices chunk {}/{} flushed", i + 1, price_chunks_len),
             Err(e) => {
                 tracing::error!("❌ Prices chunk {} failed: {}", i + 1, e);
                 return Err(e.into());
             }
         }
    }

    Ok(())
}

fn extract_price_f64(v: &Value) -> Option<f64> {
    if let Some(f) = v.as_f64() {
        return Some(f);
    }
    if let Some(s) = v.as_str() {
        return s.replace(',', ".").parse::<f64>().ok();
    }
    None
}

fn slugify(name: &str) -> String {
    name.to_lowercase()
        .chars()
        .map(|c| if c.is_alphanumeric() { c } else { '-' })
        .collect::<String>()
        .split('-')
        .filter(|s| !s.is_empty())
        .collect::<Vec<_>>()
        .join("-")
}
