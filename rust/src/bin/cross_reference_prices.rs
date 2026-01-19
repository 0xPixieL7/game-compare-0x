use anyhow::Context;
use i_miss_rust::database_ops::db::Db;
use i_miss_rust::util::env::{bootstrap_cli, db_url_prefer_session};
use serde_json::Value;
use sqlx::{Postgres, Row};
use tracing::{info, warn};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    bootstrap_cli("cross_reference_prices");

    let database_url = db_url_prefer_session().context("no database URL env vars set")?;
    let db = Db::connect(&database_url, 10).await?;

    info!("Starting price cross-referencing scan...");

    // 1. Check for orphaned prices
    let orphaned_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM video_game_prices WHERE video_game_id IS NULL")
            .fetch_one(&db.pool)
            .await?;

    info!(
        "Found {} orphaned prices (video_game_id IS NULL)",
        orphaned_count
    );

    if orphaned_count == 0 {
        info!("No orphans to process. Exiting.");
        return Ok(());
    }

    // 2. Process in batches
    let batch_size = 1000;
    let mut offset = 0;

    loop {
        let prices = sqlx::query(
            "SELECT id, metadata, retailer, sku 
             FROM video_game_prices 
             WHERE video_game_id IS NULL 
             LIMIT $1 OFFSET $2",
        )
        .bind(batch_size)
        .bind(offset)
        .fetch_all(&db.pool)
        .await?;

        if prices.is_empty() {
            break;
        }

        info!(
            "Processing batch of {} orphans (offset {})",
            prices.len(),
            offset
        );

        for row in prices {
            let price_id: i64 = row.get("id");
            let metadata: Option<Value> = row.get("metadata");
            let retailer: Option<String> = row.get("retailer");
            let sku: Option<String> = row.get("sku");

            // Attempt to resolve video_game_id
            if let Some(game_id) = resolve_game_id(&db, &metadata, &retailer, &sku).await? {
                info!("MATCH FOUND! Price {} -> Game {}", price_id, game_id);

                // Update
                sqlx::query("UPDATE video_game_prices SET video_game_id = $1 WHERE id = $2")
                    .bind(game_id)
                    .bind(price_id)
                    .execute(&db.pool)
                    .await?;
            } else {
                // debug!("Could not resolve price {}", price_id);
            }
        }

        offset += batch_size;
    }

    info!("Cross-referencing complete.");
    Ok(())
}

async fn resolve_game_id(
    db: &Db,
    metadata: &Option<Value>,
    retailer: &Option<String>,
    sku: &Option<String>,
) -> anyhow::Result<Option<i64>> {
    // Strategy 1: Check metadata for provider/external_id
    if let Some(meta) = metadata {
        if let Some(source) = meta.get("source") {
            let provider = source.get("provider").and_then(|v| v.as_str());
            let ext_id = source.get("external_id").and_then(|v| v.as_str());

            if let (Some(p), Some(eid)) = (provider, ext_id) {
                let row = sqlx::query_scalar::<_, i64>(
                    "SELECT id FROM video_games WHERE provider = $1 AND external_id = $2",
                )
                .bind(p)
                .bind(eid)
                .fetch_optional(&db.pool)
                .await?;

                if let Some(gid) = row {
                    return Ok(Some(gid));
                }
            }

            // Strategy 3: Name based matching (if provider ID failing)
            if let Some(name) = source.get("name").and_then(|v| v.as_str()) {
                let slug = slugify(name);

                // Try finding by normalized title or slug
                // Note: we want the VIDEO GAME ID, not the title ID.
                let row = sqlx::query_scalar::<_, i64>(
                    "SELECT vg.id 
                     FROM video_games vg
                     JOIN video_game_titles vgt ON vgt.id = vg.title_id
                     WHERE vgt.slug = $1 OR vgt.normalized_title = $1 OR vgt.title = $2
                     LIMIT 1",
                )
                .bind(&slug)
                .bind(name)
                .fetch_optional(&db.pool)
                .await?;

                if let Some(gid) = row {
                    return Ok(Some(gid));
                }
            }
        }
    }

    // Strategy 2: SKU matching (if SKU is known to be global or reliable)
    // Placeholder logic
    if let Some(s) = sku {
        // e.g. querying other prices with the same SKU that HAVE a game_id
        let row = sqlx::query_scalar::<_, i64>(
            "SELECT video_game_id FROM video_game_prices WHERE sku = $1 AND video_game_id IS NOT NULL LIMIT 1"
        )
        .bind(s)
        .fetch_optional(&db.pool)
        .await?;

        if let Some(gid) = row {
            return Ok(Some(gid));
        }
    }

    Ok(None)
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
