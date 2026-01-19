use anyhow::Context;
use i_miss_rust::database_ops::db::Db;
use i_miss_rust::util::env::db_url;
use sqlx::Row;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let database_url = db_url().context("no database URL env vars set")?;
    let db = Db::connect(&database_url, 5).await?;

    // Check total prices
    let total_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM video_game_prices")
        .fetch_one(&db.pool)
        .await?;
    println!("Total video_game_prices: {}", total_count);

    // Check orphaned prices (video_game_id IS NULL)
    let orphaned_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM video_game_prices WHERE video_game_id IS NULL")
            .fetch_one(&db.pool)
            .await?;
    println!(
        "Orphaned prices (video_game_id IS NULL): {} ({:.2}%)",
        orphaned_count,
        if total_count > 0 {
            (orphaned_count as f64 / total_count as f64) * 100.0
        } else {
            0.0
        }
    );

    // Sample orphaned prices
    if orphaned_count > 0 {
        println!("\n=== Sample Orphaned Prices ===");
        let samples = sqlx::query(
            "SELECT id, retailer, country_code, currency, amount_minor, sku, url, metadata
             FROM video_game_prices
             WHERE video_game_id IS NULL
             LIMIT 10",
        )
        .fetch_all(&db.pool)
        .await?;

        for row in samples {
            let id: i64 = row.get("id");
            let retailer: Option<String> = row.get("retailer");
            let cc: Option<String> = row.get("country_code");
            let currency: Option<String> = row.get("currency");
            let amount: i64 = row.get("amount_minor");
            let sku: Option<String> = row.get("sku");
            let url: Option<String> = row.get("url");
            let metadata: Option<serde_json::Value> = row.get("metadata");

            println!(
                "  Price id={}: {} ({}) {} {} - SKU: {:?}, URL: {:?}",
                id,
                retailer.as_deref().unwrap_or("?"),
                cc.as_deref().unwrap_or("?"),
                amount,
                currency.as_deref().unwrap_or("?"),
                sku,
                url
            );
            if let Some(meta) = metadata {
                println!("    Metadata: {}", meta);
            }
        }
    }

    Ok(())
}
