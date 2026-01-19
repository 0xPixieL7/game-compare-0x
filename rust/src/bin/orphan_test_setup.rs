use i_miss_rust::database_ops::db::Db;
use i_miss_rust::util::env::bootstrap_cli;
use sqlx::Row;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    bootstrap_cli("orphan_test_setup");
    let url = std::env::var("DATABASE_URL").or_else(|_| std::env::var("SUPABASE_DB_URL"))?;
    let db = Db::connect(&url, 5).await?;

    // Find a price that we know comes from our ingest (has metadata)
    let row = sqlx::query(
        "SELECT id, metadata FROM video_game_prices 
         WHERE video_game_id IS NOT NULL 
         AND metadata IS NOT NULL
         LIMIT 1",
    )
    .fetch_optional(&db.pool)
    .await?;

    if let Some(r) = row {
        let id: i64 = r.get("id");
        println!("Orphaning price id {} (metadata present)", id);
        sqlx::query("UPDATE video_game_prices SET video_game_id = NULL WHERE id = $1")
            .bind(id)
            .execute(&db.pool)
            .await?;
        println!("Success. Run cross_reference_prices to fix.");
    } else {
        println!("No prices with metadata found to orphan. Ingest possibly pending or failed.");
    }
    Ok(())
}
