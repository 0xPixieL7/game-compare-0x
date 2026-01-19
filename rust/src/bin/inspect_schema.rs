use i_miss_rust::database_ops::db::Db;
use i_miss_rust::util::env::db_url;
use sqlx::Row;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let database_url = db_url().expect("DB URL must be set");
    let db = Db::connect(&database_url, 1).await?;

    let rows = sqlx::query(
        "SELECT column_name, data_type, is_nullable 
         FROM information_schema.columns 
         WHERE table_name = 'video_game_prices'",
    )
    .fetch_all(&db.pool)
    .await?;

    println!("Columns in video_game_prices:");
    for row in rows {
        let name: String = row.get("column_name");
        let dtype: String = row.get("data_type");
        let nullable: String = row.get("is_nullable");
        println!("  - {} ({}, nullable: {})", name, dtype, nullable);
    }

    Ok(())
}
