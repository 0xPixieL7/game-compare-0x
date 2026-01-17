/*!
 * Standalone Azure Token Tester
 * Tests Azure/Entra ID OAuth authentication without library dependencies
 */

use anyhow::{Context, Result};
use serde::Deserialize;
use std::env;

#[derive(Debug, Deserialize)]
struct TokenResponse {
    access_token: String,
    expires_in: i64,
    token_type: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();

    println!("\n🔐 Azure/Entra ID OAuth Token Tester");
    println!("═══════════════════════════════════════════════════════════\n");

    let tenant_id = env::var("AZURE_TENANT_ID")
        .context("AZURE_TENANT_ID not set")?;
    let client_id = env::var("AZURE_CLIENT_ID")
        .context("AZURE_CLIENT_ID not set")?;
    let client_secret = env::var("AZURE_CLIENT_SECRET")
        .context("AZURE_CLIENT_SECRET not set")?;
    let scope = env::var("AZURE_SCOPE")
        .unwrap_or_else(|_| "https://onestore.microsoft.com/.default".to_string());

    println!("📋 Configuration:");
    println!("  Tenant ID: {}", tenant_id);
    println!("  Client ID: {}", client_id);
    println!("  Scope: {}\n", scope);

    println!("🔄 Requesting token...");

    let url = format!(
        "https://login.microsoftonline.com/{}/oauth2/v2.0/token",
        tenant_id
    );

    let params = [
        ("client_id", client_id.as_str()),
        ("client_secret", client_secret.as_str()),
        ("grant_type", "client_credentials"),
        ("scope", scope.as_str()),
    ];

    let client = reqwest::Client::new();
    let response = client
        .post(&url)
        .form(&params)
        .send()
        .await
        .context("Failed to send token request")?;

    if !response.status().is_success() {
        let status = response.status();
        let error_text = response.text().await.unwrap_or_default();
        anyhow::bail!("Token request failed: {} - {}", status, error_text);
    }

    let token_data = response
        .json::<TokenResponse>()
        .await
        .context("Failed to parse token response")?;

    println!("\n✅ SUCCESS!");
    println!("═══════════════════════════════════════════════════════════");
    println!("Token Type: {}", token_data.token_type);
    println!("Expires In: {} seconds ({} minutes)",
        token_data.expires_in,
        token_data.expires_in / 60
    );
    println!("\nToken Preview (first 80 chars):");
    println!("{}", &token_data.access_token[..80.min(token_data.access_token.len())]);
    println!("...\n");

    println!("Full token length: {} characters", token_data.access_token.len());
    println!("═══════════════════════════════════════════════════════════\n");

    Ok(())
}
