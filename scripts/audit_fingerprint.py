import sys
import os
import time
from playwright.sync_api import sync_playwright
from playwright_stealth import Stealth

def run_audit():
    with sync_playwright() as p:
        # Launch exactly as we do in the signup agent
        browser = p.chromium.launch(headless=False) 
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        )
        page = context.new_page()
        
        # Apply the exact same stealth logic
        Stealth().apply_stealth_sync(page)
        
        print("🕵️‍♀️ Navigating to Bot Detection Suite...")
        page.goto("https://bot.sannysoft.com/")
        
        # Wait for tests to run
        time.sleep(5)
        
        # Capture the evidence
        screenshot_path = "fingerprint_report.png"
        page.screenshot(path=screenshot_path, full_page=True)
        print(f"📸 Report saved to: {screenshot_path}")
        
        # Extract key metrics text for CLI output
        webdriver = page.evaluate("navigator.webdriver")
        print(f"🔍 navigator.webdriver: {webdriver}")
        
        # Keep open briefly for visual check
        time.sleep(5)
        browser.close()

if __name__ == "__main__":
    run_audit()
