from playwright.sync_api import sync_playwright
import sys

def run():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        
        print("Visiting http://127.0.0.1:8000/")
        page.on("console", lambda msg: print(f"CONSOLE: {msg.type}: {msg.text}"))
        page.on("pageerror", lambda exc: print(f"PAGE ERROR: {exc}"))
        
        try:
            page.goto('http://127.0.0.1:8000/', timeout=10000)
            page.wait_for_load_state('networkidle')
            print("Page title:", page.title())
            page.screenshot(path='welcome_page.png')
        except Exception as e:
            print(f"Failed to visit home: {e}")

        print("\nVisiting http://127.0.0.1:8000/games/117836")
        try:
            page.goto('http://127.0.0.1:8000/games/117836', timeout=10000)
            page.wait_for_load_state('networkidle')
            print("Page title:", page.title())
            page.screenshot(path='game_show_page.png')
        except Exception as e:
            print(f"Failed to visit show page: {e}")
            
        browser.close()

if __name__ == "__main__":
    run()
