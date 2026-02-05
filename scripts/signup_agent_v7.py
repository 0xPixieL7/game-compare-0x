import sys
import os
import logging
import time
import re
from playwright.sync_api import sync_playwright
from playwright_stealth import Stealth

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

try:
    from hustle.engine.captcha_solver import CaptchaSolver
except ImportError:
    CaptchaSolver = None

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

class IntelligentSignupAgent:
    def __init__(self, headless=False):
        self.headless = headless
        self.solver = CaptchaSolver() if CaptchaSolver else None

    def attempt_signup(self, url, email, password, username=None):
        if not username:
            username = email.split('@')[0]
        username = username.replace("@", "")
        
        logging.info(f"🚀 Starting Modal-Aware Signup | Target: {url} | User: {username}")

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=self.headless, args=["--start-maximized"])
            context = browser.new_context(viewport=None, locale="en-US")
            page = context.new_page()
            Stealth().apply_stealth_sync(page)

            try:
                page.goto(url)
                page.wait_for_load_state("domcontentloaded")
                time.sleep(2)

                # --- STEP 1: CLICK SIGN IN (Primary) ---
                logging.info("🔎 Finding Sign In button (Primary Path)...")
                
                # User instruction: Click "Sign In" -> Modal -> Register
                # We prioritize "Log In" or "Sign In" to avoid the "Join Now" tour modal
                signin_candidates = [
                    page.get_by_text("Sign In", exact=True),
                    page.get_by_text("Log In", exact=True),
                    page.get_by_role("link", name="Sign In"),
                    page.get_by_role("link", name="Log In")
                ]
                
                signin_btn = None
                for btn in signin_candidates:
                    if btn.count() > 0 and btn.first.is_visible():
                        signin_btn = btn.first
                        break
                
                if signin_btn:
                    logging.info(f"   👉 Clicking: {signin_btn.inner_text()}")
                    signin_btn.click()
                    time.sleep(2) # Wait for modal animation
                else:
                    logging.warning("⚠️ Could not find explicit 'Sign In'. Trying 'Join' as fallback...")
                    # Fallback
                    join_btn = page.get_by_text(re.compile(r"join|register", re.I)).first
                    if join_btn.is_visible():
                        join_btn.click()
                        time.sleep(2)

                # --- STEP 2: HANDLE LOGIN MODAL -> SWITCH TO REGISTER ---
                logging.info("👀 Checking for Login Modal...")
                
                # Wait for modal container
                modal = page.locator(".reveal, .modal, [role='dialog']").first
                if modal.is_visible():
                    logging.info("   ✓ Modal appeared.")
                    
                    # Look for "Register" or "Create Account" switch INSIDE the modal
                    reg_switch = modal.get_by_text(re.compile(r"register|create account|sign up", re.I)).first
                    if reg_switch.is_visible():
                        logging.info("   👉 Switching to Register view...")
                        reg_switch.click()
                        time.sleep(1)
                    else:
                        logging.warning("   ⚠️ No 'Register' switch found in modal. Are we already on register?")
                else:
                    logging.warning("   ⚠️ No modal appeared. Maybe we navigated?")

                # --- STEP 3: FILL FORM (Targeting Modal Scope) ---
                logging.info("📝 Filling Form...")
                
                # Scope finding to the modal if it exists, otherwise page
                container = modal if modal.is_visible() else page
                
                # Email
                email_input = container.locator('input[type="email"]').first
                if email_input.is_visible():
                    email_input.fill(email)
                    logging.info("   ✓ Email filled")
                else:
                    logging.error("❌ Email input not found in modal.")
                    return

                # Username
                user_input = container.locator('input[name*="user"], input[name*="login"]').first
                if user_input.is_visible():
                    user_input.fill(username)
                    logging.info("   ✓ Username filled")

                # Password
                pass_input = container.locator('input[type="password"]').first
                if pass_input.is_visible():
                    pass_input.fill(password)
                    logging.info("   ✓ Password filled")
                
                # Repeat Password
                all_pass = container.locator('input[type="password"]')
                if all_pass.count() > 1:
                    all_pass.nth(1).fill(password)

                # --- STEP 4: CAPTCHA ---
                if self.solver:
                    logging.info("🧩 Checking CAPTCHA...")
                    self.solver.solve_recaptcha(page)

                # --- STEP 5: SUBMIT ---
                submit = container.locator('button[type="submit"]').first
                if submit.is_visible():
                    logging.info("🚀 Submitting...")
                    submit.click()
                
                time.sleep(10)

            except Exception as e:
                logging.error(f"❌ Error: {e}")
            finally:
                time.sleep(2)
                browser.close()

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python3 signup_agent_v7.py <url> <email> <password> [username]")
        sys.exit(1)
    
    agent = IntelligentSignupAgent(headless=False)
    agent.attempt_signup(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4] if len(sys.argv)>4 else None)