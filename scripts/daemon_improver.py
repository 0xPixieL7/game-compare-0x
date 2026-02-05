import subprocess
import time
import sys
import logging
import os

# Configuration
TARGET_SCRIPT = "scripts/signup_agent_v7.py"
MAX_RETRIES = 5
logging.basicConfig(level=logging.INFO, format='%(asctime)s - [LOGIC-DAEMON] - %(message)s')

def run_agent():
    """Runs the target signup agent and captures output."""
    logging.info(f"🚀 Executing {TARGET_SCRIPT} (FULL VIEWPORT)...")
    try:
        # Run using the venv python
        result = subprocess.run(
            ["source hustle/.venv/bin/activate && python3 " + TARGET_SCRIPT + " 'https://odealo.com' 'zeroex.hustle@proton.me' 'SecurePass123!' 'ZeroExHustler'"],
            shell=True,
            capture_output=True,
            text=True
        )
        return result.returncode, result.stdout, result.stderr
    except Exception as e:
        return -1, "", str(e)

def trigger_openclaw_fix(stdout, stderr):
    """Demands a structured reasoning-first fix from OpenClaw."""
    logging.info("🧠 Demanding Advanced Reasoning and Logic Review...")
    
    # We combine stdout and stderr for the full picture
    full_logs = f"--- STDOUT ---\n{stdout}\n\n--- STDERR ---\n{stderr}"
    
    prompt = (
        f"CRITICAL: The script `{TARGET_SCRIPT}` failed. You must fix it using a LOGICAL REASONING workflow.\n\n"
        f"**LOGS TO REVIEW:**\n{full_logs[-3000:]}\n\n"
        f"**MANDATORY STEPS:**\n"
        f"1. **STEP 1: REASONING.** Call `@advanced-reasoning` to ingest these logs and the current content of `{TARGET_SCRIPT}`. Identify exactly why the interaction failed (e.g. was the button hidden? Was it a different modal? Was the viewport wrong?).\n"
        f"2. **STEP 2: LOGIC.** Use `@mcp-logic` to deduce the best strategy to bypass the observed failure. Check if the 'No thanks' button is truly visible or if it's an overlay issue.\n"
        f"3. **STEP 3: ENFORCE STANDARDS.** Ensure the fixed code uses `viewport=None` for a guaranteed full-screen experience on Mac.\n"
        f"4. **STEP 4: FIX.** Rewrite `{TARGET_SCRIPT}` with the improved logic. Do NOT provide commentary, just overwrite the file with the solution."
    )
    
    try:
        cmd = [
            "openclaw", "agent",
            "--message", prompt,
            "--session-id", "logic-repair-engine",
            "--timeout", "900" # 15 mins for deep thinking
        ]
        subprocess.run(cmd, check=True)
        return True
    except Exception as e:
        logging.error(f"Failed to trigger OpenClaw Fix: {e}")
        return False

def daemon_loop():
    for attempt in range(1, MAX_RETRIES + 1):
        logging.info(f"--- LOGIC ITERATION {attempt}/{MAX_RETRIES} ---")
        
        code, stdout, stderr = run_agent()
        
        # Check for success
        if "Signup attempt completed" in stdout or "Signup attempt completed" in stderr:
            # We also check if there were no errors in the completed run
            if "❌" not in stdout and "Error" not in stderr:
                logging.info("✅ SUCCESS! Logic Loop reached a valid state.")
                break
        
        logging.warning("🛑 Failure in execution. Starting Review Phase.")
        
        if attempt < MAX_RETRIES:
            if trigger_openclaw_fix(stdout, stderr):
                logging.info("⏳ Waiting 15s for the Agent to complete its reasoning and write the fix...")
                time.sleep(15)
            else:
                break
        else:
            logging.error("💀 Maximum reasoning iterations reached. Manual intervention required.")

if __name__ == "__main__":
    daemon_loop()
