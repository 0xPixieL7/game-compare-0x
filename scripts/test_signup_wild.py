import sys
import subprocess
import datetime
import os

# Script Location: ./scripts/test_signup_wild.py

def run_wild_test():
    """
    Triggers OpenClaw to perform a LIVE TEST of the signup_agent on a real target.
    """
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] Starting LIVE WILD TEST of Signup Agent...")
    
    base_prompt = (
        "You are the Testing Engineer. We need to verify our new ZERO-COST CAPTCHA SOLVER in the wild.\n\n"
        
        "**OBJECTIVE: LIVE SIGNUP TEST**\n"
        "- Target: 'https://www.reddit.com/register/' (or a similar site with reCAPTCHA v2).\n"
        "- Action: Use `signup_agent.py` to attempt a registration flow.\n"
        "- Constraint: You DO NOT need to complete the email verification (we just want to pass the CAPTCHA).\n"
        "- Monitor: Watch the logs closely for 'CaptchaSolver' output.\n"
        "- Success Criteria: The solver detects the iframe, clicks the audio button, transcribes the audio using `whisper-cli`, and submits the text.\n\n"
        
        "**EXECUTION PLAN:**\n"
        "1. Use the REAL vault email: 'zeroex.hustle@proton.me' (Retrieved from .vault/secrets.json).\n"
        "2. Run the `signup_agent.py` script with this email and a secure random password.\n"
        "3. Report back the specific logs from the CAPTCHA solving step.\n"
    )
    
    try:
        # Trigger OpenClaw Agent
        cmd = [
            "openclaw", 
            "agent", 
            "--message", base_prompt, 
            "--session-id", "test-engineer",
            "--timeout", "300" # 5 minutes for a quick test
        ]
        
        print(f"[{timestamp}] Executing: ", ' '.join(cmd))
        subprocess.run(cmd, check=True)
        
        print(f"[{timestamp}] Test Cycle Complete.")
        
    except subprocess.CalledProcessError as e:
        print(f"[{timestamp}] Agent Execution Failed: {e}")
    except FileNotFoundError:
        print(f"[{timestamp}] Error: 'openclaw' command not found.")
    except Exception as e:
        print(f"[{timestamp}] Critical Error: {e}")

if __name__ == "__main__":
    run_wild_test()
