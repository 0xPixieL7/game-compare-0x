import sys
import subprocess
import datetime
import os

# Script Location: ./scripts/social_growth_cron.py

def run_social_growth_cycle():
    """
    Triggers OpenClaw to research audience building and account creation automation.
    """
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] Starting Social Growth & Tool Improvement Cycle...")
    
    # Allow user to pass specific focus via CLI args
    user_focus = " ".join(sys.argv[1:]) if len(sys.argv) > 1 else "" 
    
    base_prompt = (
        "You are the Growth & Automation Architect. The user 'lowkey' wants to build an audience to sell services and game solvers.\n\n"
        
        "**OBJECTIVE 1: WHISPER.CPP INTEGRATION (ZERO BUDGET)**\n"
        "- We have successfully installed `whisper-cpp` and downloaded the `tiny.en` model.\n"
        "- Binary Path: `/opt/homebrew/Cellar/whisper-cpp/1.8.3/libexec/bin/whisper-cli`\n"
        "- Model Path: `/Users/lowkey/.openclaw/models/ggml-tiny.en.bin`\n"
        "- REWRITE `engine/captcha_solver.py` to use these local assets via `subprocess` instead of the Python `openai-whisper` library.\n"
        "  - The solver should download the reCAPTCHA audio challenge (usually a .mp3 or .wav).\n"
        "  - Run the `whisper-cli` command on that file.\n"
        "  - Parse the text output and submit it.\n\n"
        
        "**OBJECTIVE 2: Execution**\n"
        "- Update: '/Users/lowkey/.openclaw/knowledge/social_growth_leads.md' with the new local Whisper instructions.\n"
        "- Check if any other dependencies (like `pydub`) are still needed for audio conversion if reCAPTCHA gives an .mp3 (Whisper-cpp prefers .wav).\n"
    )
    
    final_prompt = f"{base_prompt} \n\n**Current Focus Override:** {user_focus}" if user_focus else base_prompt
    
    try:
        # Trigger OpenClaw Agent
        cmd = [
            "openclaw", 
            "agent", 
            "--message", final_prompt, 
            "--session-id", "growth-architect",
            "--timeout", "1200"
        ]
        
        print(f"[{timestamp}] Executing: ", ' '.join(cmd))
        subprocess.run(cmd, check=True)
        
        print(f"[{timestamp}] Cycle Complete. Check ~/.openclaw/knowledge/social_growth_leads.md")
        
    except subprocess.CalledProcessError as e:
        print(f"[{timestamp}] Agent Execution Failed: {e}")
    except FileNotFoundError:
        print(f"[{timestamp}] Error: 'openclaw' command not found. Ensure it is in your PATH.")
    except Exception as e:
        print(f"[{timestamp}] Critical Error: {e}")

if __name__ == "__main__":
    run_social_growth_cycle()
