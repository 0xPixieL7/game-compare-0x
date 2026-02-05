import json
import os
import sys
import shutil
from pathlib import Path

# Configuration paths
OPENCLAW_DIR = Path("/Users/lowkey/.openclaw")
GEMINI_SETTINGS = Path("/Users/lowkey/.gemini/settings.json")
OPENCODE_JSON = OPENCLAW_DIR / "opencode.json"
SUPERASSIST_DATA = Path("/Users/lowkey/superassist")

def decommission_superassistant():
    print("Decommissioning SuperAssistant Services...")
    
    configs = [OPENCODE_JSON, GEMINI_SETTINGS]
    keys_to_remove = ["superassistant", "superassist-proxy"]
    
    for config_path in configs:
        if not config_path.exists():
            continue
            
        with open(config_path, 'r') as f:
            config = json.load(f)
            
        # Check both "mcpServers" and "mcp" keys
        for mcp_key in ["mcpServers", "mcp"]:
            if mcp_key in config:
                servers = config[mcp_key]
                removed = []
                for key in keys_to_remove:
                    if key in servers:
                        del servers[key]
                        removed.append(key)
                if removed:
                    print(f"Removed {', '.join(removed)} from {config_path.name} ({mcp_key})")
            
        with open(config_path, 'w') as f:
            json.dump(config, f, indent=4)
            
    # Remove the data directory
    if SUPERASSIST_DATA.exists():
        try:
            shutil.rmtree(SUPERASSIST_DATA)
            print(f"Removed unsafe directory: {SUPERASSIST_DATA}")
        except Exception as e:
            print(f"Failed to remove {SUPERASSIST_DATA}: {e}")

    print("Decommissioning complete.")

if __name__ == "__main__":
    decommission_superassistant()
