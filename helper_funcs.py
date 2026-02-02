# functions shared across files
import socket
import os

def setup_directories():
    """Set up directory structure based on environment."""
    nodename = socket.gethostname()
    if nodename == "oMac.local":
        root = os.path.expanduser(f"/Users/ohouck/Library/CloudStorage/OneDrive-TheUniversityofChicago/ercot_sim_weather_forecasts")

    else:
        raise Exception(f"Unknown environment, Please specify the root directory. "
                        f"Nodename found: {nodename}")

    dirs = {
        'root': root,
    }

    for path in dirs.values():
        os.makedirs(path, exist_ok=True)
    return dirs