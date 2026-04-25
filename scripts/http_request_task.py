"""
HTTP Request Task for the Scheduler
Makes a GET request to a predefined URL and saves the response body to a file.
Edit the URL and OUTPUT_FILE variables below before uploading.
"""
import requests
from datetime import datetime

# ---------- CONFIGURE THESE ----------
URL = "https://api.github.com/zen"         # any URL you want to call
OUTPUT_FILE = "response_output.txt"        # file to save the response to
# --------------------------------------

def main():
    try:
        print(f"[{datetime.now()}] Requesting {URL}")
        response = requests.get(URL, timeout=30)
        response.raise_for_status()  # raise an error for bad status codes

        # Save the response text to the output file
        with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
            f.write(response.text)
        print(f"[{datetime.now()}] Response saved to {OUTPUT_FILE} (status: {response.status_code})")

    except Exception as e:
        print(f"[{datetime.now()}] ERROR: {str(e)}")
        raise  # re-raise so the scheduler can log it as a failure

if __name__ == "__main__":
    main()