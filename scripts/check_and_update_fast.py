import requests
import gzip
import shutil
import os
import io

R2_URL = "https://pub-c5e8c78162df45f4bed6224f0ebacab6.r2.dev/data/processed/options_v60_ultra.csv.gz"
TARGET_DIR = os.path.join(os.path.expanduser("~"), "Documents", "AgenaTrader_QuantCache")
TARGET_FILE = os.path.join(TARGET_DIR, "options_v60_ultra.csv")

def update():
    print(f"Downloading {R2_URL}...")
    try:
        r = requests.get(R2_URL)
        if r.status_code == 200:
            print(f"Download OK. Size: {len(r.content)} bytes")
            
            # Decompress in memory
            with gzip.open(io.BytesIO(r.content), 'rt') as f_in:
                content = f_in.read()
                
            # Check Date
            lines = content.split('\n')
            if len(lines) > 1:
                header = lines[0].split(',')
                try:
                    idx = header.index("Date")
                    first_row = lines[1].split(',')
                    date_val = first_row[idx]
                    print(f"New Data Date: {date_val}")
                except:
                    print("Could not parse Date.")

            # Save
            os.makedirs(TARGET_DIR, exist_ok=True)
            with open(TARGET_FILE, "w", encoding='utf-8') as f_out:
                f_out.write(content)
                
            print(f"Saved to {TARGET_FILE}")
            
        else:
            print(f"Error {r.status_code}")
    except Exception as e:
        print(f"Exception: {e}")

if __name__ == "__main__":
    update()
