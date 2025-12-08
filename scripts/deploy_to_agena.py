
import os
import shutil
import gzip
import datetime

# Configuration
SOURCE_DIR = "data/processed"
AGENA_DIR = "c:/Users/eugen/Documents/AgenaTrader_QuantCache"

FILES_TO_DEPLOY = [
    {
        "gz": "cot_20y_disagg_merged.csv.gz", 
        "target": "cot_20y_disagg_merged.csv"
    },
    {
        "gz": "cot_20y_tff.csv.gz",
        "target": "cot_20y_tff.csv"
    }
]

def deploy():
    if not os.path.exists(AGENA_DIR):
        print(f"AgenaTrader dir not found: {AGENA_DIR}, creating...")
        os.makedirs(AGENA_DIR, exist_ok=True)
        
    for item in FILES_TO_DEPLOY:
        gz_path = os.path.join(SOURCE_DIR, item["gz"])
        target_name = item["target"]
        final_path = os.path.join(AGENA_DIR, target_name)
        
        if not os.path.exists(gz_path):
            print(f"[WARN] Source file missing: {gz_path}")
            continue
            
        print(f"[DEPLOY] Unzipping {gz_path} -> {final_path} ...")
        
        # Unzip and copy
        try:
            with gzip.open(gz_path, 'rb') as f_in:
                with open(final_path, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
        except Exception as e:
            print(f"[ERROR] Failed to unzip/copy {target_name}: {e}")
            continue

        # Create Stamp
        stamp_path = final_path + ".stamp"
        try:
            now_iso = datetime.datetime.utcnow().isoformat()
            with open(stamp_path, 'w') as f_stamp:
                f_stamp.write(now_iso)
            print(f"[STAMP] Created {stamp_path}")
        except Exception as e:
            print(f"[ERROR] Failed to create stamp for {target_name}: {e}")

    print("[SUCCESS] All files deployed to AgenaTrader.")

if __name__ == "__main__":
    deploy()
