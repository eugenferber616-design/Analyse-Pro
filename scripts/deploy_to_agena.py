
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


PLAIN_FILES = [
    "options_v60_ultra.csv",
    "options_oi_summary.csv",
    "options_oi_by_expiry.csv",
    "options_oi_totals.csv"
]

def deploy():
    if not os.path.exists(AGENA_DIR):
        print(f"AgenaTrader dir not found: {AGENA_DIR}, creating...")
        os.makedirs(AGENA_DIR, exist_ok=True)
        
    # 1. GZ Deployment
    for item in FILES_TO_DEPLOY:
        gz_path = os.path.join(SOURCE_DIR, item["gz"])
        target_name = item["target"]
        final_path = os.path.join(AGENA_DIR, target_name)
        
        if not os.path.exists(gz_path):
            print(f"[WARN] Source file missing: {gz_path}")
            continue
            
        print(f"[DEPLOY] Unzipping {gz_path} -> {final_path} ...")
        
        try:
            with gzip.open(gz_path, 'rb') as f_in:
                with open(final_path, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
        except Exception as e:
            print(f"[ERROR] Failed to unzip/copy {target_name}: {e}")
            continue

    # 2. Plain File Deployment
    for fname in PLAIN_FILES:
        src_path = os.path.join(SOURCE_DIR, fname)
        final_path = os.path.join(AGENA_DIR, fname)
        
        if not os.path.exists(src_path):
            # print(f"[WARN] Source plain file missing: {src_path}")
            continue
        
        print(f"[DEPLOY] Copying {fname} -> {final_path} ...")
        try:
           shutil.copy2(src_path, final_path)
        except Exception as e:
           print(f"[ERROR] Failed to copy {fname}: {e}")


    # 3. Directory Merge (Profiles)
    SRC_PROFILES = os.path.join(SOURCE_DIR, "profiles")
    DST_PROFILES = os.path.join(AGENA_DIR, "profiles")
    
    if os.path.exists(SRC_PROFILES):
        print(f"[DEPLOY] Syncing Profiles {SRC_PROFILES} -> {DST_PROFILES} ...")
        if not os.path.exists(DST_PROFILES):
            os.makedirs(DST_PROFILES)
            
        for pfile in os.listdir(SRC_PROFILES):
            s = os.path.join(SRC_PROFILES, pfile)
            d = os.path.join(DST_PROFILES, pfile)
            if os.path.isfile(s):
                shutil.copy2(s, d)
    else:
        print("[WARN] No profiles/ folder found to deploy.")

    print("[SUCCESS] All files deployed to AgenaTrader.")

if __name__ == "__main__":
    deploy()
