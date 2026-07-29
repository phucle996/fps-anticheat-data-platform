#!/usr/bin/env python3
"""
PUBG Anti-Cheat Data Platform — Invariant Verification Suite
Kiểm tra 7 Định luật Bảo toàn Dữ liệu (Data Invariants) trên Data Lake S3 & Pipeline
"""

import sys
import os
import argparse
import boto3
import io
import pandas as pd

def parse_args():
    parser = argparse.ArgumentParser(description="PUBG Anti-Cheat Data Invariant Verifier")
    parser.add_argument("--endpoint", default=os.getenv("MINIO_ENDPOINT", "http://localhost:9000"))
    parser.add_argument("--access-key", default=os.getenv("MINIO_ACCESS_KEY", "minioadmin"))
    parser.add_argument("--secret-key", default=os.getenv("MINIO_SECRET_KEY", "minioadmin"))
    parser.add_argument("--bucket", default=os.getenv("MINIO_BUCKET", "fps-anticheat-datalake"))
    parser.add_argument("--allow-empty", action="store_true", help="Cho phép Data Lake rỗng (khi mới khởi tạo chưa ingest)")
    return parser.parse_args()

def list_all_keys(s3, bucket, prefix):
    """Quét tất cả keys với pagination ContinuationToken (vượt giới hạn 1,000 objects của AWS S3)"""
    keys = []
    continuation_token = None
    while True:
        kwargs = {"Bucket": bucket, "Prefix": prefix}
        if continuation_token:
            kwargs["ContinuationToken"] = continuation_token
        resp = s3.list_objects_v2(**kwargs)
        for obj in resp.get("Contents", []):
            if obj["Key"].endswith(".parquet"):
                keys.append(obj["Key"])
        if resp.get("IsTruncated"):
            continuation_token = resp.get("NextContinuationToken")
        else:
            break
    return keys

def verify_invariants(args):
    print("==================================================================")
    print("  PUBG ANTI-CHEAT DATA PLATFORM — INVARIANT VERIFICATION SUITE")
    print("==================================================================")

    s3 = boto3.client(
        "s3",
        endpoint_url=args.endpoint,
        aws_access_key_id=args.access_key,
        aws_secret_access_key=args.secret_key,
    )

    # Quét tất cả Parquet files theo pagination
    bronze_keys = list_all_keys(s3, args.bucket, "bronze/")
    silver_ke_keys = list_all_keys(s3, args.bucket, "silver/kill-events/")
    silver_pm_keys = list_all_keys(s3, args.bucket, "silver/player-match/")
    gold_keys = list_all_keys(s3, args.bucket, "gold/player-match-features/")

    print(f"[DATA LAKE OBJECTS]")
    print(f"  - Bronze Parquet files:              {len(bronze_keys)}")
    print(f"  - Silver kill-events Parquet files:  {len(silver_ke_keys)}")
    print(f"  - Silver player-match Parquet files: {len(silver_pm_keys)}")
    print(f"  - Gold features Parquet files:       {len(gold_keys)}")
    print("------------------------------------------------------------------")

    violations = []

    if not args.allow_empty:
        if not bronze_keys:
            violations.append("INVARIANT 1 VIOLATED: Bronze Data Lake layer trống (Không tìm thấy Parquet file nào trong bronze/)")
        if not silver_ke_keys:
            violations.append("INVARIANT 2 VIOLATED: Silver kill-events layer trống")
        if not silver_pm_keys:
            violations.append("INVARIANT 3 VIOLATED: Silver player-match layer trống")
        if not gold_keys:
            violations.append("INVARIANT 4 VIOLATED: Gold feature store layer trống")

    total_bronze_rows = 0
    for key in bronze_keys:
        obj = s3.get_object(Bucket=args.bucket, Key=key)
        df = pd.read_parquet(io.BytesIO(obj["Body"].read()))
        total_bronze_rows += len(df)

    total_silver_ke_rows = 0
    synthetic_rows = 0
    for key in silver_ke_keys:
        obj = s3.get_object(Bucket=args.bucket, Key=key)
        df = pd.read_parquet(io.BytesIO(obj["Body"].read()))
        total_silver_ke_rows += len(df)
        
        # Checking synthetic row markers
        if "is_synthetic" in df.columns or "mock_id" in df.columns:
            synthetic_rows += len(df)
        elif "killer_name" in df.columns:
            suspect_matches = df["killer_name"].astype(str).str.contains("player_suspect_|player_alpha_")
            synthetic_rows += suspect_matches.sum()

    # Concatenate toàn bộ silver player_match để kiểm tra cross-file duplicates
    all_pm_dfs = []
    total_silver_pm_rows = 0
    for key in silver_pm_keys:
        obj = s3.get_object(Bucket=args.bucket, Key=key)
        df = pd.read_parquet(io.BytesIO(obj["Body"].read()))
        total_silver_pm_rows += len(df)
        if len(df) > 0:
            all_pm_dfs.append(df)

    cross_file_duplicates = 0
    if all_pm_dfs:
        full_pm_df = pd.concat(all_pm_dfs, ignore_index=True)
        if "match_id" in full_pm_df.columns and "player_id" in full_pm_df.columns:
            dups = full_pm_df.duplicated(subset=["match_id", "player_id"], keep=False)
            cross_file_duplicates = dups.sum()

    total_gold_rows = 0
    for key in gold_keys:
        obj = s3.get_object(Bucket=args.bucket, Key=key)
        df = pd.read_parquet(io.BytesIO(obj["Body"].read()))
        total_gold_rows += len(df)

    print(f"Bronze Rows Total:            {total_bronze_rows}")
    print(f"Silver Kill Events Rows:      {total_silver_ke_rows}")
    print(f"Silver Player-Match Rows:     {total_silver_pm_rows}")
    print(f"Gold Features Rows:           {total_gold_rows}")
    print(f"Synthetic Rows Detected:      {synthetic_rows}")
    print(f"Cross-file Duplicate (match,player) PM: {cross_file_duplicates}")
    print("------------------------------------------------------------------")

    if total_bronze_rows > 0 and total_silver_ke_rows != total_bronze_rows:
        violations.append(f"INVARIANT 5 VIOLATED: Silver kill events ({total_silver_ke_rows}) != Bronze rows ({total_bronze_rows})")

    if cross_file_duplicates > 0:
        violations.append(f"INVARIANT 6 VIOLATED: Silver player-match chứa {cross_file_duplicates} cross-file duplicate (match_id, player_id) keys!")

    if synthetic_rows > 0:
        violations.append(f"INVARIANT 7 VIOLATED: Phát hiện {synthetic_rows} synthetic/mock seed rows trong Data Lake!")

    if violations:
        print("[FAIL] Phát hiện vi phạm Data Invariants:")
        for v in violations:
            print(f"  ❌ {v}")
        sys.exit(1)
    else:
        print("[PASS] Tất cả Data Invariants hợp lệ 100%! Pipeline an toàn & đúng đắn.")
        sys.exit(0)

if __name__ == "__main__":
    args = parse_args()
    verify_invariants(args)
