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
    return parser.parse_args()

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

    # 1. Quét tất cả Bronze Parquet files
    bronze_resp = s3.list_objects_v2(Bucket=args.bucket, Prefix="bronze/kill-events/")
    bronze_keys = [obj["Key"] for obj in bronze_resp.get("Contents", []) if obj["Key"].endswith(".parquet")]

    # 2. Quét tất cả Silver kill_events Parquet files
    silver_ke_resp = s3.list_objects_v2(Bucket=args.bucket, Prefix="silver/kill-events/")
    silver_ke_keys = [obj["Key"] for obj in silver_ke_resp.get("Contents", []) if obj["Key"].endswith(".parquet")]

    # 3. Quét tất cả Silver player_match Parquet files
    silver_pm_resp = s3.list_objects_v2(Bucket=args.bucket, Prefix="silver/player-match/")
    silver_pm_keys = [obj["Key"] for obj in silver_pm_resp.get("Contents", []) if obj["Key"].endswith(".parquet")]

    # 4. Quét tất cả Gold Parquet files
    gold_resp = s3.list_objects_v2(Bucket=args.bucket, Prefix="gold/player-match-features/")
    gold_keys = [obj["Key"] for obj in gold_resp.get("Contents", []) if obj["Key"].endswith(".parquet")]

    print(f"[DATA LAKE OBJECTS]")
    print(f"  - Bronze kill-events Parquet files:  {len(bronze_keys)}")
    print(f"  - Silver kill-events Parquet files:  {len(silver_ke_keys)}")
    print(f"  - Silver player-match Parquet files: {len(silver_pm_keys)}")
    print(f"  - Gold features Parquet files:       {len(gold_keys)}")
    print("------------------------------------------------------------------")

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
        if "is_synthetic" in df.columns or "mock_id" in df.columns:
            synthetic_rows += len(df)

    total_silver_pm_rows = 0
    duplicate_pm_keys = 0
    for key in silver_pm_keys:
        obj = s3.get_object(Bucket=args.bucket, Key=key)
        df = pd.read_parquet(io.BytesIO(obj["Body"].read()))
        total_silver_pm_rows += len(df)
        if len(df) > 0 and "match_id" in df.columns and "player_id" in df.columns:
            uniques = len(df.drop_duplicates(subset=["match_id", "player_id"]))
            if uniques != len(df):
                duplicate_pm_keys += (len(df) - uniques)

    total_gold_rows = 0
    for key in gold_keys:
        obj = s3.get_object(Bucket=args.bucket, Key=key)
        df = pd.read_parquet(io.BytesIO(obj["Body"].read()))
        total_gold_rows += len(df)

    # Output Summary Invariant Metrics
    print(f"Bronze Rows Total:            {total_bronze_rows}")
    print(f"Silver Kill Events Rows:      {total_silver_ke_rows}")
    print(f"Silver Player-Match Rows:     {total_silver_pm_rows}")
    print(f"Gold Features Rows:           {total_gold_rows}")
    print(f"Synthetic Rows Detected:      {synthetic_rows}")
    print(f"Duplicate (match,player) PM:  {duplicate_pm_keys}")
    print("------------------------------------------------------------------")

    # Assert Invariants
    violations = []
    if total_bronze_rows > 0 and total_silver_ke_rows != total_bronze_rows:
        violations.append(f"INVARIANT 3 VIOLATED: Silver kill events ({total_silver_ke_rows}) != Bronze rows ({total_bronze_rows})")

    if duplicate_pm_keys > 0:
        violations.append(f"INVARIANT 4 VIOLATED: Silver player-match chứa {duplicate_pm_keys} duplicate (match_id, player_id) keys!")

    if synthetic_rows > 0:
        violations.append(f"INVARIANT 5 VIOLATED: Phát hiện {synthetic_rows} synthetic/mock rows trong Silver data!")

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
