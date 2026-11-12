import argparse
import csv
import os
from pathlib import Path

import boto3


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--bucket",
        default=os.getenv("S3_BUCKET"),
        help="S3 bucket name. Can also be provided with S3_BUCKET env var.",
    )
    args = parser.parse_args()

    if not args.bucket:
        raise ValueError(
            "Bucket name is required. Use --bucket or set S3_BUCKET."
        )

    s3 = boto3.client("s3")

    prefix = "bronze/"
    key = "bronze/netflix_titles.csv"

    print("=== Bronze objects ===")
    response = s3.list_objects_v2(
        Bucket=args.bucket,
        Prefix=prefix,
    )

    for obj in response.get("Contents", []):
        print(f"{obj['Key']} | {obj['Size']} bytes")

    output_dir = Path("data")
    output_dir.mkdir(parents=True, exist_ok=True)

    local_file = output_dir / "netflix_titles.csv"

    s3.download_file(
        args.bucket,
        key,
        str(local_file),
    )

    print(f"\nDownloaded: s3://{args.bucket}/{key}")
    print(f"Local file: {local_file}")

    with local_file.open(
        "r",
        encoding="utf-8-sig",
        newline="",
    ) as f:
        reader = csv.reader(f)
        next(reader, None)
        record_count = sum(1 for _ in reader)

    print(f"CSV record count excluding header: {record_count}")


if __name__ == "__main__":
    main()