#!/usr/bin/env python3

import argparse
import logging
import os
from pathlib import Path
import sys

from tqdm import tqdm
import boto3
from botocore.exceptions import ClientError, NoCredentialsError
from dotenv import load_dotenv
from boto3.s3.transfer import TransferConfig

load_dotenv()

# --- Configuration ---
GB = 1024 ** 3
MB = 1024 ** 2
TRANSFER_CONFIG = TransferConfig(
    multipart_threshold=100 * MB,
    max_concurrency=10,
    multipart_chunksize=32 * MB,
    use_threads=True
)
# -------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

def get_target_prefix(filename_stem: str) -> str:
    """
    Determines the S3 prefix based on the filename stem.
    - "bicam_PREFIX.zip" -> "PREFIX"
    - "bicam.zip" -> "complete"

    Args:
        filename_stem: The name of the file without the .zip extension.

    Returns:
        The derived S3 prefix (folder name).
    """
    if filename_stem == "bicam":
        return "complete"
    elif filename_stem.startswith("bicam_"):
        parts = filename_stem.split('_', 1)
        if len(parts) > 1:
            return parts[1]
        else:
            logging.warning(f"Filename stem '{filename_stem}' started with 'bicam_' but could not extract prefix. Using full stem.")
            return filename_stem
    else:
        logging.warning(f"Unexpected filename stem pattern: '{filename_stem}'. Using full stem as prefix.")
        return filename_stem

def upload_file_to_s3(local_path: Path, bucket: str, s3_key: str) -> bool:
    """
    Uploads a single file to an S3 bucket with TransferConfig.
    Uses a shared client, assuming sequential execution.
    """
    try:
        s3_client = boto3.client("s3")

        local_file_size = local_path.stat().st_size
        logging.info(f"Starting upload for {local_path.name} ({local_file_size / MB:.2f} MB) to s3://{bucket}/{s3_key}...")

        s3_client.upload_file(
            str(local_path),
            bucket,
            s3_key,
            Config=TRANSFER_CONFIG
        )
        logging.info(f"Successfully completed upload for {local_path.name} to s3://{bucket}/{s3_key}")
        return True
    except NoCredentialsError:
        logging.error(
            "AWS credentials not found. Configure credentials (e.g., via "
            "AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY environment variables, "
            "~/.aws/credentials file, or IAM role)."
        )
        return False # Indicate failure to stop if credentials missing globally
    except ClientError as e:
        logging.error(f"S3 ClientError uploading {local_path.name}: {e}")
        return False
    except Exception as e:
        logging.exception(f"An unexpected error occurred uploading {local_path.name}: {e}")
        return False

def main():
    parser = argparse.ArgumentParser(
        description=f"Upload zipped dataset files from a local directory to an S3 bucket (SEQUENTIAL UPLOAD).",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "-d",
        "--directory",
        required=True,
        help="Path to the local directory containing the .zip files to upload.",
    )
    s3_bucket_name = os.getenv('S3_BUCKET_NAME')
    parser.description = f"Upload zipped dataset files from a local directory to the '{s3_bucket_name or 'Not Set!'}' S3 bucket (SEQUENTIAL UPLOAD)."

    args = parser.parse_args()

    if not s3_bucket_name:
         logging.error("Error: S3_BUCKET_NAME environment variable not set. Please set it in your .env file or environment.")
         sys.exit(1)

    local_dir = Path(args.directory)
    if not local_dir.is_dir():
        logging.error(f"Error: Directory not found or is not a directory: {local_dir}")
        sys.exit(1)

    logging.info(f"Scanning directory: {local_dir}")
    zip_files_to_upload = []
    for item in local_dir.iterdir():
        if item.is_file() and item.suffix.lower() == ".zip":
            try:
                filename_stem = item.stem
                s3_prefix = get_target_prefix(filename_stem)
                if not s3_prefix:
                    logging.warning(f"Could not determine target prefix for {item.name}. Skipping.")
                    continue
                s3_key = f"{s3_prefix}/{item.name}"
                file_size_mb = item.stat().st_size / MB
                logging.info(f"Found '{item.name}' ({file_size_mb:.2f} MB). Target S3 key: s3://{s3_bucket_name}/{s3_key}")
                zip_files_to_upload.append({"local_path": item, "s3_key": s3_key})
            except Exception as e:
                 logging.exception(f"Error processing file {item.name}: {e}")
        elif item.is_file() and item.suffix.lower() != ".zip":
            logging.debug(f"Skipping non-zip file: {item.name}")
        elif item.is_dir():
             logging.debug(f"Skipping sub-directory: {item.name}")

    if not zip_files_to_upload:
        logging.warning(f"No .zip files found in directory: {local_dir}")
        sys.exit(0)

    logging.info(f"Found {len(zip_files_to_upload)} .zip files to upload. Starting sequential uploads...")

    successful_uploads = 0
    failed_uploads = 0

    for file_info in tqdm(zip_files_to_upload, desc="Uploading files sequentially to S3", unit="file"):
        local_path = file_info["local_path"]
        s3_key = file_info["s3_key"]

        try:
            success = upload_file_to_s3(
                local_path,
                s3_bucket_name,
                s3_key,
            )
            if success:
                successful_uploads += 1
            else:
                failed_uploads += 1
                # Check if the failure was due to credentials
            
                if successful_uploads == 0 and failed_uploads == 1:
                     
                     logging.warning("First upload failed. Check credentials if uploads continue to fail.")


        except Exception as exc:
            # This catches errors in the loop logic itself, not usually from upload_file_to_s3
            logging.error(f"{local_path.name} generated an exception in the main loop: {exc}")
            failed_uploads += 1

    logging.info("-" * 30)
    logging.info("Upload Summary:")
    logging.info(f"  Successful: {successful_uploads}")
    logging.info(f"  Failed:     {failed_uploads}")
    logging.info("-" * 30)

    if failed_uploads > 0:
        sys.exit(1)
    else:
        sys.exit(0) # Explicitly exit with 0 on success


if __name__ == "__main__":
    main()