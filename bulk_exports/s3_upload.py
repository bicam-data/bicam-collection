#!/usr/bin/env python3

import argparse
import logging
import os
from pathlib import Path
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed

from tqdm import tqdm
import boto3
from botocore.exceptions import ClientError, NoCredentialsError
from dotenv import load_dotenv

load_dotenv()

# --- Configuration ---
MAX_WORKERS = 11  # Adjust based on your machine and network capacity
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
        # Special case for the master zip file
        return "complete"
    elif filename_stem.startswith("bicam_"):
        # Extract the part after "bicam_"
        parts = filename_stem.split('_', 1)
        if len(parts) > 1:
            return parts[1] # e.g., "bills" from "bicam_bills"
        else:
            # Should not happen if starts with "bicam_", but handle defensively
            logging.warning(f"Filename stem '{filename_stem}' started with 'bicam_' but could not extract prefix. Using full stem.")
            return filename_stem
    else:
        # Handle filename patterns that don't match expected "bicam_" or "bicam"
        # Option 1: Use the full stem as prefix (current fallback)
        logging.warning(f"Unexpected filename stem pattern: '{filename_stem}'. Using full stem as prefix.")
        return filename_stem
        # Option 2: Assign a default prefix like 'unknown'
        # logging.warning(f"Unexpected filename stem pattern: '{filename_stem}'. Using 'unknown' prefix.")
        # return "unknown"
        # Option 3: Raise an error if strict matching is required
        # raise ValueError(f"Unexpected filename stem pattern: {filename_stem}")

def upload_file_to_s3(local_path: Path, bucket: str, s3_key: str) -> bool:
    """
    Uploads a single file to an S3 bucket.

    Args:
        local_path: The Path object of the local file to upload.
        bucket: The target S3 bucket name.
        s3_key: The target key (full path including filename) in the S3 bucket.

    Returns:
        True if upload was successful, False otherwise.
    """
    try:
        # Create a new client per thread for thread safety
        s3_client = boto3.client("s3")
        logging.info(f"Uploading {local_path.name} to s3://{bucket}/{s3_key}...")
        s3_client.upload_file(str(local_path), bucket, s3_key)
        logging.info(f"Successfully uploaded {local_path.name} to s3://{bucket}/{s3_key}")
        return True
    except NoCredentialsError:
        logging.error(
            "AWS credentials not found. Configure credentials (e.g., via "
            "AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY environment variables, "
            "~/.aws/credentials file, or IAM role)."
        )
        # Stop further uploads if credentials are missing
        return False
    except ClientError as e:
        logging.error(f"Failed to upload {local_path.name}: {e}")
        return False
    except Exception as e:
        logging.error(f"An unexpected error occurred uploading {local_path.name}: {e}")
        return False

def main():
    parser = argparse.ArgumentParser(
        description=f"Upload zipped dataset files from a local directory to the '{os.getenv('S3_BUCKET_NAME')}' S3 bucket.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "-d",
        "--directory",
        required=True,
        help="Path to the local directory containing the .zip files to upload.",
    )
    args = parser.parse_args()

    local_dir = Path(args.directory)

    if not local_dir.is_dir():
        logging.error(f"Error: Directory not found or is not a directory: {local_dir}")
        sys.exit(1)

    logging.info(f"Scanning directory: {local_dir}")
    zip_files_to_upload = []

    for item in local_dir.iterdir():
        if item.is_file() and item.suffix.lower() == ".zip":
            filename_stem = item.stem
            s3_prefix = get_target_prefix(filename_stem)
            s3_key = f"{s3_prefix}/{item.name}"
            logging.info(f"Found '{item.name}'. Target S3 key: s3://{os.getenv('S3_BUCKET_NAME')}/{s3_key}")
            zip_files_to_upload.append({"local_path": item, "s3_key": s3_key})
        elif item.is_file() and item.suffix.lower() != ".zip":
            logging.debug(f"Skipping non-zip file: {item.name}")
        elif item.is_dir():
             logging.debug(f"Skipping sub-directory: {item.name}")


    if not zip_files_to_upload:
        logging.warning(f"No .zip files found in directory: {local_dir}")
        sys.exit(0)

    logging.info(f"Found {len(zip_files_to_upload)} .zip files to upload. Starting parallel uploads...")

    successful_uploads = 0
    failed_uploads = 0
    credentials_missing = False

    # Use ThreadPoolExecutor for parallel I/O-bound tasks
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Submit all upload tasks
        future_to_file = {
            executor.submit(
                upload_file_to_s3,
                file_info["local_path"],
                os.getenv('S3_BUCKET_NAME'),
                file_info["s3_key"],
            ): file_info
            for file_info in zip_files_to_upload
        }

        # Process results as they complete with a progress bar
        with tqdm(total=len(future_to_file), desc="Uploading files to S3", unit="file") as pbar:
            for future in as_completed(future_to_file):
                file_info = future_to_file[future]
                filename = file_info["local_path"].name
                try:
                    success = future.result()
                    if success:
                        successful_uploads += 1
                    else:
                        failed_uploads += 1
                        # Check if failure was due to credentials to stop early
                        if "AWS credentials not found" in str(future.exception()):
                            credentials_missing = True

                except Exception as exc:
                    logging.error(f"{filename} generated an exception during upload: {exc}")
                    failed_uploads += 1

                # Update progress bar
                pbar.update(1)
                pbar.set_postfix(successful=successful_uploads, failed=failed_uploads)

                # Optional: Stop processing if credentials failed on any thread
                if credentials_missing:
                    logging.error("Halting further uploads due to missing AWS credentials.")
                    # Attempt to cancel remaining futures (may not always be possible)
                    for f in future_to_file:
                        if not f.done():
                            f.cancel()
                    break # Exit the result processing loop


    logging.info("-" * 30)
    logging.info("Upload Summary:")
    logging.info(f"  Successful: {successful_uploads}")
    logging.info(f"  Failed:     {failed_uploads}")
    logging.info("-" * 30)

    if failed_uploads > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()