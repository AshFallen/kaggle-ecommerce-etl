import kaggle
import pandas as pd
import os
import boto3
import logging
from datetime import datetime
import io
from dotenv import load_dotenv

load_dotenv()
# -----------------
# LOGGING
# -----------------
logger = logging.getLogger()
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

# -----------------
# CONFIG
# -----------------
s3_bucket = os.getenv('S3_BUCKET')

kaggle_key = os.getenv('KAGGLE_KEY')
kaggle_username = os.getenv('KAGGLE_USERNAME')

if not all([s3_bucket, kaggle_key, kaggle_username]):
    raise EnvironmentError("Missing one or more required environment variables: S3_BUCKET, KAGGLE_KEY, KAGGLE_USERNAME")

assert kaggle_key is not None
assert kaggle_username is not None

os.environ['KAGGLE_USERNAME'] = kaggle_username 
os.environ['KAGGLE_KEY'] = kaggle_key

# S3 client
s3 = boto3.client('s3')

# -----------------
# EXTRACT & UPLOAD
# -----------------
def extract_raw_data_and_upload_to_s3():
    kaggle.api.authenticate()
    dataset = 'thedevastator/unlock-profits-with-e-commerce-sales-data'
    download_path = os.path.join(os.getcwd(), "kaggle_extracted")
    os.makedirs(download_path, exist_ok=True)


    files_response = kaggle.api.dataset_list_files(dataset)
    if not files_response.files:
        raise ValueError("No files found in the dataset.")

    available_files = [f.name for f in files_response.files if f and hasattr(f, 'name')]
    target_files = [f for f in available_files if "sale" in f.lower()]

    if not target_files:
        raise FileNotFoundError("No files containing 'sale' or 'sales' were found.")

    logger.info(f"Found {len(target_files)} sale-related file(s): {target_files}")

    for file_name in target_files:
        kaggle.api.authenticate()
    dataset = 'thedevastator/unlock-profits-with-e-commerce-sales-data'
    download_path = os.path.join(os.getcwd(), "kaggle_extracted")
    os.makedirs(download_path, exist_ok=True)

    logger.info("Downloading full dataset ZIP from Kaggle...")
    kaggle.api.dataset_download_files(dataset, path=download_path, unzip=True)
    logger.info("Dataset downloaded and extracted.")

    # List files in extracted folder
    all_files = os.listdir(download_path)
    target_files = [
        f for f in all_files
        if "sale" in f.lower() and f.lower().endswith('.csv') and "%20" not in f
    ]

    if not target_files:
        raise FileNotFoundError("No 'sale' CSV files found in the extracted dataset.")

    logger.info(f"Found {len(target_files)} sale-related file(s): {target_files}")
    logger.info(f"Dataset URL: https://www.kaggle.com/datasets/thedevastator/unlock-profits-with-e-commerce-sales-data")
    
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    for file_name in target_files:
        try:
            csv_path = os.path.join(download_path, file_name)

            if not os.path.exists(csv_path):
                raise FileNotFoundError(f"Expected file not found: {csv_path}")

            # Read the CSV (with fallback encoding)
            try:
                df = pd.read_csv(csv_path, low_memory=False, encoding='utf-8')
            except UnicodeDecodeError:
                logger.warning(f"UTF-8 decoding failed for {file_name}, retrying with ISO-8859-1...")
                df = pd.read_csv(csv_path, low_memory=False, encoding='ISO-8859-1')

            # Convert to in-memory buffer
            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, index=False)

            # Build S3 key with timestamp
            processed_key = f'kaggle/raw/{file_name.replace(".csv", f"_{timestamp}.csv")}'

            # Upload only the processed version
            s3.put_object(Bucket=s3_bucket, Key=processed_key, Body=csv_buffer.getvalue())
            logger.info(f"Uploaded processed CSV to s3://{s3_bucket}/{processed_key}")

        except Exception as e:
            logger.error(f"Failed to process {file_name}: {e}")
# -----------------
# MAIN
# -----------------
def main():
    try:
        raw_data = extract_raw_data_and_upload_to_s3()
        logger.info("Pipeline completed successfully")
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        raise

if __name__ == '__main__':
    main()