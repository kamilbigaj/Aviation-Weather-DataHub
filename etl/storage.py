import boto3
import json
from datetime import datetime
from etl.config import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION, S3_BUCKET_NAME, logger

def upload_raw_to_s3(data, file_prefix):
    """Persists raw JSON payloads directly to AWS S3 (Data Lake layer)."""
    if not S3_BUCKET_NAME:
        logger.warning("S3_BUCKET_NAME not found in .env. Skipping S3 upload.")
        return

    try:
        s3_client = boto3.client(
            's3',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=AWS_REGION
        )
        today_str = datetime.now().strftime('%Y-%m-%d')
        file_name = f"raw_data/{today_str}/{file_prefix}.json"

        s3_client.put_object(Bucket=S3_BUCKET_NAME, Key=file_name, Body=json.dumps(data))
        logger.info(f"SUCCESS (S3): Raw data saved to Data Lake -> s3://{S3_BUCKET_NAME}/{file_name}")
    except Exception as e:
        logger.error(f"FAILED (S3): Error uploading file {file_prefix}. Reason: {e}")