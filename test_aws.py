import boto3
import os
from dotenv import load_dotenv

load_dotenv()

s3_client = boto3.client(
    's3',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    region_name=os.getenv('AWS_REGION')
)

BUCKET_NAME = 'aviation-data-lake-kamil-2026'

try:
    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key='test_folder/hello_cloud.txt',
        Body='First test'
    )
    print("SUKCES! Plik został wysłany na S3.")
except Exception as e:
    print(f"BŁĄD POŁĄCZENIA: {e}")