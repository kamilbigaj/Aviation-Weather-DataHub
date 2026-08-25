import os
import logging
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

AERO_API_KEY = os.getenv('AERO_API_KEY')
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION = os.getenv('AWS_REGION')
S3_BUCKET_NAME = os.getenv('S3_BUCKET_NAME')
DATABASE_URL = os.getenv('DATABASE_URL')

AIRPORTS = ['EPWA', 'EGLL', 'EDDF']
RAW_DIR = "data/raw"

# Configure the application logger
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(module)s] - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("aviation_etl")