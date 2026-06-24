import logging 
import sys
import traceback
from datetime import datetime, timedelta
import os

import boto3
import numpy as np
from dotenv import load_dotenv
import pandas as pd
import snowflake.connector

import io

load_dotenv()

#Configure logging

logging.basicConfig(
    level=logging.DEBUG, 
    format='%(asctime)s [%(levelname)s]  %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

#S3/Minio

MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_BUCKET = os.getenv("MINIO_BUCKET")

#Snowflake








