import json
import tempfile
import time
import os
import logging
from datetime import datetime

import pandas as pd
from confluent_kafka import Consumer
from minio import Minio
from minio.error import S3Error
from dotenv import load_dotenv
from typing import Optional

#load env

load_dotenv()


#Configure logging

logging.basicConfig(
    level=logging.DEBUG, 
    format='%(asctime)s [%(levelname)s]  %(message)s'
)

logger = logging.getLogger(__name__)

#Kafka variables
KAFKA_BOOTSTRAP_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVER")
KAFKA_TOPIC_BATCH = os.getenv("KAFKA_TOPIC_BATCH")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_BATCH_ID")

#Minio configuration
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_BUCKET = os.getenv("MINIO_BUCKET")  



def create_minio_client():
    """Initialize Minio client"""
    
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )
    
def ensure_bucket_exists(client, bucket_name):
    """Ensure the specified bucket exists in Minio, create if it doesn't"""
    
    try:
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)
            logger.info(f"Created bucket: {bucket_name}")
        else:
            logger.info(f"Bucket already exists: {bucket_name}")
    except S3Error as e:
        logger.error(f"Error checking/creating bucket: {e}")
        raise e

def main():
    #create a Minio client
    minio_client = create_minio_client()
    
    ensure_bucket_exists(minio_client, MINIO_BUCKET)
    
    conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVER,
        'group.id': KAFKA_GROUP_ID,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False
    }
    
    consumer = Consumer(conf)
    consumer.subscribe([KAFKA_TOPIC_BATCH])
    
    logger.info(f"Starting batch data consumer for topic: {KAFKA_TOPIC_BATCH} with group ID: {KAFKA_GROUP_ID}")
    
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                logger.error(f"Consumer error: {msg.error()}")
                continue
            try:
                data = json.loads(msg.value().decode('utf-8'))
                symbol = data.get("symbol")
                date = data.get("batch_date")
                
                year, month, day = date.split("-")
                
                df = pd.DataFrame([data])
                
                #Saving to Minio
                
                object_name = f"raw/historical/year={year}/month={month}/day={day}/{symbol}_{datetime.now().strftime('%H%M%S')}.parquet"
                parquet_file = os.path.join(
                tempfile.gettempdir(),
                f"{symbol}.parquet"
                )
                df.to_parquet(parquet_file, index=False)
                
                minio_client.fput_object(
                    MINIO_BUCKET,
                    object_name,
                    parquet_file
                )
                logger.info(f"Wrote data for symbol: {symbol} to Minio at {object_name}")
                
                os.remove(parquet_file)
                
                consumer.commit()
            
                logger.info(f"Received message: {data}")
            except Exception as e:
                logger.error(f"Failed to decode message: {e}")
            
    except KeyboardInterrupt:
        logger.info("Batch data consumer interrupted by user")
    finally:
        consumer.close()
        logger.info("Batch data consumer closed")
    
if __name__ == "__main__":
    main()
            