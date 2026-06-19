import json
import tempfile
import time
import os
import logging
from datetime import datetime
import io

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
KAFKA_TOPIC_REALTIME = os.getenv("KAFKA_TOPIC_REALTIME")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_REALTIME_ID")

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
    
    BATCH_SIZE = 10  # Number of messages to batch before writing to Minio
    flush_time = time.time()  # Track the last time we flushed messages to Minio
    flush_interval = 60  
    messages = []
    consumer = Consumer(conf)
    
    consumer.subscribe([KAFKA_TOPIC_REALTIME])
    
    logger.info(f"Starting real-time data consumer for topic: {KAFKA_TOPIC_REALTIME} with group ID: {KAFKA_GROUP_ID}")
    
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                logger.error(f"Consumer error: {msg.error()}")
                continue
            try:
                key = msg.key().decode('utf-8') if msg.key() else None
                value = json.loads(msg.value().decode('utf-8'))
                messages.append(value)
                
                if len(messages) % 10 == 0:
                    logger.info(f"Received {len(messages)} messages, waiting for more to batch...")
            
                current_time = time.time()
            
                if(len(messages) >= BATCH_SIZE or (current_time - flush_time) >= flush_interval and len(messages) > 0):
                    df = pd.DataFrame(messages)
                    now = datetime.now()
                    timestamp = now.strftime("%Y%m%d%H%M%S")
                    
                    parquet_buffer = io.BytesIO()
                    df.to_parquet(parquet_buffer, index=False)
                    parquet_buffer.seek(0)
                    
                    #Saving to Minio
                    object_name = f"raw/realtime/year={now.year}/month={now.month:02d}/day={now.day:02d}/hour={now.hour:02d}/stock_data_{timestamp}.parquet"
                    
                    minio_client.put_object(
                        MINIO_BUCKET,
                        object_name,
                        parquet_buffer,
                        length=parquet_buffer.getbuffer().nbytes,
                        content_type='application/octet-stream'
                    )
                    
                    logger.info(f"Wrote batch of {len(messages)} messages to Minio at {object_name}")
                    messages.clear()
                    flush_time = current_time
                    consumer.commit()
            except Exception as e:
                logger.error(f"Failed to process message: {e}")
        
    except KeyboardInterrupt:
        logger.info("Real-time data consumer interrupted by user")
    finally:
        consumer.close()
        logger.info("Real-time data consumer closed")
    
if __name__ == "__main__":
    main()
