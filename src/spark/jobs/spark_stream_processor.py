import os
import sys
import traceback
from datetime import datetime, timedelta
import logging
from dotenv import load_dotenv

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.sql import functions as F

from minio import Minio
from minio.error import S3Error

#load env

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


#Minio configuration
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_BUCKET = os.getenv("MINIO_BUCKET")

def create_spark_session():
    logger.info("Initializing Spark session with S3 configuration...")
    spark = (SparkSession.builder
             .appName("StockMarketBatchProcessor")
             .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.11.901")
             .config("spark.streaming.StopGracefullyOnShutdown", "true")
             .config("spark.executor.memory", "1g")
             .config("spark.execuror.cores", "1")
             .config("spark.default.parallelism", "2")
             .config("spark.sql.shuffle.partitions", "2")
             .getOrCreate())
    
    spark_conf = spark.sparkContext._jsc.hadoopConfiguration()
    spark_conf.set("fs.s3a.access.key", MINIO_ACCESS_KEY)
    spark_conf.set("fs.s3a.secret.key", MINIO_SECRET_KEY)
    spark_conf.set("fs.s3a.endpoint", MINIO_ENDPOINT)
    spark_conf.set("fs.s3a.path.style.access", "true")
    spark_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    spark_conf.set("fs.s3a.connection.ssl.enabled", "false")
    spark_conf.set("fs.s3a.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    
    spark.sparkContext.setLogLevel("WARN")
    logger.info("Spark session initialized successfully")
    
    return spark


def log_raw_data(df, batch_id):
    if df.count() > 0:
        logger.info(f"Batch {batch_id} - Raw Data Count: {df.count()}")
        for row in df.collect():
            logger.info(f"Raw data row: {row.asDict()}")
    else:
        logger.info(f"Batch {batch_id} - No raw data found")
        
        


def define_schema():
    logger.info("Defining schema for stock data")
    
    return StructType([
        StructField("symbol", StringType(), False),
        StructField("price", DoubleType(), True),
        StructField("change", DoubleType(), True),
        StructField("percent_change", DoubleType(), True),
        StructField("volume", StringType(), True),
        StructField("timestamp", StringType(), True),   
    ])

def read_stream_from_s3(spark):
    logger.info("\n --- Setting up streaming read from s3 ---")
    
    schema = define_schema()
    
    s3_path = f"s3a://{MINIO_BUCKET}/raw/realtime/"
    logger.info(f"Reading from s3 path: {s3_path}")
    
    try:
        streaming_df = (spark.readStream
                        .schema(schema)
                        .option("header", "true")
                        .parquet(s3_path))
        
        streaming_df = (streaming_df
                        .withColumn(("timestamp"), F.to_timestamp("timestamp"))
                        .withColumn(("price"), F.col("price").cast(DoubleType()))
                        .withColumn(("change"), F.col("change").cast(DoubleType()))
                        .withColumn(("percent_change"), F.col("percent_change").cast(DoubleType()))
                        .withColumn(("volume"), F.col("volume").cast(IntegerType()))
                        )
        logger.info("Streaming Dataframe Schema:")
        logger.info("\n" + streaming_df._jdf.schema().treeString())
        
        streaming_df.writeStream.foreachBatch(log_raw_data(streaming_df, )).outputMode("append").start()
        
        
        return streaming_df
    except Exception as e:
        logger.error(f"Error reading from S3: {e}")
        return None
        
    


def process_streaming_data(df):
    logger.info("\n --- Processing streaming Data ---")
    
    if df is None:
        logger.error("No streaming data found")
        return None
    
    try:
        window_15min = F.window("timestamp", "15 minutes", "5 minutes")
        window_1h = F.window("timestamp", "1 hour", "10 minutes")
        
        
        df_15min = (df
                    .groupBy(
                        F.col("symbol"),
                        window_15min.alias("window")
                    ).agg(
                        F.avg("price").alias("ma_15m"),
                        F.stddev("price").alias("volatility_15m"),
                        F.sum("volume").alias("volume_sum_15m")
                    )
                    .withColumn("window_start", F.col("window.start"))
                    .withColumn("window_end", F.col("window.end"))
                    .drop("window")
                    )
        
        df_1hour = (df
                    .groupBy(
                        F.col("symbol"),
                        window_1h.alias("window")
                    ).agg(
                        F.avg("price").alias("ma_1h"),
                        F.stddev("price").alias("volatility_1h"),
                        F.sum("volume").alias("volume_sum_1h")
                    )
                    .withColumn("window_start", F.col("window.start"))
                    .withColumn("window_end", F.col("window.end"))
                    .drop("window")
                    )
        
        processed_df = (df_15min
                        .join(
                            df_1hour,
                            (df_15min.symbol == df_1hour.symbol) &
                            (df_15min.window_start == df_1hour.window_start),
                            "inner"
                        )
                        .select(
                            df_15min.symbol,
                            df_15min.window_start.alias("window_start"),
                            df_15min.window_end.alias("window_15m_end"),
                            df_15min.window_start.alias("window_1h_end"),
                            df_15min.ma_15m,
                            df_1hour.ma_1h,
                            df_15min.volatility_15m,
                            df_1hour.volatility_1h,
                            df_15min.volume_sum_15m,
                            df_1hour.volume_sum_1h,
                        )
                        )
        logger.info("Processed Streaming Dataframe schema")
        logger.info("\n" + processed_df._jdf.schema().treeString())
        
        return processed_df
    except Exception as e:
        logger.error(f"Error processing streaming stock data: {e}")
        logger.error(traceback.format_exc())
        return None
    
    
def process_write_batch(df, batch_id):
    if df.count() > 0:
        logger.info(f"Processing processed data batch {batch_id} with {df.count()} rows")
        for row in df.collect():
            logger.info(f"Processed data row: {row.asDict()}")
        
        output_path = f"s3a://{MINIO_BUCKET}/processed/realtime/"
        logger.info(f"writing batch {batch_id} to {output_path}")
        df.write \
            .mode("append") \
            .partitionBy("symbol") \
            .parquet(output_path)            
    else:
        logger.info(f"Processing data bath {batch_id} with 0 rows")     
    
        


def write_stream_to_s3(df):
    logger.info("\n ---- Writing Processed Streaming Data to S3 ")
    
    if df is None:
        logger.info("No processed Dataframe to write to S3")
        return None
    
    output_path = f"s3a://{MINIO_BUCKET}/processed/realtime/"
    logger.info(f"Writing processed streaming data to: {output_path}")
    
    try:
        checkpoint_path = f"s3a://{MINIO_BUCKET}/checkpoints/streaming_processor"
        
        query = (df.writeStream
                 .foreachBatch(process_write_batch)
                 .trigger(processingTime = '1 minute')
                 .option("checkpointLocation", checkpoint_path)
                 .outputMode("append")
                 .start())
        logger.info(f"Streaming query started, writing to {output_path}")
        return query
    except Exception as e:
        logger.error(f"Error load streaming stock data: {e}")
        logger.error(traceback.format_exc())
        return None




def main():
    """Main function to process stream stock data."""
    logger.info("\n =========================================================")
    logger.info("Starting Spark stream Processor...")
    logger.info(" =========================================================\n")
    
    date = None
    
    spark = create_spark_session()
    
    try:
        streamed_df = read_stream_from_s3(spark, date)
        
        if streamed_df is not None:
            processed_df = process_streaming_data(streamed_df)
            if processed_df is not None:
                query = write_stream_to_s3(processed_df)
                if query is not None:
                    logger.info("\n Streaming processor is running...")
                    query.awaitTermination()
                else: 
                    logger.info("\nFailed to start stream query")
            else:
                logger.info("\n Error processing data")
            
        
        else:
            logger.error("Failed to read data from S3")
    except Exception as e:
        logger.error(f"Error occured: {str(e)}")
    finally:
        logger.info("\n Stoping Spark session")
        spark.stop()
        logger.info("\n =========================================================")
        logger.info("Stoping Spark Batch Processor...")
        logger.info(" =========================================================\n")



if __name__ == "__main__":
    main()