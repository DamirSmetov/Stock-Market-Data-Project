import json
import time
import os
import logging
from datetime import datetime

import pandas as pd
import yfinance as yf
from confluent_kafka import Producer
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
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID")

#Define stocks to collect for historical data
STOCKS = ["AAPL", "MSFT", "GOOGL", "AMZN", "META", "TSLA", "NVDA", "JPM", "V", "DIS", "INT", "CSCO", "ADBE", "NFLX", "PYPL", "CRM", "ORCL", "IBM", "QCOM", "TXN"]


class HistoricalDataCollector:
    def __init__(self, bootstrap_server = KAFKA_BOOTSTRAP_SERVER, topic = KAFKA_TOPIC_BATCH):
        self.logger = logger
        self.topic = topic
        
        #producer instance
        self.producer = {
            'bootstrap.servers': bootstrap_server,
            'client.id': 'historical-data-collector',
        }
        
        try: 
            self.producer = Producer(self.producer)
            self.logger.info(f"Kafka producer initialized successfully with bootstrap server: {bootstrap_server}")
        except Exception as e:
            self.logger.error(f"Failed to initialize Kafka producer: {e}")
            raise e
        
    def delivery_report(self, err, msg):
        if err is not None:
            self.logger.error(f"Message delivery failed: {err}")
        else:
            self.logger.debug(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

    
    def producer_to_kafka(self, df: pd.DataFrame, symbol: str):
        
        batch_id = datetime.now().strftime("%Y%m%d%H%M%S")
        df['batch_id'] = batch_id
        df['batch_date'] = datetime.now().strftime("%Y-%m-%d")
        
        records = df.to_dict(orient='records')
        successful_records = 0
        failed_records = 0
        
        for record in records:
            try:
                data = json.dumps(record)
                self.producer.produce(
                    topic=self.topic,
                    key=symbol,
                    value=data,
                    callback=self.delivery_report
                )
                self.producer.poll(0)
                successful_records += 1
            
            except Exception as e:
                self.logger.error(f"Error producing record for symbol {symbol}: {e}")
                failed_records += 1
                
        self.producer.flush()
        self.logger.info(f"Finished producing records for symbol: {symbol}. Successful records: {successful_records}, Failed records: {failed_records}")

    def fetch_historical_data(self, symbol: str, period: str = "1d") -> Optional[pd.DataFrame]:
        try:
            self.logger.info(f"Fetching historical data for symbol: {symbol} with period: {period}")
            ticker = yf.Ticker(symbol)
            df = ticker.history(period=period)
            df.reset_index(inplace=True)
            df.rename(columns={
                'Date': 'date',
                'Open': 'open',
                'High': 'high',
                'Low': 'low',
                'Close': 'close',
                'Volume': 'volume'
            }, inplace=True)
            
            df['date'] = df['date'].dt.strftime('%Y-%m-%d')
            
            df['symbol'] = symbol
            
            df = df[['date', 'open', 'high', 'low', 'close', 'volume', 'symbol']]
            
            self.logger.info(f"Successfully fetched historical data for symbol: {symbol}. Data points: {len(df)}")
            
            return df
        except Exception as e:
            self.logger.error(f"Error fetching historical data for symbol {symbol}: {e}")
            return None


    def collect_historical_data(self, period):
        symbols = STOCKS
        
        self.logger.info(f"Collecting historical data for period: {period} for stocks: {symbols}")
        
        successful_symbols = 0
        failed_symbols = 0
        
        for symbol in symbols:
            try:
                #fetch historical data
                df = self.fetch_historical_data(symbol, period)
                if df is not None and not df.empty:
                    self.producer_to_kafka(df, symbol)
                    successful_symbols += 1
                else:
                    self.logger.warning(f"No data found for symbol: {symbol}")
                    failed_symbols += 1
            except Exception as e:
                self.logger.error(f"Error processing symbol {symbol}: {e}")
                failed_symbols += 1

            time.sleep(1) 
             
        self.logger.info(f"Historical data collection completed. Successful symbols: {successful_symbols}, Failed symbols: {failed_symbols}")
                    
    
    
def main():
    try:
        logger.info("Starting historical data collection...")
        collector = HistoricalDataCollector(
            bootstrap_server=KAFKA_BOOTSTRAP_SERVER,
            topic=KAFKA_TOPIC_BATCH
        )
        
        collector.collect_historical_data(period="1y")
    except Exception as e:
        logger.error(f"Failed to initialize HistoricalDataCollector: {e}")
        return
    
    
    
    
    
if __name__ == "__main__":
    main()