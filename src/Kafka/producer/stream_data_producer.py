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
KAFKA_TOPIC_REALTIME = os.getenv("KAFKA_TOPIC_REALTIME")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID")


STOCKS = ["AAPL", "MSFT", "GOOGL", "AMZN", "META", "TSLA", "NVDA", "JPM", "V", "DIS", "INTC", "CSCO", "ADBE", "NFLX", "PYPL", "CRM", "ORCL", "IBM", "QCOM", "TXN"]

class StreamDataProducer:
    def __init__(self, bootstrap_server = KAFKA_BOOTSTRAP_SERVER, topic = KAFKA_TOPIC_REALTIME, interval = 60):
        self.logger = logger
        self.topic = topic
        self.interval = interval
        self.current_stocks = STOCKS.copy()

        #producer instance
        self.producer = {
            'bootstrap.servers': bootstrap_server,
            'client.id': 'stream-data-producer',
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
            
    def generate_stock_data(self, symbol):
        stock = yf.Ticker(symbol)
        data = stock.history(period="1d", interval="1m")
        if not data.empty:
            latest = data.iloc[-1]
            stock_data = {
                "symbol": symbol,
                "timestamp": latest.name.isoformat(),
                "open": latest["Open"],
                "high": latest["High"],
                "low": latest["Low"],
                "close": latest["Close"],
                "volume": int(latest["Volume"])
            }
            return stock_data
        return None
    
    def produce_stock_data(self):
        self.logger.info(f"Starting to produce real-time stock data every {self.interval} seconds...")
        try:
            while True:
                successful_symbols = 0
                failed_symbols = 0
                
                for symbol in self.current_stocks:
                    try:
                        stock_data = self.generate_stock_data(symbol)
                        if stock_data:
                            message = json.dumps(stock_data)
                            
                            self.producer.produce(
                                topic=self.topic,
                                value=message,
                                key=symbol,
                                callback=self.delivery_report
                            )
                            self.producer.poll(0)
                            successful_symbols += 1
                    except Exception as e:
                        self.logger.error(f"Failed to produce data for {symbol}: {e}")
                        failed_symbols += 1
                self.logger.info(f"Produced data for {successful_symbols} symbols, failed for {failed_symbols} symbols.")
                self.producer.flush()
                time.sleep(self.interval)
        except KeyboardInterrupt:
            self.logger.info("Stream data producer stopped by user.")
        except Exception as e:
            self.logger.error(f"Unexpected error in stream data producer: {e}")
        finally:
            self.producer.flush()
            self.logger.info("Kafka producer flushed and closed.")
            

def main():
    try:
        logger.info("Starting real-time data streaming...")
        collector = StreamDataProducer(
            bootstrap_server=KAFKA_BOOTSTRAP_SERVER,
            topic=KAFKA_TOPIC_REALTIME,
            interval=60
        )
        
        collector.produce_stock_data()
    except Exception as e:
        logger.error(f"Failed to initialize StreamDataProducer: {e}")
        return
    
    
    
    
    
if __name__ == "__main__":
    main()            