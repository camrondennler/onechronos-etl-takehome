"""
ETL Pipeline for Processing Simulated Trade Data
"""

import logging
import yaml
from pyspark.sql import SparkSession


class ETLPipeline:
    # Main ETL Pipeline class for processing trade data
    
    def __init__(self, config_path: str = "config.yaml"):
        # Initialize the ETL pipeline with configuration

        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        # Set up logging
        logging.basicConfig(
            level=getattr(logging, self.config['logging']['level']),
            format=self.config['logging']['format']
        )
        self.logger = logging.getLogger(__name__)
        
        # Initialize Spark session
        self.spark = SparkSession.builder \
            .appName("SimulatedTradeETLPipeline") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .getOrCreate()
        
        self.logger.info("ETL Pipeline initialized")
    
    def extract(self):   
        # Extract data from CSV files

        self.logger.info("Starting data extraction...")
        
        # Read CSV files
        trades_df = self.spark.read \
            .option("header", "true") \
            .csv("trades.csv")
        
        counterparty_df = self.spark.read \
            .option("header", "true") \
            .csv("counterparty_fills.csv")
        
        symbols_df = self.spark.read \
            .option("header", "true") \
            .csv("symbols_reference.csv")
        
        # Collect and log initial counts
        trades_count = trades_df.count()
        counterparty_count = counterparty_df.count()
        symbols_count = symbols_df.count()
        
        self.logger.info(f"Extracted {trades_count} trades")
        self.logger.info(f"Extracted {counterparty_count} counterparty fills")
        self.logger.info(f"Extracted {symbols_count} reference symbols")
        
        return trades_df, counterparty_df, symbols_df
    
    def run(self):
        # Execute the complete ETL pipeline
        
        try:
            self.logger.info("Starting ETL Pipeline execution...")
            
            # Extract
            trades_df, counterparty_df, symbols_df = self.extract()
            
            self.logger.info("ETL Pipeline completed successfully!")
            
        except Exception as e:
            self.logger.error(f"ETL Pipeline failed with error: {str(e)}", exc_info=True)
            raise
        finally:
            self.spark.stop()


if __name__ == "__main__":
    pipeline = ETLPipeline()
    pipeline.run()
