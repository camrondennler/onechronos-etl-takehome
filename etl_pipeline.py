"""
ETL Pipeline for Processing Simulated Trade Data
"""

import logging
import yaml
from pyspark.sql import SparkSession
from pyspark.sql.functions import col


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
        
        self.metrics = {
            "processed_trades": 0,
            "successful_trades": 0,
            "cancelled_trades": 0,
            "duplicate_trades": 0,
            "invalid_trades": 0,
        }
        
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
            .csv("counterparty_fills.csv") \
            .select( \
                col("external_ref_id").alias("external_ref_id"), \
                col("our_trade_id").alias("our_trade_id"), \
                col("timestamp").alias("counterparty_timestamp"), \
                col("symbol").alias("counterparty_symbol"), \
                col("quantity").alias("counterparty_quantity"), \
                col("price").alias("counterparty_price"), \
                col("counterparty_id").alias("counterparty_id") \
            )
        
        symbols_df = self.spark.read \
            .option("header", "true") \
            .csv("symbols_reference.csv")
        
        # Collect and log initial counts
        trades_count = trades_df.count()
        counterparty_count = counterparty_df.count()
        symbols_count = symbols_df.count()
        
        self.metrics["processed_trades"] = trades_count
        self.logger.info(f"Extracted {trades_count} trades")
        self.logger.info(f"Extracted {counterparty_count} counterparty fills")
        self.logger.info(f"Extracted {symbols_count} reference symbols")
        
        return trades_df, counterparty_df, symbols_df

    def filter_duplicate_and_cancelled(self, trades_df):
        # Track count of enriched_trades_df; update as we filter and validate.
        filtered_trades_df = trades_df
        current_count = self.metrics["processed_trades"]

        # Isolate duplicate trades.
        without_duplicate_trades_df = filtered_trades_df.dropDuplicates(["trade_id"])
        without_duplicate_trades_count = without_duplicate_trades_df.count()
        duplicate_count = current_count - without_duplicate_trades_count

        # Save and log duplicate metric.
        self.metrics["duplicate_trades"] += duplicate_count
        self.logger.info(f"Identified {duplicate_count} duplicated trades")

        # Filter duplicate trades based on config.
        if self.config["data_quality"]["filter_duplicates"]:
            filtered_trades_df = without_duplicate_trades_df
            self.logger.info(f"Removed {duplicate_count} duplicated trades")
            current_count -= duplicate_count
        
        # Isolate cancelled trades.
        without_cancelled_trades_df = filtered_trades_df.filter(col("trade_status") != "CANCELLED")
        without_cancelled_trades_count = without_cancelled_trades_df.count()
        cancelled_count = current_count - without_cancelled_trades_count

        # Save and log cancelled metric.
        self.metrics["cancelled_trades"] += cancelled_count
        self.logger.info(f"Identified {cancelled_count} cancelled trades")

        # Filter cancelled trades based on config.
        if self.config["data_quality"]["filter_cancelled_trades"]:
            filtered_trades_df = without_cancelled_trades_df
            self.logger.info(f"Removed {cancelled_count} cancelled trades")
            current_count -= cancelled_count
        
        return filtered_trades_df, current_count
    
    def transform(self, trades_df, counterparty_df, symbols_df):
        # Run all transform steps on extracted data

        # Identify and filter duplicate/cancelled trades.
        filtered_trades_df, filtered_trades_count = self.filter_duplicate_and_cancelled(trades_df)
        
        # Join remaining trades with symbols and counterparty fills to create an enriched dataframe.
        enriched_trades_df = trades_df \
            .join(counterparty_df, trades_df["trade_id"] == counterparty_df["our_trade_id"], "left") \
            .join(symbols_df, "symbol", "left")

        return enriched_trades_df, None
    
    def run(self):
        # Execute the complete ETL pipeline
        
        try:
            self.logger.info("Starting ETL Pipeline execution...")
            
            # Extract
            trades_df, counterparty_df, symbols_df = self.extract()
            
            # Transform
            valid_trades_df, invalid_trades_df = self.transform(trades_df, counterparty_df, symbols_df)
            
            self.logger.info(
                "ETL Pipeline completed successfully! Metrics: %s",
                self.metrics,
            )
            
        except Exception as e:
            self.logger.error(f"ETL Pipeline failed with error: {str(e)}", exc_info=True)
            raise
        finally:
            self.spark.stop()


if __name__ == "__main__":
    pipeline = ETLPipeline()
    pipeline.run()
