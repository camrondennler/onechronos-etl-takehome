"""
ETL Pipeline for Processing Simulated Trade Data
"""

import logging
import json
import yaml
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, abs as spark_abs, when, concat_ws, lit, regexp_replace, round as spark_round, date_format, to_timestamp, struct, trim


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
        
        # Initialize Spark session (local only, no network)
        self.spark = (
            SparkSession.builder
            .appName("SimulatedTradeETLPipeline")
            .master("local[*]")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.driver.host", "localhost")
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .getOrCreate()
        )
        
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

    def validate_trades(self, enriched_trades_df):
        """
        Validate enriched trades:
        - Symbol must exist in reference data and be active
        - Trade quantity and price must be positive and castable to numeric types
        - Compute basic discrepancy flags vs counterparty fills
        """
        
        self.logger.info("Validating enriched trades...")

        # Cast numeric fields for validation and discrepancy checks
        df = (
            enriched_trades_df
            .withColumn("trade_quantity_int", col("quantity").cast("int"))
            .withColumn("trade_price_dec", col("price").cast("double"))
            .withColumn("cp_quantity_int", col("counterparty_quantity").cast("int"))
            .withColumn("cp_price_dec", col("counterparty_price").cast("double"))
        )

        # Identify specific validation failures
        symbol_invalid = (
            col("company_name").isNull()
            | (
                (col("is_active") != "true")
                & (col("is_active") != True)
            )
        )
        
        quantity_invalid = (
            col("trade_quantity_int").isNull()
            | (col("trade_quantity_int") <= 0)
        )
        
        price_invalid = (
            col("trade_price_dec").isNull()
            | (col("trade_price_dec") <= 0)
        )

        quantity_mismatch = (
            col("trade_price_dec").isNull()
            | (col("trade_price_dec") <= 0)
        )

        # Build exception_type and details columns
        # Symbol validation
        symbol_exception_type = when(symbol_invalid, lit("INVALID_SYMBOL")).otherwise(lit(""))
        symbol_details = (
            when(
                col("company_name").isNull(),
                lit("Symbol not found in reference data")
            ).when(
                (col("is_active") != "true") & (col("is_active") != True),
                lit("Symbol is inactive")
            ).otherwise(lit(""))
        )
        
        # Quantity validation
        quantity_exception_type = when(quantity_invalid, lit("INVALID_QUANTITY")).otherwise(lit(""))
        quantity_details = (
            when(
                col("trade_quantity_int").isNull(),
                lit("Quantity is null or not numeric")
            ).when(
                col("trade_quantity_int") <= 0,
                lit("Quantity must be positive")
            ).otherwise(lit(""))
        )
        
        # Price validation
        price_exception_type = when(price_invalid, lit("INVALID_PRICE")).otherwise(lit(""))
        price_details = (
            when(
                col("trade_price_dec").isNull(),
                lit("Price is null or not numeric")
            ).when(
                col("trade_price_dec") <= 0,
                lit("Price must be positive")
            ).otherwise(lit(""))
        )

        # Combine exception types and details, then clean up leading/trailing separators
        df = df.withColumn(
            "exception_type",
            trim(
                regexp_replace(
                    regexp_replace(
                        concat_ws(",", symbol_exception_type, quantity_exception_type, price_exception_type),
                        "^[,\\s]+|[,\\s]+$", ""
                    ),
                    ",,+", ","
                )
            )
        ).withColumn(
            "details",
            trim(
                regexp_replace(
                    regexp_replace(
                        concat_ws("; ", symbol_details, quantity_details, price_details),
                        "^[;,\\s]+|[;,\\s]+$", ""
                    ),
                    "; +;+", "; "
                )
            )
        )

        # Counterparty confirmation and price / quantity discrepancies
        threshold = float(self.config["validation"]["price_discrepancy_threshold_exclusive"])

        df = df.withColumn(
            "counterparty_confirmed",
            col("cp_quantity_int").isNotNull() | col("cp_price_dec").isNotNull(),
        ).withColumn(
            "discrepancy_flag",
            (
                col("counterparty_confirmed")
                & (
                    (col("cp_quantity_int").isNotNull()
                     & (col("cp_quantity_int") != col("trade_quantity_int")))
                    | (
                        col("cp_price_dec").isNotNull()
                        & (spark_abs(col("cp_price_dec") - col("trade_price_dec")) > threshold)
                    )
                )
            ),
        )

        # Overall validity: symbol and trade numerics must be valid
        is_valid = ~symbol_invalid & ~quantity_invalid & ~price_invalid

        valid_trades_df = df.filter(is_valid)
        invalid_trades_df = df.filter(~is_valid)

        valid_count = valid_trades_df.count()
        invalid_count = invalid_trades_df.count()

        self.metrics["successful_trades"] += valid_count
        self.metrics["invalid_trades"] += invalid_count

        self.logger.info(f"Validated trades: {valid_count} valid, {invalid_count} invalid")

        return valid_trades_df, invalid_trades_df
    
    def clean_trades(self, valid_trades_df, invalid_trades_df):
        """
        Clean and format trades DataFrames to match expected output schemas.
        
        For valid trades: Format to match cleaned_trades.json schema
        For invalid trades: Keep exception_type and details for exceptions_report.json
        """
        self.logger.info("Cleaning trades to match output schemas...")
        
        # Format valid trades to match cleaned_trades.json schema
        # Normalize timestamp if needed (handle various formats), then format to ISO 8601
        cleaned_valid_trades_df = valid_trades_df.withColumn(
            "timestamp_normalized",
            when(
                col("timestamp").rlike(r'^\d{4}-\d{2}-\d{2}T'),
                to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
            ).when(
                col("timestamp").rlike(r'^\d{10}$'),
                to_timestamp(col("timestamp").cast("long"))
            ).when(
                # Handle M/d/yyyy H:mm:ss or M/d/yyyy HH:mm:ss (single or double digit hours)
                col("timestamp").rlike(r'^\d{1,2}/\d{1,2}/\d{4} \d{1,2}:\d{2}:\d{2}'),
                to_timestamp(col("timestamp"), "M/d/yyyy H:mm:ss")
            ).otherwise(
                to_timestamp(col("timestamp"))
            )
        ).select(
            col("trade_id").alias("trade_id"),
            # Format timestamp to ISO 8601 string
            date_format(col("timestamp_normalized"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").alias("timestamp_utc"),
            col("symbol").alias("symbol"),
            # Use validated integer quantity
            col("trade_quantity_int").alias("quantity"),
            # Round price to configured decimal places
            spark_round(
                col("trade_price_dec"),
                self.config["validation"]["price_decimal_places"],
            ).alias("price"),
            col("buyer_id").alias("buyer_id"),
            col("seller_id").alias("seller_id"),
            col("counterparty_confirmed").alias("counterparty_confirmed"),
            col("discrepancy_flag").alias("discrepancy_flag")
        )
        
        # For invalid trades, keep exception_type and details for exceptions report
        # We'll format this properly when building the exceptions report
        cleaned_invalid_trades_df = invalid_trades_df
        
        valid_count = cleaned_valid_trades_df.count()
        invalid_count = cleaned_invalid_trades_df.count()
        
        self.logger.info(f"Cleaned {valid_count} valid trades and {invalid_count} invalid trades")
        
        return cleaned_valid_trades_df, cleaned_invalid_trades_df
    
    def transform(self, trades_df, counterparty_df, symbols_df):
        # Run all transform steps on extracted data

        # Identify and filter duplicate/cancelled trades.
        filtered_trades_df, filtered_trades_count = self.filter_duplicate_and_cancelled(trades_df)
        
        # Join remaining trades with symbols and counterparty fills to create an enriched dataframe.
        self.logger.info("Enriching trades with symbol and counterparty data...")
        enriched_trades_df = filtered_trades_df \
            .join(counterparty_df, filtered_trades_df["trade_id"] == counterparty_df["our_trade_id"], "left") \
            .join(symbols_df, "symbol", "left")
        self.logger.info("Enrichment complete.")

        # Validate remaining trades against existing symbols and with quantity/price accuracy.
        valid_trades_df, invalid_trades_df = self.validate_trades(enriched_trades_df)

        # Clean trades dataframes to match desired output schema.
        cleaned_valid_trades_df, cleaned_invalid_trades_df = self.clean_trades(valid_trades_df, invalid_trades_df)

        return cleaned_valid_trades_df, cleaned_invalid_trades_df
    
    def load(self, valid_trades_df, invalid_trades_df):
        """
        Load cleaned trades and exceptions to output files.
        
        Writes:
        - valid_trades_df to cleaned_trades.json (matches cleaned_trades.json schema)
        - invalid_trades_df to exceptions_report.json (matches exceptions_report.json schema)
        """
        self.logger.info("Loading output files...")

        # Helper: write a DataFrame as a single pretty-printed JSON file
        def _write_single_json(df, output_path: str) -> int:
            records = [json.loads(r) for r in df.toJSON().collect()]
            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(records, f, indent=2)
            return len(records)
        
        # Write cleaned trades as a single JSON file
        cleaned_trades_path = self.config["output"]["cleaned_trades_path"]
        valid_count = valid_trades_df.count()
        
        if valid_count > 0:
            written_valid = _write_single_json(valid_trades_df, cleaned_trades_path)
            self.logger.info(f"Wrote {written_valid} cleaned trades to {cleaned_trades_path}")
        else:
            self.logger.warning(f"No valid trades to write to {cleaned_trades_path}")
        
        # Format invalid trades to match exceptions_report.json schema
        invalid_count = invalid_trades_df.count()
        
        if invalid_count > 0:
            exceptions_df = invalid_trades_df.select(
                col("trade_id").alias("record_id"),
                lit("trades.csv").alias("source_file"),
                col("exception_type").alias("exception_type"),
                col("details").alias("details"),
                # Create raw_data object with original trade fields
                struct(
                    col("trade_id"),
                    col("timestamp"),
                    col("symbol"),
                    col("quantity"),
                    col("price"),
                    col("buyer_id"),
                    col("seller_id"),
                    col("trade_status"),
                ).alias("raw_data"),
            )
            
            exceptions_report_path = self.config["output"]["exceptions_report_path"]
            written_invalid = _write_single_json(exceptions_df, exceptions_report_path)
            self.logger.info(f"Wrote {written_invalid} exceptions to {exceptions_report_path}")
        else:
            self.logger.info("No exceptions to write - all trades passed validation")
    
    def run(self):
        # Execute the complete ETL pipeline
        
        try:
            self.logger.info("Starting ETL Pipeline execution...")
            
            # Extract
            trades_df, counterparty_df, symbols_df = self.extract()
            
            # Transform
            valid_trades_df, invalid_trades_df = self.transform(trades_df, counterparty_df, symbols_df)

            # Load
            self.load(valid_trades_df, invalid_trades_df)
            
            self.logger.info(
                "ETL Pipeline completed successfully. Metrics: %s",
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
