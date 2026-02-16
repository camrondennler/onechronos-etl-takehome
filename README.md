# onechronos-etl-takehome
Take-home challenge for ETL Engineer interview opportunity.

## Installation and Setup

### Prerequisites
- Python 3.8 or higher
- Java 21 (required for PySpark and Hadoop compatibility)

### Installing Java 21 and Setting `JAVA_HOME`

On macOS using Homebrew:

```bash
brew install openjdk@21

# Add the following to your shell profile (e.g. ~/.zshrc or ~/.bashrc)
echo 'export JAVA_HOME="$(/usr/libexec/java_home -v 21)"' >> ~/.zshrc
echo 'export PATH="$JAVA_HOME/bin:$PATH"' >> ~/.zshrc

# Reload your shell
source ~/.zshrc
```

On Linux (using a Debian/Ubuntu-based distribution):

```bash
sudo apt-get update
sudo apt-get install -y openjdk-21-jdk

echo 'export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64' >> ~/.bashrc
echo 'export PATH="$JAVA_HOME/bin:$PATH"' >> ~/.bashrc

source ~/.bashrc
```

On Windows (PowerShell, after installing a Java 21 JDK such as Temurin):

1. Install a Java 21 JDK (e.g. Temurin 21) from the vendor's website.
2. Set environment variables:
   - `JAVA_HOME` to the JDK install directory (e.g. `C:\Program Files\Eclipse Adoptium\jdk-21.x.x`)
   - Add `%JAVA_HOME%\bin` to the `Path` environment variable.

You can verify the installation with:

```bash
java -version
echo $JAVA_HOME  # on macOS/Linux
```

### Setup Instructions

1. **Create a Python Virtual Environment**
   ```bash
   python3 -m venv venv
   ```

2. **Activate the Virtual Environment**
   
   On macOS/Linux:
   ```bash
   source venv/bin/activate
   ```
   
   On Windows:
   ```bash
   venv\Scripts\activate
   ```

3. **Install Dependencies**
   ```bash
   pip install -r requirements.txt
   ```

### Running the Pipeline

Once the virtual environment is activated and dependencies are installed, run the ETL pipeline:

```bash
python etl_pipeline.py
```

The pipeline will:
- Read configuration from `config.yaml`
- Process the input CSV files (`trades.csv`, `counterparty_fills.csv`, `symbols_reference.csv`)
- Generate output files (`cleaned_trades.json`, `exceptions_report.json`)

### Deactivating the Virtual Environment

When you're done working, deactivate the virtual environment:

```bash
deactivate
```

# Initially Provided Requirements
## ETL Engineer Coding Take-Home Challenge
### Background
Our dark pool exchange processes millions of trades daily. Trade data flows from multiple sources (internal matching engine, regulatory feeds, counterparty systems) and must be cleaned, validated, transformed, and loaded into our data warehouse for compliance reporting, analytics, and reconciliation.

### The Challenge
Build an ETL pipeline that processes simulated trade data with realistic data quality issues.

### Input Data
You'll receive three CSV files:

trades.csv - Raw trade executions from our matching engine
- trade_id, timestamp, symbol, quantity, price, buyer_id, seller_id, trade_status

counterparty_fills.csv - Trade confirmations from external counterparties (may have discrepancies)
- external_ref_id, our_trade_id, timestamp, symbol, quantity, price, counterparty_id

symbols_reference.csv - Valid trading symbols and metadata
- symbol, company_name, sector, is_active

### Data Quality Issues (Intentionally Embedded)
Address all data quality issues, including but not limited to duplicates, data normalization, and data types

### Requirements
Core Functionality
1. Extract: Read all three CSV files
2. Transform:
    - Validate symbols against reference data
    - Flag discrepancies between our trades and counterparty fills (>$0.01 price difference or quantity mismatch)
    - Filter cancelled trades
    - Round prices to 2 decimal places
    - Address all other data quality issues
3. Load: Output two files:
    - cleaned_trades.json - Validated, cleaned trades
    - exceptions_report.json - All records that failed validation with reasons
###  Output Schema
cleaned_trades.json:

json
```
{
  "trade_id": "string",
  "timestamp_utc": "ISO 8601 string",
  "symbol": "string",
  "quantity": "integer",
  "price": "decimal (2 places)",
  "buyer_id": "string",
  "seller_id": "string",
  "counterparty_confirmed": "boolean",
  "discrepancy_flag": "boolean"
}
```

exceptions_report.json:

json
```
{
  "record_id": "string",
  "source_file": "string",
  "exception_type": "string",
  "details": "string",
  "raw_data": "object"
}
```
### Constraints
- Use any coding language
- Should complete in under 3 hours
- Include a requirements.txt or similar for dependencies
- Submit as a Git repository with clear commit history
- Add configurable validation rules (e.g., via YAML config)
- Include observability (logging, metrics on records processed/failed)


Time Limit: 4 hours

Questions: You may email clarifying questions, but we encourage you to make reasonable assumptions and document them.
