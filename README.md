# onechronos-etl-takehome
Take-home challenge for ETL Engineer interview opportunity.

# ETL Engineer
## Coding Take-Home Challenge
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
