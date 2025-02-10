# slash-analytics-prashulk

## High level Overview
![image](https://github.com/user-attachments/assets/87bc44d0-1b1f-4c7e-963c-38fd7fcdd8ee)


## Project Overview
The `slash_analytics_take_home` project is a data pipeline built using dbt (Data Build Tool) to transform raw financial transactions and entity data into a structured and optimized format for analytics. The project integrates currency conversion rates, cleans data from multiple sources, and ensures efficient querying through indexing strategies.

## Project Structure

### 1. **Staging Models** (`models/staging`)
Staging models clean and standardize raw data before further transformation.

- **`stg_entity.sql`**: Cleans entity data, ensuring required fields (`entity_id`, `subaccount_id`, `account_creation_date`) are present.
- **`stg_cardtransactions.sql`**: Deduplicates transactions based on `TRANSACTION_ID`, assigns validity flags, and retains only the most relevant record.
- **`stg_cardevent.sql`**: Cleans card event data by validating `card_event_id`.

### 2. **Final Models** (`models/final`)
Final models further transform staged data for analysis.

- **`final_entity.sql`**: Filters out invalid entities.
- **`final_cardtransactions.sql`**: Converts currency codes, joins with exchange rate data, and computes the final transaction amount.
- **`final_cardevents.sql`**: Cleans up event types and statuses by removing `pending_` prefixes.
- **`final_currencyconv.sql`**: Stores historical currency conversion rates.

### 3. **Currency Fetch and Conversion Logic**
- The `currency.py` script is used to fetch historical currency exchange rates from an external API (`fxratesapi.com`). The script iterates over a predefined date range (from `2023-01-01` to `2024-12-31`), making daily API calls to retrieve exchange rates.
- The fetched exchange rates are stored in `currency_conv.csv`, which is then loaded into the `final_currencyconv` table via dbt seeds.
- When joining with card transactions, the `final_cardtransactions.sql` model uses `exchange_rt_dt` (the transaction date) to match the corresponding currency conversion rate. This ensures historical transactions are properly converted to a standardized currency based on the currency exchange rate for that specific date for accurate financial analysis.

### 4. **Seeds** (`seeds/`)
Static CSV files loaded into the database for reference.

- **`card_events.csv`**
- **`card_transactions.csv`**
- **`entity.csv`**
- **`currency_conv.csv`** (populated via `currency.py` script)

## dbt Configuration (`dbt_project.yml`)
- **Materialization**: All final tables are materialized as `table`.
- **Indexing**:
  - `final_entity`: Index on `(entity_id, subaccount_id)`
  - `final_cardtransactions`: Index on `transaction_id`
  - `final_cardevents`: Index on `card_event_id`
- **Hooks**: Pre-hooks drop indexes before execution, and post-hooks create them for optimized query performance.

## Running the Project
1. Install dbt and required dependencies.
2. Set up the dbt profile in `profiles.yml`.
3. Run the pipeline:
   ```sh
   dbt seed    # Load seed data
   dbt debug
   dbt run
   dbt test  
   ```
4. Use `currency.py` to fetch and update exchange rates:
   ```sh
   python currency.py
   ```

### IMP NOTE:
The csv files for the 3 tables have not been uploaded due to size constraints, but the currency_conv.csv has been uploaded. Also, a few blank folders may not have been uploaded. The scratchwork.sql file shows the prior analysis done to create final queries for the dashboard
