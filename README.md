# dbt_repo

### dbt Bitcoin Analytics Pipeline

This repository contains a dbt project for analyzing a public blockchain dataset. The pipeline extracts, transforms, and loads (ETL) raw blockchain transaction data into a clean, queryable data mart.<br>

The core of this project focuses on:

- <b>Cost Optimization</b>: Using techniques like table partitioning and selective column retrieval to minimize BigQuery costs.
- <b>Data Transformation</b>: Unnesting complex nested data structures (inputs, outputs) and transforming raw transaction data into a final balances table.
- <b>Business Logic</b>: Applying business rules, such as excluding all addresses that have ever been involved in a Coinbase transaction.

### Prerequisites

Before you can run this dbt project, ensure you have the following installed:

- **Python**: Version 3.9 or higher
- **dbt-core**: The dbt command-line tool.
- **dbt-bigquery**: The BigQuery adapter for dbt.
- **Google Cloud SDK**: To manage authentication with your GCP account.

### Usage

Follow these steps to set up and run the dbt project:

1.  **Clone the Repository**:

    ```bash
    git clone https://github.com/NoobToDaMaximum/dbt_repo.git
    cd dbt_repo
    ```

2.  **Install dbt Dependencies**:

    ```bash
    pip install -r requirements.txt
    ```

3.  **Configure Your `profiles.yml`**:
    dbt needs a `profiles.yml` file to connect to your BigQuery project. You can find your `profiles.yml` at `~/.dbt/profiles.yml`. Configure it to match your project details, using the service account key you set up with Terraform. A basic template is included in the GitHub Actions workflow.

4.  **Run dbt**:
    Once configured, you can test your connection and run your models:
    ```bash
    dbt debug
    dbt run
    ```

### Project Structure

- `dbt_project.yml`: The main configuration file for the dbt project. It defines project settings, profiles, and schema configurations.
- `models/`: Contains the SQL models for the data pipeline, organized into two layers:

  - `staging/`: Holds models that clean and prepare the raw data. The `stg_transactions.sql` model is a cost-optimized staging table for the raw blockchain data.
  - `marts/`: Holds the final, business-ready data. The `dim_balances.sql` model aggregates the data to provide the final balance for each address.

- `dbt-ci.yml`: A GitHub Actions workflow that automates the dbt pipeline, authenticating with Google Cloud and running the models on every push to the main branch.

### Code Explanation

- `stg_transactions.sql`: This model filters the massive public dataset down to the last three months of data. It is partitioned by block_timestamp and only retrieves the necessary columns to ensure cost efficiency.
- `dim_balances.sql`: This model joins the inputs and outputs from the staging model to calculate a total balance for each address. It contains the business logic to exclude any address that has participated in a coinbase transaction, providing a clean and accurate final table for analysis.
