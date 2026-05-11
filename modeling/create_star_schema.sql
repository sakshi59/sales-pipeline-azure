-- Sales Pipeline Star Schema
-- Azure Synapse Analytics Serverless SQL Pool

-- Create database
CREATE DATABASE sales_pipeline;

-- Switch to database
USE sales_pipeline;

-- Create master key
CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'Pipeline@2026!';

-- Create credential
CREATE DATABASE SCOPED CREDENTIAL StorageCredential
WITH IDENTITY = 'SHARED ACCESS SIGNATURE',
SECRET = 'YOUR_SAS_TOKEN_HERE';

-- Create external data source
CREATE EXTERNAL DATA SOURCE CuratedStorage
WITH (
    LOCATION = 'https://stgsalespipelinesv.dfs.core.windows.net/curated',
    CREDENTIAL = StorageCredential
);

-- Create Parquet file format
CREATE EXTERNAL FILE FORMAT ParquetFormat
WITH (
    FORMAT_TYPE = PARQUET
);

-- Create fact table
CREATE EXTERNAL TABLE fact_sales (
    Row_ID        INT,
    Order_ID      VARCHAR(20),
    Order_Date    DATE,
    Ship_Date     DATE,
    Customer_ID   VARCHAR(20),
    Product_ID    VARCHAR(20),
    Region        VARCHAR(20),
    Sales         FLOAT,
    Quantity      INT,
    Discount      FLOAT,
    Profit        FLOAT,
    Revenue       FLOAT,
    Profit_Margin FLOAT
)
WITH (
    LOCATION = 'superstore_curated/',
    DATA_SOURCE = CuratedStorage,
    FILE_FORMAT = ParquetFormat
);

-- Create dim_customer
CREATE EXTERNAL TABLE dim_customer (
    Customer_ID   VARCHAR(20),
    Customer_Name VARCHAR(100),
    Segment       VARCHAR(50)
)
WITH (
    LOCATION = 'superstore_curated/',
    DATA_SOURCE = CuratedStorage,
    FILE_FORMAT = ParquetFormat
);

-- Create dim_product
CREATE EXTERNAL TABLE dim_product (
    Product_ID    VARCHAR(50),
    Product_Name  VARCHAR(200),
    Category      VARCHAR(50),
    Sub_Category  VARCHAR(50)
)
WITH (
    LOCATION = 'superstore_curated/',
    DATA_SOURCE = CuratedStorage,
    FILE_FORMAT = ParquetFormat
);

-- Create dim_region
CREATE EXTERNAL TABLE dim_region (
    Region  VARCHAR(20),
    State   VARCHAR(50),
    City    VARCHAR(50),
    Country VARCHAR(50)
)
WITH (
    LOCATION = 'superstore_curated/',
    DATA_SOURCE = CuratedStorage,
    FILE_FORMAT = ParquetFormat
);
