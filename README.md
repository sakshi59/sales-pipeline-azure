# Sales Analytics Pipeline

An end-to-end data engineering pipeline built on Microsoft Azure. Covers the full workflow from raw data ingestion to an interactive business dashboard — designed to reflect production pipeline patterns used in enterprise environments.

## What This Project Does

Takes raw retail sales data and moves it through a structured pipeline:

1. Raw CSV lands in Azure Data Lake (bronze layer)
2. Azure Data Factory copies and stages the data
3. Databricks cleans and transforms it using PySpark (silver layer)
4. Validated data is modeled into fact and dimension tables (gold layer)
5. Synapse Analytics serves as the data warehouse
6. Tableau dashboard surfaces revenue, regional, and product KPIs

## Stack

- **Azure Data Lake Storage Gen2** — layered storage (raw / processed / curated)
- **Azure Data Factory** — pipeline orchestration
- **Azure Databricks** — PySpark transformations
- **Azure Synapse Analytics** — data warehouse
- **Tableau Desktop** — reporting layer
- **SQL** — star schema modeling

## Dataset

Superstore retail sales — 9,994 transactions, 21 columns,
4 US regions, 2014–2017. Simulates transactional data exported
from a retail point-of-sale system.

## Author

Sakshi Vikas Jadhav  
[LinkedIn](https://www.linkedin.com/in/sakshi-jadhav)
