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

## Project Screenshots

### Azure Data Lake — Storage Containers
![Storage](<img width="1172" height="667" alt="storage-container" src="https://github.com/user-attachments/assets/357e5b99-1f76-4b8f-ba2a-9bcec4dc5f06" />
)

### ADF Pipeline Run — Succeeded
![ADF Pipeline](<img width="1469" height="414" alt="adf_pipeline_success" src="https://github.com/user-attachments/assets/1c55b3bf-b3d3-4c1d-811e-790722fd64a6" />
)

### Databricks — Curated Layer Write Verification
![Databricks](<img width="1470" height="831" alt="databricks-output" src="https://github.com/user-attachments/assets/1cce2c08-d41e-4918-9b18-5b37a4bdb76c" />
)

### Synapse Analytics — Live Query Result
![Synapse](<img width="1470" height="833" alt="synapse_query_result" src="https://github.com/user-attachments/assets/07657730-c615-470b-95ca-b995fe477a25" />
)

### Tableau Dashboard
![Dashboard](<img width="1062" height="836" alt="Sales Analytics Dashboard" src="https://github.com/user-attachments/assets/ac9b5e80-44bb-47f1-84d7-1bdfcbceb83d" />
)

## Author

Sakshi Vikas Jadhav  
[LinkedIn](https://www.linkedin.com/in/sakshi-jadhav)
