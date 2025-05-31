# Fraud Detection

## Stream Processing

  1. Install python version 3 or above
  2. Pip install google.cloud
  3. Upload google credentials (Pub/Sub Producer)
  4. Create Pub/Sub topics and dataset Google Big Query
  5. Determine the schema of the dataset in Google Pub/Sub, Google Big Query
  6  Run the code
 
## Batch Processing

  1. Setup the Google Composer
  2. Created schema of dataset in dags
  3. Run the code

## Transformation (with dbt)
  
  1. Start your dbt cloud/dbt core
  2. Use the models in "Transformation with dbt" folders
  3. Connect your main table in bigquery to ```source_table.sql``` model
  4. In ```tidying_the_columns.sql``` model, you can specify columns order and data types to your liking
  5. Do some transformation. In this repo we create partitioning/clustering in ```final_project_partitioned.sql``` and denormalizing in ```final_project_nested.sql``` to compare their performace in bigquery
  
  The lineage should look like this :
  ![](https://github.com/mhaniffajari/IYKRA_Fraud_Detection/blob/main/Transformation%20with%20dbt/lineage_graph.png)
