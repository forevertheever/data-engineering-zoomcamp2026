## Homework Week2

**Q1 & Q2 has no Sql commands.**

A Modificaiton in the yaml file for the following questions:

In the **gcp_taxi_scheduled** YAML file for green and yellow taxi dataset, I added another task after merging the tables, which is to delete the external table that has been created by the task 'bq_yellow/green_table_ext' because these external tables will cause error in BigQuery when i do the iterative selection across all the tables with surfix '{yellow/green}_tripdate_{year}_*'

Q3. How many rows are there for the Yellow Taxi data for all CSV files in the year 2020?

Answer: 24,648,499

Command:
```
SELECT
  COUNT(*) AS total_rows
FROM `data-engineering-2026-484614.zoomcamp.yellow_tripdata_2020_*`
```

Q4. How many rows are there for the Green Taxi data for all CSV files in the year 2020?

Answer: 1,734,051

Command:
```
SELECT
  COUNT(*) AS total_rows
FROM `data-engineering-2026-484614.zoomcamp.green_tripdata_2020_*`
```

Q5. How many rows are there for the Yellow Taxi data for the March 2021 CSV file?

Answer: 1,925,152

Command:
```
SELECT
  COUNT(*) AS total_rows
FROM `data-engineering-2026-484614.zoomcamp.yellow_tripdata_2021_03`
```

- id: delete_green_external_table
  type: io.kestra.plugin.gcp.bigquery.Query
  sql: |
    DROP EXTERNAL TABLE IF EXISTS `{{kv('GCP_PROJECT_ID')}}.{{render(vars.table)}}_ext`;