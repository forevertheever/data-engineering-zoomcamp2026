## Homework Week3

# Setup in Bigquery:

* Create external table of yellow taxi 2024 data
```
CREATE OR REPLACE EXTERNAL TABLE `data-engineering-2026-484614.zoomcamp.yellow_tripdata_2024`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://bucket-date-engineering-2026/yellow_tripdata_2024-*.parquet']
);
```

* Create regular/materialized table 
```
CREATE OR REPLACE TABLE `data-engineering-2026-484614.zoomcamp.yellow_tripdata_2024_native`
AS
SELECT *
FROM `data-engineering-2026-484614.zoomcamp.yellow_tripdata_2024`;
```

Question 1. Counting records

It can be checked in the table details. It shows **20,332,093** rows.

Question 2. Data read estimation

Count unique PULocationID in the external table, this will process 0B
```
SELECT
  COUNT(DISTINCT PULocationID) AS distinct_pu_locations
FROM `data-engineering-2026-484614.zoomcamp.yellow_tripdata_2024`;
```

Count unique PULocationID in the table, this will process 155.12MB
```
SELECT
  COUNT(DISTINCT PULocationID) AS distinct_pu_locations
FROM `data-engineering-2026-484614.zoomcamp.yellow_tripdata_2024_native`;
```

Question 3. Understanding columnar storage

Retrieve PULocationID in the table, this will process **155.12MB**
```
SELECT
  PULocationID
FROM `data-engineering-2026-484614.zoomcamp.yellow_tripdata_2024_native`;
```

Retrieve PULocationID, DOLocationID in the table, this will process **310.24MB**
```
SELECT
  PULocationID, DOLocationID
FROM `data-engineering-2026-484614.zoomcamp.yellow_tripdata_2024_native`;
```

Question 4. Counting zero fare trips

Counting zero fare trips, result is **8333**
```
SELECT
  COUNT(*) AS zero_fare_records
FROM `data-engineering-2026-484614.zoomcamp.yellow_tripdata_2024`
WHERE fare_amount = 0;
```

Question 5. Partitioning and clustering

Partition by tpep_dropoff_datetime and Cluster on VendorID.

Create a table partioned by the tpep_dropoff_datetime:

```
CREATE OR REPLACE TABLE `data-engineering-2026-484614.zoomcamp.yellow_tripdata_partitioned`
PARTITION BY DATE(tpep_dropoff_datetime)
AS
SELECT *
FROM `data-engineering-2026-484614.zoomcamp.yellow_tripdata_2024_native`;
```

Question 6. Partition benefits

Retrieve unique VendorID on the partitioned table within dropoff_date between 2024-03-01 and 2024-03-15, it processes **26.84 MB**
```
SELECT DISTINCT
  VendorID
FROM `data-engineering-2026-484614.zoomcamp.yellow_tripdata_partitioned`
WHERE tpep_dropoff_datetime >= '2024-03-01'
  AND tpep_dropoff_datetime < '2024-03-16';
```

 Retrieve unique VendorID on the unpartitioned table within dropoff_date between 2024-03-01 and 2024-03-15, it processes **310.24 MB**
```
SELET DISTINCT
  VendorID
FROM `data-engineering-2026-484614.zoomcamp.yellow_tripdata_2024_native`
WHERE tpep_dropoff_datetime >= '2024-03-01'
  AND tpep_dropoff_datetime < '2024-03-16';
```

Question 7. External table storage

From **GCP Bucket**

Question 8. Clustering best practices

It is best practice in Big Query to always cluster your data: False

Question 9. Understanding table scans

This command shows processing 0 Byte because Bigquery is a columnar storage. It will read the row numbers from metadata instead of scanning any columns in the table.

```
SELECT count(*) FROM `data-engineering-2026-484614.zoomcamp.yellow_tripdata_2024_native`;
```