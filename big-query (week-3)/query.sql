-- a Externak table which data stays in gcs and metadata in bigquery
CREATE EXTERNAL TABLE IF NOT EXISTS `integrated-net-411608.datatalk_dataset.external_green_trip_data`
OPTIONS (
  format = "PARQUET",
  uris = [
    'gs://datatalk_storage/feb=2024/green_data_taxi.parquet'
  ]
);

-- a physical table from external table
CREATE OR REPLACE TABLE `integrated-net-411608.datatalk_dataset.green_trip_data`
AS
SELECT * FROM `integrated-net-411608.datatalk_dataset.external_green_trip_data`;

-- Et External table
SELECT COUNT (DISTINCT PULocationID) AS PUlocationID_Count_Et 
  FROM `integrated-net-411608.datatalk_dataset.external_green_trip_data`;
-- Bgt Bigquery table
SELECT COUNT (DISTINCT PULocationID) AS PUlocationID_Count_Bgt 
  FROM `integrated-net-411608.datatalk_dataset.green_trip_data`;

-- Fare_amount = zero , fare_amount_zero
SELECT COUNT(fare_amount) AS fare_amount_zero 
  FROM `integrated-net-411608.datatalk_dataset.external_green_trip_data`
  WHERE fare_amount = 0;

-- create partition and cluster table 
CREATE TABLE IF NOT EXISTS 
  `datatalk_dataset.partition_cluster_green_trip_data`
  PARTITION BY TIMESTAMP_TRUNC(lpep_pickup_datetime, MONTH) 
  -- PARTITION BY lpep_pickup_datetime
  CLUSTER BY PULocationID
  OPTIONS (
    require_partition_filter = TRUE
  )
  AS
  SELECT * FROM `integrated-net-411608.datatalk_dataset.green_trip_data`;


--  Query unpartitioned or clustered table 
SELECT DISTINCT PULocationID FROM `integrated-net-411608.datatalk_dataset.green_trip_data`
WHERE ((lpep_pickup_datetime >= '2022-06-01') AND (lpep_pickup_datetime <= '2022-06-30'));

--  Query partitioned and clustered table 
SELECT DISTINCT PULocationID FROM `integrated-net-411608.datatalk_dataset.partition_cluster_green_trip_data`
WHERE ((lpep_pickup_datetime >= '2022-06-01') AND (lpep_pickup_datetime <= '2022-06-30'));

-- count physical table
SELECT count(*) FROM `integrated-net-411608.datatalk_dataset.green_trip_data`;


