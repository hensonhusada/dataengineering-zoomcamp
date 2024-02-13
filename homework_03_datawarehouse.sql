CREATE OR REPLACE EXTERNAL TABLE `dataengineering_dataset_henson.nyc_green_taxi_data`
OPTIONS (
  format = 'parquet',
  uris = ['gs://dataengineering-bucket-henson/nyc_green_taxi_data/lpep_pickup_date=*']
);

CREATE OR REPLACE TABLE `dataengineering_dataset_henson.nyc_green_taxi_data_nonpartition`
AS SELECT * FROM `dataengineering_dataset_henson.nyc_green_taxi_data`;

select * from `dataengineering_dataset_henson.nyc_green_taxi_data`

select * from `dataengineering_dataset_henson.nyc_green_taxi_data_nonpartition`

select count(1) from `dataengineering_dataset_henson.nyc_green_taxi_data`
where fare_amount = 0
