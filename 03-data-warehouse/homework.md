Query to create a table:
CREATE OR REPLACE EXTERNAL TABLE `de-zoomcamp-course-458518.module_3_hw.yellow_tripdata_external`
OPTIONS (
  format = 'PARQUET',
  uris = [
    'gs://de-zoomcamp-course-458518-zoomcamp-bucket/parquet/yellow_tripdata_2024-01.parquet',
    'gs://de-zoomcamp-course-458518-zoomcamp-bucket/parquet/yellow_tripdata_2024-02.parquet',
    'gs://de-zoomcamp-course-458518-zoomcamp-bucket/parquet/yellow_tripdata_2024-03.parquet',
    'gs://de-zoomcamp-course-458518-zoomcamp-bucket/parquet/yellow_tripdata_2024-04.parquet',
    'gs://de-zoomcamp-course-458518-zoomcamp-bucket/parquet/yellow_tripdata_2024-05.parquet',
    'gs://de-zoomcamp-course-458518-zoomcamp-bucket/parquet/yellow_tripdata_2024-06.parquet']
);


Create materialized table:
CREATE OR REPLACE TABLE `de-zoomcamp-course-458518.module_3_hw.yellow_tripdata_materialized` AS
SELECT * 
FROM `de-zoomcamp-course-458518.module_3_hw.yellow_tripdata_external`;


