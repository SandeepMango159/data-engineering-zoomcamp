# Converts green csv files to parquet with schema to match dbt model
from pathlib import Path
import pandas as pd

# Define input/output directories
input_dir = Path("C:/Sandeep SSD/Programming SSD/Data Engineering Zoomcamp/data-engineering-zoomcamp/Data/data/csv/green")
output_dir = Path("C:/Sandeep SSD/Programming SSD/Data Engineering Zoomcamp/data-engineering-zoomcamp/Data/data/csv/green/parquet")
output_dir.mkdir(parents=True, exist_ok=True)

# Conversion function for green taxi data
def convert_green_csv_to_parquet(csv_gz_path):
    df = pd.read_csv(csv_gz_path, compression="gzip")
    
    df["VendorID"]              = pd.to_numeric(df["VendorID"], errors="coerce").astype("Int64")

    df["lpep_pickup_datetime"]  = pd.to_datetime(df["lpep_pickup_datetime"],  errors="coerce").astype(str)
    df["lpep_dropoff_datetime"] = pd.to_datetime(df["lpep_dropoff_datetime"], errors="coerce").astype(str)

    df["store_and_fwd_flag"]    = df["store_and_fwd_flag"].astype("string")

    df["RatecodeID"]            = pd.to_numeric(df["RatecodeID"], errors="coerce").astype("Int64")
    df["PULocationID"]          = pd.to_numeric(df["PULocationID"], errors="coerce").astype("Int64")
    df["DOLocationID"]          = pd.to_numeric(df["DOLocationID"], errors="coerce").astype("Int64")

    df["passenger_count"]       = pd.to_numeric(df["passenger_count"], errors="coerce").astype("Int64")
    df["trip_distance"]         = pd.to_numeric(df["trip_distance"], errors="coerce")

    df["fare_amount"]           = pd.to_numeric(df["fare_amount"], errors="coerce")
    df["extra"]                 = pd.to_numeric(df["extra"], errors="coerce")
    df["mta_tax"]               = pd.to_numeric(df["mta_tax"], errors="coerce")
    df["tip_amount"]            = pd.to_numeric(df["tip_amount"], errors="coerce")
    df["tolls_amount"]          = pd.to_numeric(df["tolls_amount"], errors="coerce")
    df["ehail_fee"]             = pd.to_numeric(df.get("ehail_fee"), errors="coerce")
    df["improvement_surcharge"] = pd.to_numeric(df["improvement_surcharge"], errors="coerce")
    df["total_amount"]          = pd.to_numeric(df["total_amount"], errors="coerce")

    df["payment_type"]          = pd.to_numeric(df["payment_type"], errors="coerce").astype("Int64")
    df["trip_type"]             = pd.to_numeric(df.get("trip_type"), errors="coerce").astype("Int64")
    df["congestion_surcharge"] = pd.to_numeric(df["congestion_surcharge"], errors="coerce")

    # Save to Parquet
    output_path = output_dir / csv_gz_path.with_suffix('').with_suffix('.parquet').name
    df.to_parquet(output_path, engine="pyarrow", index=False)
    print(f"✔ Converted: {csv_gz_path.name} → {output_path.name}")

# Process all .csv.gz files
for csv_gz in input_dir.glob("green_tripdata_*.csv.gz"):
    convert_green_csv_to_parquet(csv_gz)