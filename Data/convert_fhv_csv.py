import pandas as pd
from pathlib import Path

# Update these paths
input_dir = Path("C:/Sandeep SSD/Programming SSD/Data Engineering Zoomcamp/data-engineering-zoomcamp/Data/data/csv/fhv")
output_dir = Path("C:/Sandeep SSD/Programming SSD/Data Engineering Zoomcamp/data-engineering-zoomcamp/Data/data/csv/fhv/parquet")
output_dir.mkdir(parents=True, exist_ok=True)

def convert_file(csv_gz_path):
    df = pd.read_csv(csv_gz_path, compression='gzip')

    # Fix schema
    df['PUlocationID'] = pd.to_numeric(df.get('PUlocationID'), errors='coerce').astype('Int64')
    df['DOlocationID'] = pd.to_numeric(df.get('DOlocationID'), errors='coerce').astype('Int64')

    # Fix datetime: convert to datetime and then to ISO string for BigQuery compatibility
    df['pickup_datetime'] = pd.to_datetime(df.get('pickup_datetime'), errors='coerce').astype(str)
    df['dropOff_datetime'] = pd.to_datetime(df.get('dropOff_datetime'), errors='coerce').astype(str)

    df['dispatching_base_num'] = df['dispatching_base_num'].astype('string')
    df['SR_Flag'] = pd.to_numeric(df['SR_Flag'], errors='coerce')  # Already float64
    df['Affiliated_base_number'] = df['Affiliated_base_number'].astype('string')

    # Output path
    output_path = output_dir / csv_gz_path.with_suffix('').with_suffix('.parquet').name
    df.to_parquet(output_path, engine='pyarrow', index=False)
    print(f"✔ Converted: {csv_gz_path.name} → {output_path.name}")

# Process all .csv.gz files
for csv_gz in input_dir.glob("fhv_tripdata_*.csv.gz"):
    convert_file(csv_gz)
