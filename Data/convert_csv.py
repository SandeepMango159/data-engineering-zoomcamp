import pandas as pd
from pathlib import Path

# Change these paths as needed
SOURCE_DIR = Path("C:/Sandeep SSD/Programming SSD/Data Engineering Zoomcamp/data-engineering-zoomcamp/Data/data/csv") # where your .csv files are
DEST_DIR = Path("C:/Sandeep SSD/Programming SSD/Data Engineering Zoomcamp/data-engineering-zoomcamp/Data/data/csv/parquet") # where the .parquet files will go
DEST_DIR.mkdir(parents=True, exist_ok=True)

# Loop over all .csv and .csv.gz files
for csv_file in SOURCE_DIR.glob("*.csv*"):
    print(f"Converting: {csv_file.name}")
    df = pd.read_csv(csv_file, compression="infer")  # handles .csv or .csv.gz
    parquet_file = DEST_DIR / csv_file.with_suffix(".parquet").name.replace(".gz", "")
    df.to_parquet(parquet_file, index=False)
    print(f"Saved to: {parquet_file}")

print("✅ All files converted.")
