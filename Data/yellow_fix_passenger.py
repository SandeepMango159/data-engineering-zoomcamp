import pandas as pd
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq

src_dir   = Path("C:/Sandeep SSD/Programming SSD/Data Engineering Zoomcamp/data-engineering-zoomcamp/Data/data/csv/yellowschemafix/")        # where the *.csv.gz live
out_dir   = Path("C:/Sandeep SSD/Programming SSD/Data Engineering Zoomcamp/data-engineering-zoomcamp/Data/data/csv/yellowschemafix/yellow_tripdata_2019-10_passenger_clean") # temp output
out_dir.mkdir(exist_ok=True)

print(src_dir)

for gz in src_dir.glob("yellow_tripdata_*.csv.gz"):
    print("in loop")
    df = pd.read_csv(gz, compression="gzip")

    # Make passenger_count a nullable integer
    df["passenger_count"] = (
        pd.to_numeric(df["passenger_count"], errors="coerce")
          .astype("Int64")
    )

    # Force payment_type to nullable integer
    df["payment_type"] = pd.to_numeric(df["payment_type"], errors="coerce").astype("Int64")



    # Write clean parquet
    pq.write_table(
        pa.Table.from_pandas(df),
        out_dir / gz.with_suffix("").with_suffix(".parquet").name,
        compression="snappy"
    )
