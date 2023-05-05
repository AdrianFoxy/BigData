import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

csv_path = "../resources/csv/trip.csv"
df = pd.read_csv(csv_path)

table = pa.Table.from_pandas(df)

parquet_path = "../resources/parquet/trip.parquet"
pq.write_table(table, parquet_path)
