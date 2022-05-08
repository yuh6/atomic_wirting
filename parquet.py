import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import decorator_atomic_write
# define Schema
schema = pa.schema([
    ('id', pa.int32()),
    ('email', pa.string())
])

# prepare data
ids = pa.array([1, 2], type=pa.int32())
emails = pa.array(['first@example.com', 'second@example.com'], pa.string())

# generate Parquet data
batch = pa.RecordBatch.from_arrays(
    [ids, emails],
    schema=schema
)

table = pa.Table.from_batches([batch])

# Write Parquet file into  plain.parquet

decorator_atomic_write.write_file(table, 'plain.parquet', 'parquet_w')
schema = pq.read_schema('plain.parquet')

df = pd.read_parquet('plain.parquet')
result = df.to_json()
if result == '{"id":{"0":1,"1":2},"email":{"0":"first@example.com","1":"second@example.com"}}':
    print('The answer is correct!')