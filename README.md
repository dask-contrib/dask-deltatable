## Dask-DeltaTable

Reading and writing to Delta Lake using Dask engine.

### Installation

To install the package:

```
pip install dask-deltatable
```

### Features:

1. Read the parquet files from Delta Lake and parallelize with Dask
2. Write Dask dataframes to Delta Lake (limited support)
3. Supports multiple filesystems (s3, azurefs, gcsfs)
4. Subset of Delta Lake features:
   - Time Travel
   - Schema evolution
   - Parquet filters
     - row filter
     - partition filter
5. Query Delta commit info and history
6. API to ``vacuum`` the old / unused parquet files
7. Load different versions of data by timestamp or version.

### Not supported

1. Writing to Delta Lake is still in development.
2. `optimize` API to run a bin-packing operation on a Delta Table.

### Reading from Delta Lake

```python
import dask_deltatable as ddt

# read delta table
ddt.read_delta_table("delta_path")

# with specific version
ddt.read_delta_table("delta_path", version=3)

# with specific datetime
ddt.read_delta_table("delta_path", datetime="2018-12-19T16:39:57-08:00")
```

### Accessing remote file systems

To be able to read from S3, azure, gcsfs, and other remote filesystems,
you ensure the credentials are properly configured in environment variables
or config files. For AWS, you may need `~/.aws/credential`; for gcsfs,
`GOOGLE_APPLICATION_CREDENTIALS`. Refer to your cloud provider documentation
to configure these.

```python
ddt.read_delta_table("s3://bucket_name/delta_path", version=3)
```

### Accessing AWS Glue catalog

`dask-deltatable` can connect to AWS Glue catalog to read the delta table.
The method will look for `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`
environment variables, and if those are not available, fall back to
`~/.aws/credentials`.

Example:

```python
ddt.read_delta_table(catalog="glue", database_name="science", table_name="physics")
```

### Inspecting Delta Table history

One of the features of Delta Lake is preserving the history of changes, which can be is useful
for auditing and debugging. `dask-deltatable` provides APIs to read the commit info and history.

```python

```python
# read delta complete history
ddt.read_delta_history("delta_path")

# read delta history upto given limit
ddt.read_delta_history("delta_path", limit=5)
```

### Managing Delta Tables

Vacuuming a table will delete any files that have been marked for deletion. This
may make some past versions of a table invalid, so this can break time travel.
However, it will save storage space. Vacuum will retain files in a certain
window, by default one week, so time travel will still work in shorter ranges.

```python
# read delta history to delete the files
ddt.vacuum("delta_path", dry_run=False)
```
