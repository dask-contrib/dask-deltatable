## Dask Deltatable Reader

Reads a Delta Table from directory using Dask engine.

To Try out the package:

```
pip install dask-deltatable
```

### Features:
1. Reads the parquet files based on delta logs parallely using dask engine
2. Supports all three filesystem like s3, azurefs, gcsfs
3. Supports some delta features like
   - Time Travel
   - Schema evolution
   - parquet filters
     - row filter
     - partition filter
4. Query Delta commit info - History
5. vacuum the old/ unused parquet files
6. load different versions of data using datetime.

### Usage:

```
import dask_deltatable as ddt

# read delta table
ddt.read_delta_table("delta_path")

# read delta table for specific version
ddt.read_delta_table("delta_path",version=3)

# read delta table for specific datetime
ddt.read_delta_table("delta_path",datetime="2018-12-19T16:39:57-08:00")


# read delta complete history
ddt.read_delta_history("delta_path")

# read delta history upto given limit
ddt.read_delta_history("delta_path",limit=5)

# read delta history to delete the files
ddt.vacuum("delta_path",dry_run=False)

# Can read from S3,azure,gcfs etc.
ddt.read_delta_table("s3://bucket_name/delta_path",version=3)
# please ensure the credentials are properly configured as environment variable or
# configured as in ~/.aws/credential
```
