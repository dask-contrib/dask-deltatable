## Dask-DeltaTable

Reading and writing to Delta Lake using Dask engine.

### Installation

`dask-deltatable` is available on PyPI:

```
pip install dask-deltatable
```

And conda-forge:

```
conda install -c conda-forge dask-deltatable
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

### Not supported

1. Writing to Delta Lake is still in development.
2. `optimize` API to run a bin-packing operation on a Delta Table.

### Reading from Delta Lake

```python
import dask_deltatable as ddt

# read delta table
df = ddt.read_deltalake("delta_path")

# with specific version
df = ddt.read_deltalake("delta_path", version=3)

# with specific datetime
df = ddt.read_deltalake("delta_path", datetime="2018-12-19T16:39:57-08:00")
```

`df` is a Dask DataFrame that you can work with in the same way you normally would. See
[the Dask DataFrame documentation](https://docs.dask.org/en/stable/dataframe.html) for
available operations.

### Accessing remote file systems

To be able to read from S3, azure, gcsfs, and other remote filesystems,
you ensure the credentials are properly configured in environment variables
or config files. For AWS, you may need `~/.aws/credential`; for gcsfs,
`GOOGLE_APPLICATION_CREDENTIALS`. Refer to your cloud provider documentation
to configure these.

```python
ddt.read_deltalake("s3://bucket_name/delta_path", version=3)
```

### Accessing AWS Glue catalog

`dask-deltatable` can connect to AWS Glue catalog to read the delta table.
The method will look for `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`
environment variables, and if those are not available, fall back to
`~/.aws/credentials`.

Example:

```python
ddt.read_deltalake(catalog="glue", database_name="science", table_name="physics")
```

### Accessing Unity catalog

`dask-deltatable` can connect to Unity catalog to read the delta table.
The method will look for `DATABRICKS_HOST` and `DATABRICKS_TOKEN` environment
variables or try to find them as `kwargs` with the same name but lowercase.

Example:

```python
ddt.read_unity_catalog(
    catalog_name="projects",
    schema_name="science",
    table_name="physics"
)
```

### Writing to Delta Lake

To write a Dask dataframe to Delta Lake, use `to_deltalake` method.

```python
import dask.dataframe as dd
import dask_deltatable as ddt

df = dd.read_csv("s3://bucket_name/data.csv")
# do some processing on the dataframe...
ddt.to_deltalake("s3://bucket_name/delta_path", df)
```

Writing to Delta Lake is still in development, so be aware that some features
may not work.
