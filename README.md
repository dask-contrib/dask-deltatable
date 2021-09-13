## Dask Deltatable Reader

Reads a Delta Table from directory using Dask engine.

To Try out the package:

```
pip install dask-deltatable
```

Features:
1. Reads the parquet files based on delta logs parallely using dask engine
2. Supports all three filesystem like s3, azurefs, gcsfs
3. Supports some delta features like
   - Time Travel
   - Schema evolution
   - parquet filters
     - row filter
     - partition filter
