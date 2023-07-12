from __future__ import annotations

"""
Most of this code was taken from

https://github.com/data-engineering-collective/plateau

https://github.com/data-engineering-collective/plateau/blob/d4c4522f5a829d43e3368fc82e1568c91fa352f3/plateau/core/common_metadata.py

and adapted to this project

under the original license

MIT License

Copyright (c) 2022 The plateau contributors.
Copyright (c) 2020-2021 The kartothek contributors.
Copyright (c) 2019 JDA Software, Inc

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

"""
import difflib
import json
import logging
import pprint
from collections.abc import Iterable
from copy import deepcopy

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

_logger = logging.getLogger()


class SchemaWrapper:
    def __init__(self, schema: pa.Schema):
        self.schema = schema

    def __hash__(self):
        # FIXME: pyarrow raises a "cannot hash type dict" error
        return hash(_schema2bytes(self.schema))


def pyarrow_to_deltalake(schema: pa.Schema) -> pa.Schema:
    """Adjust data types to make schema compatible with Delta Lake dtypes.
    Not all Arrow data types are supported by Delta Lake. See also
    ``deltalake.schema.delta_arrow_schema_from_pandas``.

    Notes
    -----
    We shouldn't need this when https://github.com/delta-io/delta-rs/issues/686 is closed
    """
    schema_out = []
    for field in schema:
        if isinstance(field.type, pa.TimestampType):
            f = pa.field(
                name=field.name,
                type=pa.timestamp("us"),
                nullable=field.nullable,
                metadata=field.metadata,
            )
            schema_out.append(f)
        else:
            schema_out.append(field)
    return pa.schema(schema_out, metadata=schema.metadata)


def _pandas_in_schemas(schemas):
    """Check if any schema contains pandas metadata."""
    has_pandas = False
    for schema in schemas:
        if schema.metadata and b"pandas" in schema.metadata:
            has_pandas = True
    return has_pandas


def _determine_schemas_to_compare(
    schemas: Iterable[pa.Schema], ignore_pandas: bool
) -> tuple[pa.Schema | None, list[tuple[pa.Schema, list[str]]]]:
    """Iterate over a list of `pyarrow.Schema` objects and prepares them for
    comparison by picking a reference and determining all null columns.

    .. note::

        If pandas metadata exists, the version stored in the metadata is overwritten with the currently
        installed version since we expect to stay backwards compatible

    Returns
    -------
    reference: Schema
        A reference schema which is picked from the input list. The reference schema is guaranteed
        to be a schema having the least number of null columns of all input columns. The set of null
        columns is guaranteed to be a true subset of all null columns of all input schemas. If no such
        schema can be found, an Exception is raised
    list_of_schemas: List[Tuple[Schema, List]]
        A list holding pairs of (Schema, null_columns) where the null_columns are all columns which are null and
        must be removed before comparing the schemas
    """
    has_pandas = _pandas_in_schemas(schemas) and not ignore_pandas
    schemas_to_evaluate: list[tuple[pa.Schema, list[str]]] = []
    reference = None
    null_cols_in_reference = set()
    # Hashing the schemas is a very fast way to reduce the number of schemas to
    # actually compare since in most circumstances this reduces to very few
    # (which differ in e.g. null columns)
    for schema_wrapped in set(map(SchemaWrapper, schemas)):
        schema = schema_wrapped.schema
        del schema_wrapped
        if has_pandas:
            metadata = schema.metadata
            if metadata is None or b"pandas" not in metadata:
                raise ValueError(
                    "Pandas and non-Pandas schemas are not comparable. "
                    "Use ignore_pandas=True if you only want to compare "
                    "on Arrow level."
                )
            pandas_metadata = json.loads(metadata[b"pandas"].decode("utf8"))

            # we don't care about the pandas version, since we assume it's safe
            # to read datasets that were written by older or newer versions.
            pandas_metadata["pandas_version"] = f"{pd.__version__}"

            metadata_clean = deepcopy(metadata)
            metadata_clean[b"pandas"] = _dict_to_binary(pandas_metadata)
            current = pa.schema(schema, metadata_clean)
        else:
            current = schema

        # If a field is null we cannot compare it and must therefore reject it
        null_columns = {field.name for field in current if field.type == pa.null()}

        # Determine a valid reference schema. A valid reference schema is considered to be the schema
        # of all input schemas with the least empty columns.
        # The reference schema ought to be a schema whose empty columns are a true subset for all sets
        # of empty columns. This ensures that the actual reference schema is the schema with the most
        # information possible. A schema which doesn't fulfil this requirement would weaken the
        # comparison and would allow for false positives

        # Trivial case
        if reference is None:
            reference = current
            null_cols_in_reference = null_columns
        # The reference has enough information to validate against current schema.
        # Append it to the list of schemas to be verified
        elif null_cols_in_reference.issubset(null_columns):
            schemas_to_evaluate.append((current, null_columns))
        # current schema includes all information of reference and more.
        # Add reference to schemas_to_evaluate and update reference
        elif null_columns.issubset(null_cols_in_reference):
            schemas_to_evaluate.append((reference, list(null_cols_in_reference)))
            reference = current
            null_cols_in_reference = null_columns
        # If there is no clear subset available elect the schema with the least null columns as `reference`.
        # Iterate over the null columns of `reference` and replace it with a non-null field of the `current`
        # schema which recovers the loop invariant (null columns of `reference` is subset of `current`)
        else:
            if len(null_columns) < len(null_cols_in_reference):
                reference, current = current, reference
                null_cols_in_reference, null_columns = (
                    null_columns,
                    null_cols_in_reference,
                )

            for col in null_cols_in_reference - null_columns:
                # Enrich the information in the reference by grabbing the missing fields
                # from the current iteration. This assumes that we only check for global validity and
                # isn't relevant where the reference comes from.
                reference = _swap_fields_by_name(reference, current, col)
                null_cols_in_reference.remove(col)
            schemas_to_evaluate.append((current, null_columns))

    assert (reference is not None) or (not schemas_to_evaluate)

    return reference, schemas_to_evaluate


def _swap_fields_by_name(reference, current, field_name):
    current_field = current.field(field_name)
    reference_index = reference.get_field_index(field_name)
    return reference.set(reference_index, current_field)


def _strip_columns_from_schema(schema, field_names):
    stripped_schema = schema

    for name in field_names:
        ix = stripped_schema.get_field_index(name)
        if ix >= 0:
            stripped_schema = stripped_schema.remove(ix)
        else:
            # If the returned index is negative, the field doesn't exist in the schema.
            # This is most likely an indicator for incompatible schemas and we refuse to strip the schema
            # to not obfurscate the validation result
            _logger.warning(
                "Unexpected field `%s` encountered while trying to strip `null` columns.\n"
                "Schema was:\n\n`%s`" % (name, schema)
            )
            return schema
    return stripped_schema


def _schema2bytes(schema: SchemaWrapper) -> bytes:
    buf = pa.BufferOutputStream()
    pq.write_metadata(schema, buf, coerce_timestamps="us")
    return buf.getvalue().to_pybytes()


def _remove_diff_header(diff):
    diff = list(diff)
    for ix, el in enumerate(diff):
        # This marks the first actual entry of the diff
        # e.g. @@ -1,5 + 2,5 @@
        if el.startswith("@"):
            return diff[ix:]
    return diff


def _diff_schemas(first, second):
    # see https://issues.apache.org/jira/browse/ARROW-4176

    first_pyarrow_info = str(first.remove_metadata())
    second_pyarrow_info = str(second.remove_metadata())
    pyarrow_diff = _remove_diff_header(
        difflib.unified_diff(
            str(first_pyarrow_info).splitlines(), str(second_pyarrow_info).splitlines()
        )
    )

    first_pandas_info = first.pandas_metadata
    second_pandas_info = second.pandas_metadata
    pandas_meta_diff = _remove_diff_header(
        difflib.unified_diff(
            pprint.pformat(first_pandas_info).splitlines(),
            pprint.pformat(second_pandas_info).splitlines(),
        )
    )

    diff_string = (
        "Arrow schema:\n"
        + "\n".join(pyarrow_diff)
        + "\n\nPandas_metadata:\n"
        + "\n".join(pandas_meta_diff)
    )

    return diff_string


def validate_compatible(
    schemas: Iterable[pa.Schema], ignore_pandas: bool = False
) -> pa.Schema:
    """Validate that all schemas in a given list are compatible.

    Apart from the pandas version preserved in the schema metadata, schemas must be completely identical. That includes
    a perfect match of the whole metadata (except the pandas version) and pyarrow types.

    In the case that all schemas don't contain any pandas metadata, we will check the Arrow
    schemas directly for compatibility.

    Parameters
    ----------
    schemas: List[Schema]
        Schema information from multiple sources, e.g. multiple partitions. List may be empty.
    ignore_pandas: bool
        Ignore the schema information given by Pandas an always use the Arrow schema.

    Returns
    -------
    schema: SchemaWrapper
        The reference schema which was tested against

    Raises
    ------
    ValueError
        At least two schemas are incompatible.
    """
    reference, schemas_to_evaluate = _determine_schemas_to_compare(
        schemas, ignore_pandas
    )

    for current, null_columns in schemas_to_evaluate:
        # We have schemas so the reference schema should be non-none.
        assert reference is not None
        # Compare each schema to the reference but ignore the null_cols and the Pandas schema information.
        reference_to_compare = _strip_columns_from_schema(
            reference, null_columns
        ).remove_metadata()
        current_to_compare = _strip_columns_from_schema(
            current, null_columns
        ).remove_metadata()

        def _fmt_origin(origin):
            origin = sorted(origin)
            # dask cuts of exception messages at 1k chars:
            #   https://github.com/dask/distributed/blob/6e0c0a6b90b1d3c/distributed/core.py#L964
            # therefore, we cut the the maximum length
            max_len = 200
            inner_msg = ", ".join(origin)
            ellipsis = "..."
            if len(inner_msg) > max_len + len(ellipsis):
                inner_msg = inner_msg[:max_len] + ellipsis
            return f"{{{inner_msg}}}"

        if reference_to_compare != current_to_compare:
            schema_diff = _diff_schemas(reference, current)
            exception_message = """Schema violation

Origin schema: {origin_schema}
Origin reference: {origin_reference}

Diff:
{schema_diff}

Reference schema:
{reference}""".format(
                schema_diff=schema_diff,
                reference=str(reference),
                origin_schema=_fmt_origin(current.origin),
                origin_reference=_fmt_origin(reference.origin),
            )
            raise ValueError(exception_message)

    # add all origins to result AFTER error checking, otherwise the error message would be pretty misleading due to the
    # reference containing all origins.
    if reference is None:
        return None
    else:
        return reference


def _dict_to_binary(dct):
    return json.dumps(dct, sort_keys=True).encode("utf8")
