import os
import glob

def set_up_iceberg():

    import pyarrow
    import opteryx
    from pyiceberg.catalog.sql import SqlCatalog

    ICEBERG_BASE_PATH: str = f"scratch/iceberg"

    def cast_dataset(dataset):
        for column in dataset.column_names:
            if pyarrow.types.is_date64(dataset.schema.field(column).type):
                dataset = dataset.set_column(
                    dataset.schema.get_field_index(column),
                    column,
                    pyarrow.compute.cast(dataset[column], pyarrow.timestamp('ms'))
                )
        return dataset

    existing = os.path.exists(ICEBERG_BASE_PATH)
    os.makedirs(ICEBERG_BASE_PATH, exist_ok=True)

    # Step 1: Create a local Iceberg catalog
    catalog = SqlCatalog(
        "default",
        **{
            "uri": f"sqlite:///{ICEBERG_BASE_PATH}/pyiceberg_catalog.db",
            "warehouse": f"file://{ICEBERG_BASE_PATH}",
        },
    )

    if existing:
        return catalog
    
    schema = opteryx.query("SELECT * FROM hits LIMIT 1e6").arrow().schema

    catalog.create_namespace("lake")
    table = catalog.create_table("lake.hits", schema=schema)

    table.add_files(glob.glob("hits/*.parquet"))

    return catalog

import opteryx
from opteryx.connectors.iceberg_connector import IcebergConnector

catalog = set_up_iceberg()
opteryx.register_store(
    "lake",
    IcebergConnector,
    catalog=catalog,
    remove_prefix=True,
)

print(opteryx.query("SELECT * FROM lake.hits LIMIT 10"))
