"""Unit tests for inlined schema definitions."""


import pytest

from opteryx.types import LogicalCategory
from opteryx.types.logical_type import FLOAT64, INT64, VARCHAR
from opteryx.planner.plan_context import PlanContext
from opteryx.types.schema import ColumnDescriptor, ColumnDisposition, RelationDescriptor, RelationSchema, SchemaColumn



def _col(name, column_type, **fields):
    """A bound column, minted the only way one is: in a query's ColumnTable."""
    return PlanContext().columns.relation_column("users", name, column_type=column_type, **fields)


def _const(name, column_type, **fields):
    return PlanContext().columns.constant(name, column_type=column_type, **fields)


class TestColumnDisposition:
    """Test ColumnDisposition constants."""

    def test_disposition_constants(self):
        """Test that disposition constants are defined."""
        assert ColumnDisposition.INTERNAL == "INTERNAL"
        assert ColumnDisposition.PRIMARY_KEY == "PRIMARY_KEY"
        assert ColumnDisposition.INDEXED == "INDEXED"


class TestSchemaColumn:
    """Test SchemaColumn dataclass."""

    def test_bound_column_is_minted_not_constructed(self):
        """A bound column exists only as a row of its query's ColumnTable: built
        without a slot it is refused where it is made (stage 4C)."""
        from opteryx.exceptions import InvalidInternalStateError

        with pytest.raises(InvalidInternalStateError):
            SchemaColumn(name="test_col", column_type=VARCHAR, identity=b"test_col")

        col = _col("test_col", VARCHAR)
        assert col.slot is not None
        assert col.category == LogicalCategory.VARCHAR
        assert col.nullable is True

    def test_column_str(self):
        """Test string representation of column."""
        col = _col("test_col", VARCHAR)
        assert str(col) == "test_col:VARCHAR"

    def test_column_repr(self):
        """Test repr of column."""
        col = _col("test_col", VARCHAR)
        repr_str = repr(col)
        assert "SchemaColumn" in repr_str
        assert "test_col" in repr_str

    def test_column_all_names_without_aliases(self):
        """Test all_names property without aliases."""
        col = _col("col1", INT64)
        assert col.all_names == ["col1"]

    def test_column_all_names_with_aliases(self):
        """Test all_names property with aliases."""
        col = _col("col1", INT64, aliases=["col_one", "column_1"])
        assert col.all_names == ["col1", "col_one", "column_1"]

    def test_column_to_dict(self):
        """A column is persisted as its DESCRIPTION - no engine identity."""
        col = ColumnDescriptor(
            name="test_col",
            column_type=VARCHAR,
            nullable=False,
            description="Test column",
        )
        col_dict = col.to_dict()
        assert col_dict["name"] == "test_col"
        assert col_dict["type"] == "VARCHAR"
        assert "identity" not in col_dict
        assert col_dict["nullable"] is False
        assert col_dict["description"] == "Test column"

    def test_column_from_dict(self):
        """A persisted column reads back as a descriptor; a legacy `identity` key
        (a per-query handle that meant nothing once written) is discarded."""
        col_dict = {
            "name": "test_col",
            "type": "VARCHAR",
            "identity": "test_col",
            "nullable": False,
        }
        col = ColumnDescriptor.from_dict(col_dict)
        assert col.name == "test_col"
        assert col.category == LogicalCategory.VARCHAR
        assert col.nullable is False

    def test_column_from_dict_refuses_aliases(self):
        """An alias is binder state; a persisted column carrying one cannot be
        described, so it is refused rather than dropped."""
        from opteryx.exceptions import InvalidInternalStateError

        with pytest.raises(InvalidInternalStateError):
            ColumnDescriptor.from_dict({"name": "c", "type": "VARCHAR", "aliases": ["c1"]})

    def test_column_roundtrip(self):
        """Test to_dict/from_dict roundtrip."""
        original = ColumnDescriptor(
            name="col1",
            column_type=FLOAT64,
            nullable=True,
            description="Test",
        )
        restored = ColumnDescriptor.from_dict(original.to_dict())
        assert restored.name == original.name
        assert restored.column_type == original.column_type
        assert restored.nullable == original.nullable
        assert restored.description == original.description


class TestConstantColumn:
    """Test ConstantColumn dataclass."""

    def test_create_constant_column(self):
        """Test creating a ConstantColumn."""
        col = _const("const_42", INT64, value=42)
        assert col.name == "const_42"
        assert col.category == LogicalCategory.INTEGER
        assert col.value == 42

    def test_constant_column_str(self):
        """Test string representation of constant column."""
        col = _const("const_42", INT64, value=42)
        assert str(col) == "const_42=42"

class TestRelationSchema:
    """Test RelationSchema dataclass."""

    def test_create_empty_schema(self):
        """Test creating an empty schema."""
        schema = RelationSchema(name="test_table")
        assert schema.name == "test_table"
        assert schema.num_columns == 0
        assert schema.column_names == []

    def test_create_schema_with_columns(self):
        """Test creating a schema with columns."""
        col1 = _col("id", INT64)
        col2 = _col("name", VARCHAR)
        schema = RelationSchema(name="users", columns=[col1, col2])
        assert schema.name == "users"
        assert schema.num_columns == 2
        assert schema.column_names == ["id", "name"]

    def test_schema_str(self):
        """Test string representation of schema."""
        col1 = _col("id", INT64)
        col2 = _col("name", VARCHAR)
        schema = RelationSchema(name="users", columns=[col1, col2])
        schema_str = str(schema)
        assert "users" in schema_str
        assert "id:INT64" in schema_str
        assert "name:VARCHAR" in schema_str

    def test_schema_column_lookup(self):
        """Test column lookup by name."""
        col1 = _col("id", INT64)
        col2 = _col("name", VARCHAR)
        schema = RelationSchema(name="users", columns=[col1, col2])

        found_col = schema.column("id")
        assert found_col is not None
        assert found_col.name == "id"
        assert found_col.category == LogicalCategory.INTEGER

        not_found = schema.column("missing")
        assert not_found is None

    def test_schema_column_lookup_with_aliases(self):
        """Test column lookup including aliases."""
        col = _col("user_id", INT64, aliases=["uid", "id"])
        schema = RelationSchema(name="users", columns=[col])

        # Find by primary name
        found = schema.column("user_id")
        assert found is not None

        # Find by alias
        found = schema.column("uid")
        assert found is not None
        assert found.name == "user_id"

        found = schema.column("id")
        assert found is not None
        assert found.name == "user_id"

    def test_schema_pop_column(self):
        """Test removing a column."""
        col1 = _col("id", INT64)
        col2 = _col("name", VARCHAR)
        schema = RelationSchema(name="users", columns=[col1, col2])

        assert schema.num_columns == 2
        popped = schema.pop_column("id")
        assert popped is not None
        assert popped.name == "id"
        assert schema.num_columns == 1
        assert schema.column_names == ["name"]

        not_found = schema.pop_column("missing")
        assert not_found is None

    def test_schema_all_column_names_with_aliases(self):
        """Test all_column_names including aliases."""
        col1 = _col("id", INT64, aliases=["user_id"])
        col2 = _col("name", VARCHAR)
        schema = RelationSchema(name="users", columns=[col1, col2])

        all_names = schema.all_column_names
        assert "id" in all_names
        assert "user_id" in all_names
        assert "name" in all_names
        assert len(all_names) == 3

    def test_schema_to_dict(self):
        """Test converting schema to dictionary."""
        col = ColumnDescriptor(name="id", column_type=INT64)
        schema = RelationDescriptor(name="users", columns=[col], primary_key="id")
        schema_dict = schema.to_dict()

        assert schema_dict["name"] == "users"
        assert schema_dict["primary_key"] == "id"
        assert len(schema_dict["columns"]) == 1
        assert schema_dict["columns"][0]["name"] == "id"

    def test_schema_from_dict(self):
        """Test creating schema from dictionary."""
        schema_dict = {
            "name": "users",
            "columns": [
                {"name": "id", "type": "INTEGER", "identity": "id", "nullable": False},
                {"name": "name", "type": "VARCHAR", "identity": "name"},
            ],
            "primary_key": "id",
        }
        schema = RelationDescriptor.from_dict(schema_dict)

        assert schema.name == "users"
        assert len(schema.columns) == 2
        assert schema.column_names == ["id", "name"]
        assert schema.primary_key == "id"

    def test_schema_json_roundtrip(self):
        """A described relation survives a JSON round trip."""
        import json

        col1 = ColumnDescriptor(name="id", column_type=INT64)
        col2 = ColumnDescriptor(name="name", column_type=VARCHAR)
        original = RelationDescriptor(name="users", columns=[col1, col2], primary_key="id")

        json_str = json.dumps(original.to_dict())
        restored = RelationDescriptor.from_dict(json.loads(json_str))

        assert restored.name == original.name
        assert len(restored.columns) == len(original.columns)
        assert restored.column_names == original.column_names
        assert restored.primary_key == original.primary_key

    def test_schema_find_column_alias(self):
        """Test find_column method (API compatibility)."""
        col = _col("user_id", INT64)
        schema = RelationSchema(name="users", columns=[col])

        found = schema.find_column("user_id")
        assert found is not None
        assert found.name == "user_id"

        not_found = schema.find_column("missing")
        assert not_found is None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
