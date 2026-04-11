"""Unit tests for inlined schema definitions."""

import json

import pytest

from opteryx.types import OrsoTypes
from opteryx.types.schema import ColumnDisposition, ConstantColumn, FlatColumn, RelationSchema


class TestColumnDisposition:
    """Test ColumnDisposition constants."""

    def test_disposition_constants(self):
        """Test that disposition constants are defined."""
        assert ColumnDisposition.INTERNAL == "INTERNAL"
        assert ColumnDisposition.PRIMARY_KEY == "PRIMARY_KEY"
        assert ColumnDisposition.INDEXED == "INDEXED"


class TestFlatColumn:
    """Test FlatColumn dataclass."""

    def test_create_basic_column(self):
        """Test creating a basic FlatColumn."""
        col = FlatColumn(name="test_col", type=OrsoTypes.VARCHAR, identity="test_col")
        assert col.name == "test_col"
        assert col.type == OrsoTypes.VARCHAR
        assert col.identity == "test_col"
        assert col.nullable is True
        assert col.default is None

    def test_create_column_with_metadata(self):
        """Test creating FlatColumn with additional metadata."""
        col = FlatColumn(
            name="age",
            type=OrsoTypes.INTEGER,
            identity="age",
            nullable=False,
            description="User age",
            disposition=ColumnDisposition.INDEXED,
        )
        assert col.name == "age"
        assert col.type == OrsoTypes.INTEGER
        assert col.nullable is False
        assert col.description == "User age"
        assert col.disposition == ColumnDisposition.INDEXED

    def test_column_str(self):
        """Test string representation of column."""
        col = FlatColumn(name="test_col", type=OrsoTypes.VARCHAR, identity="test_col")
        assert str(col) == "test_col:VARCHAR"

    def test_column_repr(self):
        """Test repr of column."""
        col = FlatColumn(name="test_col", type=OrsoTypes.VARCHAR, identity="test_col")
        repr_str = repr(col)
        assert "FlatColumn" in repr_str
        assert "test_col" in repr_str

    def test_column_all_names_without_aliases(self):
        """Test all_names property without aliases."""
        col = FlatColumn(name="col1", type=OrsoTypes.INTEGER, identity="col1")
        assert col.all_names == ["col1"]

    def test_column_all_names_with_aliases(self):
        """Test all_names property with aliases."""
        col = FlatColumn(
            name="col1", type=OrsoTypes.INTEGER, identity="col1", aliases=["col_one", "column_1"]
        )
        assert col.all_names == ["col1", "col_one", "column_1"]

    def test_column_with_complex_type(self):
        """Test column with complex type (e.g., ARRAY with element type)."""
        col = FlatColumn(
            name="tags", type=OrsoTypes.ARRAY, identity="tags", element_type=OrsoTypes.VARCHAR
        )
        assert col.type == OrsoTypes.ARRAY
        assert col.element_type == OrsoTypes.VARCHAR

    def test_column_with_decimal_precision_scale(self):
        """Test column with DECIMAL precision and scale."""
        col = FlatColumn(
            name="price", type=OrsoTypes.DECIMAL, identity="price", precision=10, scale=2
        )
        assert col.type == OrsoTypes.DECIMAL
        assert col.precision == 10
        assert col.scale == 2

    def test_column_to_dict(self):
        """Test converting column to dictionary."""
        col = FlatColumn(
            name="test_col",
            type=OrsoTypes.VARCHAR,
            identity="test_col",
            nullable=False,
            description="Test column",
        )
        col_dict = col.to_dict()
        assert col_dict["name"] == "test_col"
        assert col_dict["type"] == "VARCHAR"
        assert col_dict["identity"] == "test_col"
        assert col_dict["nullable"] is False
        assert col_dict["description"] == "Test column"

    def test_column_from_dict(self):
        """Test creating column from dictionary."""
        col_dict = {
            "name": "test_col",
            "type": "VARCHAR",
            "identity": "test_col",
            "nullable": False,
        }
        col = FlatColumn.from_dict(col_dict)
        assert col.name == "test_col"
        assert col.type == OrsoTypes.VARCHAR
        assert col.identity == "test_col"
        assert col.nullable is False

    def test_column_roundtrip(self):
        """Test to_dict/from_dict roundtrip."""
        original = FlatColumn(
            name="col1",
            type=OrsoTypes.DOUBLE,
            identity="col1",
            nullable=True,
            description="Test",
            aliases=["c1"],
        )
        col_dict = original.to_dict()
        restored = FlatColumn.from_dict(col_dict)
        assert restored.name == original.name
        assert restored.type == original.type
        assert restored.identity == original.identity
        assert restored.nullable == original.nullable
        assert restored.description == original.description
        assert restored.aliases == original.aliases


class TestConstantColumn:
    """Test ConstantColumn dataclass."""

    def test_create_constant_column(self):
        """Test creating a ConstantColumn."""
        col = ConstantColumn(name="const_42", type=OrsoTypes.INTEGER, identity="const_42", value=42)
        assert col.name == "const_42"
        assert col.type == OrsoTypes.INTEGER
        assert col.value == 42

    def test_constant_column_str(self):
        """Test string representation of constant column."""
        col = ConstantColumn(name="const_42", type=OrsoTypes.INTEGER, identity="const_42", value=42)
        assert str(col) == "const_42=42"

    def test_constant_column_inherits_from_flatcolumn(self):
        """Test that ConstantColumn inherits FlatColumn properties."""
        col = ConstantColumn(
            name="const_val",
            type=OrsoTypes.VARCHAR,
            identity="const_val",
            value="hello",
            nullable=False,
        )
        assert col.nullable is False
        assert col.all_names == ["const_val"]
        assert str(col) == "const_val=hello"


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
        col1 = FlatColumn(name="id", type=OrsoTypes.INTEGER, identity="id")
        col2 = FlatColumn(name="name", type=OrsoTypes.VARCHAR, identity="name")
        schema = RelationSchema(name="users", columns=[col1, col2])
        assert schema.name == "users"
        assert schema.num_columns == 2
        assert schema.column_names == ["id", "name"]

    def test_schema_str(self):
        """Test string representation of schema."""
        col1 = FlatColumn(name="id", type=OrsoTypes.INTEGER, identity="id")
        col2 = FlatColumn(name="name", type=OrsoTypes.VARCHAR, identity="name")
        schema = RelationSchema(name="users", columns=[col1, col2])
        schema_str = str(schema)
        assert "users" in schema_str
        assert "id:INTEGER" in schema_str
        assert "name:VARCHAR" in schema_str

    def test_schema_column_lookup(self):
        """Test column lookup by name."""
        col1 = FlatColumn(name="id", type=OrsoTypes.INTEGER, identity="id")
        col2 = FlatColumn(name="name", type=OrsoTypes.VARCHAR, identity="name")
        schema = RelationSchema(name="users", columns=[col1, col2])

        found_col = schema.column("id")
        assert found_col is not None
        assert found_col.name == "id"
        assert found_col.type == OrsoTypes.INTEGER

        not_found = schema.column("missing")
        assert not_found is None

    def test_schema_column_lookup_with_aliases(self):
        """Test column lookup including aliases."""
        col = FlatColumn(
            name="user_id", type=OrsoTypes.INTEGER, identity="user_id", aliases=["uid", "id"]
        )
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
        col1 = FlatColumn(name="id", type=OrsoTypes.INTEGER, identity="id")
        col2 = FlatColumn(name="name", type=OrsoTypes.VARCHAR, identity="name")
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
        col1 = FlatColumn(name="id", type=OrsoTypes.INTEGER, identity="id", aliases=["user_id"])
        col2 = FlatColumn(name="name", type=OrsoTypes.VARCHAR, identity="name")
        schema = RelationSchema(name="users", columns=[col1, col2])

        all_names = schema.all_column_names
        assert "id" in all_names
        assert "user_id" in all_names
        assert "name" in all_names
        assert len(all_names) == 3

    def test_schema_validate_duplicate_names(self):
        """Test schema validation detects duplicate column names."""
        col1 = FlatColumn(name="id", type=OrsoTypes.INTEGER, identity="id")
        col2 = FlatColumn(
            name="id",  # Duplicate name
            type=OrsoTypes.VARCHAR,
            identity="id2",
        )
        schema = RelationSchema(name="users", columns=[col1, col2])

        # Validation should fail with duplicate names
        assert schema.validate() is False

    def test_schema_validate_valid(self):
        """Test schema validation passes for valid schema."""
        col1 = FlatColumn(name="id", type=OrsoTypes.INTEGER, identity="id")
        col2 = FlatColumn(name="name", type=OrsoTypes.VARCHAR, identity="name")
        schema = RelationSchema(name="users", columns=[col1, col2])

        assert schema.validate() is True

    def test_schema_to_dict(self):
        """Test converting schema to dictionary."""
        col = FlatColumn(name="id", type=OrsoTypes.INTEGER, identity="id")
        schema = RelationSchema(name="users", columns=[col], primary_key="id")
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
        schema = RelationSchema.from_dict(schema_dict)

        assert schema.name == "users"
        assert schema.num_columns == 2
        assert schema.column_names == ["id", "name"]
        assert schema.primary_key == "id"

    def test_schema_json_roundtrip(self):
        """Test to_json/from_json roundtrip."""
        col1 = FlatColumn(name="id", type=OrsoTypes.INTEGER, identity="id")
        col2 = FlatColumn(name="name", type=OrsoTypes.VARCHAR, identity="name")
        original = RelationSchema(name="users", columns=[col1, col2], primary_key="id")

        json_str = original.to_json()
        assert isinstance(json_str, str)
        restored = RelationSchema.from_json(json_str)

        assert restored.name == original.name
        assert restored.num_columns == original.num_columns
        assert restored.column_names == original.column_names
        assert restored.primary_key == original.primary_key

    def test_schema_find_column_alias(self):
        """Test find_column method (orso compatibility)."""
        col = FlatColumn(name="user_id", type=OrsoTypes.INTEGER, identity="user_id")
        schema = RelationSchema(name="users", columns=[col])

        found = schema.find_column("user_id")
        assert found is not None
        assert found.name == "user_id"

        not_found = schema.find_column("missing")
        assert not_found is None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
