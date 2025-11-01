import pytest
from security import sanitize_identifier, sanitize_schema


def test_sanitize_identifier_valid():
    assert sanitize_identifier("valid_name") == "valid_name"
    assert sanitize_identifier("another_valid_name_123") == "another_valid_name_123"


def test_sanitize_identifier_invalid_chars():
    assert sanitize_identifier("invalid-name") == "invalid_name"
    assert sanitize_identifier("name with spaces") == "name_with_spaces"
    assert sanitize_identifier("name.with.dots") == "name_with_dots"


def test_sanitize_identifier_starts_with_digit():
    assert sanitize_identifier("1name") == "_1name"


def test_sanitize_identifier_empty():
    with pytest.raises(ValueError):
        sanitize_identifier("")
    with pytest.raises(ValueError):
        sanitize_identifier("   ")


def test_sanitize_schema_valid():
    schema = {"users": ["user_id", "username"], "products": ["product_id", "name"]}
    sanitized = sanitize_schema(schema)
    assert sanitized == schema


def test_sanitize_schema_invalid():
    schema = {
        "users-table": ["user-id", "user name"],
        "1products": ["product.id", "product name"],
    }
    sanitized = sanitize_schema(schema)
    assert sanitized == {
        "users_table": ["user_id", "user_name"],
        "_1products": ["product_id", "product_name"],
    }


def test_sanitize_schema_invalid_input():
    with pytest.raises(ValueError):
        sanitize_schema("not a dict")
    with pytest.raises(ValueError):
        sanitize_schema({"users": "not a list"})
