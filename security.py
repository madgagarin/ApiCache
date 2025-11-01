from re import sub


def sanitize_identifier(identifier: str) -> str:
    """
    Sanitizes a single string to make it a safe SQL identifier.

    This function prevents SQL injection by ensuring that table and column names
    adhere to a safe format. It performs the following operations:
    - Replaces any character that is not a letter, number, or underscore with an underscore.
    - Ensures the identifier is not empty or just whitespace.
    - Prepends an underscore if the identifier starts with a digit.

    Args:
        identifier: The string to be sanitized.

    Returns:
        The sanitized string, safe for use as an SQL identifier.
    """
    if not isinstance(identifier, str) or not identifier.strip():
        raise ValueError("Identifier cannot be empty.")

    # Replace any non-alphanumeric characters (excluding underscore) with an underscore.
    sanitized = sub(r"[^a-zA-Z0-9_]", "_", identifier)

    # SQL identifiers cannot start with a digit, so prepend an underscore if they do.
    if sanitized and sanitized[0].isdigit():
        sanitized = "_" + sanitized

    return sanitized


def sanitize_schema(schema: dict[str, list[str]]) -> dict[str, list[str]]:
    """
    Sanitizes all table and column names within a given schema dictionary.

    This function iterates through the user-provided schema and applies the
    `sanitize_identifier` function to every table name and column name,
    ensuring the entire schema is safe before it's used to construct SQL queries.

    Args:
        schema: A dictionary representing the database schema, where keys are
                table names and values are lists of column names.

    Returns:
        A new dictionary with all identifiers sanitized.
    """
    if not isinstance(schema, dict):
        raise ValueError("Schema must be a dictionary.")

    sanitized_schema = {}
    for table, columns in schema.items():
        sane_table = sanitize_identifier(table)
        if not isinstance(columns, list):
            raise ValueError(f"Columns for table '{table}' must be a list.")
        sane_columns = [sanitize_identifier(col) for col in columns]
        sanitized_schema[sane_table] = sane_columns

    return sanitized_schema
