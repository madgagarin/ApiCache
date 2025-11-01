from time import time

from aiosqlite import connect
from security import sanitize_schema


async def get_last_updated_time() -> float | None:
    """
    Retrieves the timestamp of the last successful cache update.

    Returns:
        float | None: The epoch timestamp of the last update, or None if never updated.
    """
    async with connect("cache.db") as db:
        cursor = await db.cursor()
        # Ensure the metadata table exists
        await cursor.execute(
            "CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value REAL)"
        )
        await cursor.execute("SELECT value FROM metadata WHERE key = 'last_updated'")
        row = await cursor.fetchone()
        return row[0] if row else None


async def set_last_updated_time() -> None:
    """
    Records the current time as the last successful cache update timestamp.
    """
    async with connect("cache.db") as db:
        cursor = await db.cursor()
        await cursor.execute(
            "CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value REAL)"
        )
        # Insert or replace the timestamp
        await cursor.execute(
            "INSERT OR REPLACE INTO metadata (key, value) VALUES ('last_updated', ?)",
            (time(),),
        )
        await db.commit()


async def clear_and_write_data_to_db(
    tables_schema: dict[str, list], tables_data: dict[str, set[tuple]]
) -> dict[str, int]:
    """
    Rebuilds the database tables based on the provided schema and data.

    This function first sanitizes the schema, then drops all existing tables,
    recreates them, and populates them with the new data. It also creates a
    'main_table' to link all other tables for efficient querying.

    Args:
        tables_schema: A dictionary defining the tables and their columns.
        tables_data: A dictionary containing the data to be inserted.

    Returns:
        A dictionary with the count of rows added to each table.
    """
    tables_schema = sanitize_schema(tables_schema)
    async with connect("cache.db") as db:
        cursor = await db.cursor()

        # Drop all existing tables to ensure a clean slate
        await cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = await cursor.fetchall()
        for table in tables:
            await cursor.execute(f"DROP TABLE IF EXISTS {table[0]}")

        added_rows_count = {}
        # Create and populate tables based on the schema
        for table, columns in tables_schema.items():
            columns_definition = ", ".join([f"{col} TEXT" for col in columns])
            await cursor.execute(
                f"CREATE TABLE IF NOT EXISTS {table} ({columns_definition})"
            )
            added_rows_count[table] = 0
            if table in tables_data:
                for row in tables_data[table]:
                    placeholders = ", ".join(["?" for _ in columns])
                    await cursor.execute(
                        f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})",
                        row,
                    )
                    added_rows_count[table] += 1

        # Create a central 'main_table' to link all other tables by their primary keys
        all_foreign_keys = [
            f"{table}_{columns[0]} TEXT" for table, columns in tables_schema.items()
        ]
        await cursor.execute(
            f"CREATE TABLE IF NOT EXISTS main_table ({', '.join(all_foreign_keys)})"
        )

        # Populate the 'main_table'
        table_names = list(tables_schema.keys())
        primary_keys = [tables_schema[table][0] for table in table_names]
        for data_sets in zip(*[tables_data[table] for table in table_names]):
            insert_values = tuple(record[0] for record in data_sets)
            placeholders = ", ".join(["?"] * len(insert_values))
            foreign_key_columns = [
                f"{table}_{pk}" for table, pk in zip(table_names, primary_keys)
            ]
            await cursor.execute(
                f"INSERT INTO main_table ({', '.join(foreign_key_columns)}) VALUES ({placeholders})",
                insert_values,
            )

        await db.commit()

    await set_last_updated_time()
    return added_rows_count


async def extract_filter_columns(tables_schema: dict[str, list], filters: dict):
    """
    Maps filter keys from a request to their corresponding tables in the schema.

    Args:
        tables_schema: The current database schema.
        filters: A dictionary of filters from the request.

    Returns:
        A dictionary mapping each table to a list of its columns that are being filtered.
    """
    if not filters:
        return {table: [] for table in tables_schema}

    # Create a reverse mapping from column name to table name for quick lookups
    column_to_table = {
        column: table for table, columns in tables_schema.items() for column in columns
    }

    filter_columns = {table: [] for table in tables_schema}
    for column in filters.keys():
        if column in column_to_table:
            filter_columns[column_to_table[column]].append(column)

    if not any(filter_columns.values()):
        raise ValueError(
            "No matching columns found in the schema for the given filters."
        )
    return filter_columns


async def construct_sql_clauses(
    tables_schema: dict[str, list],
    filter_columns: dict,
    filters: dict,
    searchstring: str,
):
    """
    Dynamically constructs the SELECT, JOIN, and WHERE clauses for a query.

    Args:
        tables_schema: The current database schema.
        filter_columns: A mapping of tables to their filtered columns.
        filters: The filter values from the request.
        searchstring: The search string for full-text search.

    Returns:
        A tuple containing the components of the SQL query.
    """
    where_clauses = []
    filter_values = []
    select_clauses = []
    search_clauses = []
    join_clauses = []

    # Build WHERE clauses for specific column filters
    for table, columns in filter_columns.items():
        alias = table[:1]
        where_clauses.extend([f"{alias}.{col} = ?" for col in columns])
        filter_values.extend([filters[col] for col in columns])

    # Build SELECT, JOIN, and full-text search clauses for all tables in the schema
    for table, columns in tables_schema.items():
        alias = table[:1]
        select_clauses.extend([f"{alias}.{col}" for col in columns])
        search_clauses.extend([f"{alias}.{col} LIKE ?" for col in columns])
        primary_key = columns[0]
        join_clauses.append(
            f"JOIN {table} {alias} ON mt.{table}_{primary_key} = {alias}.{primary_key}"
        )

    # Add full-text search condition if a search string is provided
    if searchstring:
        where_clauses.append(f"({' OR '.join(search_clauses)})")
        filter_values.extend([f"%{searchstring}%"] * len(search_clauses))

    return select_clauses, join_clauses, where_clauses, filter_values


async def query_with_filters_and_search(
    tables_schema: dict[str, list],
    filters: dict | None = None,
    searchstring: str | None = None,
) -> list[dict]:
    """
    Queries the database with the given filters and search string.

    Args:
        tables_schema: The current database schema.
        filters: A dictionary of column filters.
        searchstring: The string for full-text search.

    Returns:
        A list of dictionaries representing the query results.
    """
    async with connect("cache.db") as db:
        # Return rows as dictionaries
        db.row_factory = lambda c, r: dict(zip([col[0] for col in c.description], r))
        cursor = await db.cursor()

        filter_columns = await extract_filter_columns(tables_schema, filters)
        (
            select_clauses,
            join_clauses,
            where_clauses,
            filter_values,
        ) = await construct_sql_clauses(
            tables_schema, filter_columns, filters, searchstring
        )

        # Construct the final query
        sql_query = f"""
            SELECT {", ".join(select_clauses)}
            FROM main_table mt
            {" ".join(join_clauses)}
        """
        if where_clauses:
            sql_query += f" WHERE {' AND '.join(where_clauses)}"
            await cursor.execute(sql_query, filter_values)
        else:
            await cursor.execute(sql_query)

        return await cursor.fetchall()
