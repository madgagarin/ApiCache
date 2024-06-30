from aiosqlite import connect


async def clear_and_write_data_to_db(
    tables_schema: dict[str, list], tables_data: dict[str, set[tuple]]
) -> dict[str, int]:
    async with connect("cache.db") as db:
        cursor = await db.cursor()
        await cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = await cursor.fetchall()
        for table in tables:
            await cursor.execute(f"DROP TABLE IF EXISTS {table[0]}")
        added_tables_count = 0
        added_rows_count = {}
        for table, columns in tables_schema.items():
            columns_definition = ", ".join([f"{col} TEXT" for col in columns])
            await cursor.execute(
                f"CREATE TABLE IF NOT EXISTS {table} ({columns_definition})"
            )
            added_tables_count += 1
            added_rows_count[table] = 0
            if table in tables_data:
                for row in tables_data[table]:
                    placeholders = ", ".join(["?" for _ in columns])
                    await cursor.execute(
                        f'INSERT INTO {table} ({", ".join(columns)}) VALUES ({placeholders})',
                        row,
                    )
                    added_rows_count[table] += 1
        all_foreign_keys = []
        for table, columns in tables_schema.items():
            primary_key = columns[0]
            all_foreign_keys.append(f"{table}_{primary_key} TEXT")
        await cursor.execute(
            f"""
            CREATE TABLE IF NOT EXISTS main_table (
                {" ,".join(all_foreign_keys)}
            )
        """
        )
        table_names = list(tables_schema.keys())
        primary_keys = [tables_schema[table][0] for table in table_names]
        for data_sets in zip(*[tables_data[table] for table in table_names]):
            insert_values = tuple(record[0] for record in data_sets)
            placeholders = ", ".join(["?"] * len(insert_values))
            await cursor.execute(
                f'INSERT INTO main_table ({", ".join([f"{table}_{pk}" for table, pk in zip(table_names, primary_keys)])}) VALUES ({placeholders})',
                insert_values,
            )
        await db.commit()
    return added_rows_count


async def extract_filter_columns(tables_schema: dict[str, list], filters: dict):
    if not filters:
        return {table: [] for table in tables_schema}
    column_to_table = {
        column: table for table, columns in tables_schema.items() for column in columns
    }
    filter_columns = {table: [] for table in tables_schema}
    for column in filters.keys():
        if column in column_to_table:
            filter_columns[column_to_table[column]].append(column)
    if not any(filter_columns.values()):
        raise ValueError("No matching columns found in the schema.")
    return filter_columns


async def construct_sql_clauses(
    tables_schema: dict[str, list],
    filter_columns: dict,
    filters: dict,
    searchstring: str,
):
    where_clauses = []
    filter_values = []
    select_clauses = []
    search_clauses = []
    join_clauses = []
    for table, columns in filter_columns.items():
        alias = table[:1]
        where_clauses.extend([f"{alias}.{col} = ?" for col in columns])
        filter_values.extend([filters[col] for col in columns])
    for table, columns in tables_schema.items():
        alias = table[:1]
        select_clauses.extend([f"{alias}.{col}" for col in columns])
        search_clauses.extend([f"{alias}.{col} LIKE ?" for col in columns])
        primary_key = columns[0]
        join_clauses.append(
            f"JOIN {table} {alias} ON mt.{table}_{primary_key} = {alias}.{primary_key}"
        )
    if searchstring:
        where_clauses.append(f"({' OR '.join(search_clauses)})")
        filter_values.extend([f"%{searchstring}%"] * len(search_clauses))
    return select_clauses, join_clauses, where_clauses, filter_values


async def query_with_filters_and_search(
    tables_schema: dict[str, list],
    filters: dict | None = None,
    searchstring: str | None = None,
) -> dict:
    async with connect("cache.db") as db:
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
        sql_query = f"""
            SELECT {', '.join(select_clauses)}
            FROM main_table mt
            {' '.join(join_clauses)}
        """
        if where_clauses:
            sql_query += f" WHERE {' AND '.join(where_clauses)}"
            await cursor.execute(sql_query, filter_values)
        else:
            await cursor.execute(sql_query)
        return await cursor.fetchall()
