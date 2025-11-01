from collections import namedtuple
from aiohttp import ClientSession, ClientResponseError, ClientConnectorError
from aiohttp.web import HTTPException
from aiohttp.web_exceptions import HTTPRequestTimeout, HTTPError
from orjson import loads
import configs

# A named tuple to structure remote source errors for consistent error handling.
RemoteSourceError = namedtuple("RemoteSourceError", ["text", "error"])


async def get_data_from_source(
    site: str, url: str, parameters: dict = None, get_timeout=30
) -> tuple[int, dict | RemoteSourceError]:
    """
    A wrapper for making GET requests to an external service.

    This function handles various exceptions (timeouts, HTTP errors, connection errors)
    and returns a consistent tuple format: (status_code, data_or_error).

    Args:
        site: The base URL of the external service (e.g., "http://example.com").
        url: The specific path for the API endpoint (e.g., "/data").
        parameters: An optional dictionary of query parameters.
        get_timeout: The timeout for the request in seconds.

    Returns:
        A tuple containing the HTTP status code and either the JSON response as a
        dictionary or a RemoteSourceError named tuple.
    """
    try:
        async with ClientSession(site) as session:
            async with session.get(url, params=parameters, timeout=get_timeout) as resp:
                resp.raise_for_status()  # Raise an exception for bad status codes (4xx or 5xx)
                # Parse the JSON response
                r = await resp.json(encoding="utf-8", loads=loads, content_type=None)
    except (HTTPRequestTimeout, TimeoutError):
        return 408, RemoteSourceError(
            "Remote Source Request timed out", f"request timed out {get_timeout}s"
        )
    except HTTPError as http_err:
        return 500, RemoteSourceError(
            "Remote Source Error", f"HTTP error occurred: {http_err}"
        )
    except HTTPException as err:
        return 500, RemoteSourceError(
            "Remote Source Error", f"Other HTTP error occurred: {err}"
        )
    except ClientConnectorError as err:
        return 500, RemoteSourceError(
            "Connect to Remote Source error", f"connection error: {err}"
        )
    except ClientResponseError as err:
        return err.status, RemoteSourceError(
            "Id not found" if err.status == 404 else "Remote Response Error",
            f"Other Response Error: {err.message}",
        )
    except Exception as ex:
        # Catch any other unexpected exceptions
        return 500, RemoteSourceError("Remote Source Error", f"Other exception: {ex}")
    else:
        # If the request was successful
        if resp.ok:
            return resp.status, r
        else:
            return resp.status, RemoteSourceError(
                "Wrong Remote Source Response", resp.reason
            )


async def grouping_data(
    data: list[dict], tables_config: dict[str, list]
) -> dict[str, set[tuple]]:
    """
    Transforms a list of dictionaries into a dictionary of sets, organized by table.

    This function iterates through the raw data and the table schema, extracting the
    relevant fields for each table and grouping them into a set of tuples.

    Args:
        data: A list of dictionaries, where each dictionary is a row of data.
        tables_config: The schema defining the tables and their columns.

    Returns:
        A dictionary where keys are table names and values are sets of tuples,
        with each tuple representing a row of data for that table.
    """
    # Initialize a dictionary to hold the grouped data for each table.
    r = {table: set() for table in tables_config}
    table_columns = configs.tables_schema.items()
    # Iterate over each row in the input data.
    for row in data:
        # For each row, iterate over the tables defined in the schema.
        for table, columns in table_columns:
            # Extract the values for the columns of the current table and add them as a tuple.
            r[table].add(tuple(row.get(column) for column in columns))
    return r
