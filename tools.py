from collections import namedtuple
from aiohttp import ClientSession, ClientResponseError, ClientConnectorError
from aiohttp.web import HTTPException
from aiohttp.web_exceptions import HTTPRequestTimeout, HTTPError
from orjson import loads
import configs

RemoteSourceError = namedtuple("RemoteSourceError", ["text", "error"])


async def get_data_from_source(
    site: str, url: str, parameters: dict = None, get_timeout=30
) -> (int, dict | RemoteSourceError):
    """A wrapper for a get request to an external service,
    returns a status code and a dictionary with data or an exception"""
    try:
        async with ClientSession(site) as session:
            async with session.get(url, params=parameters, timeout=get_timeout) as resp:
                resp.raise_for_status()
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
        return 500, RemoteSourceError("Remote Source Error", f"Other exception: {ex}")
    else:
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
    Grouping data into tables according to configuration
    """
    r = {table: set() for table in tables_config}
    table_columns = configs.tables_schema.items()
    for row in data:
        for table, columns in table_columns:
            r[table].add(tuple(row.get(column) for column in columns))
    return r
