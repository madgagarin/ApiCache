from asyncio import create_task
from time import time

from aiohttp.web_request import Request
from aiohttp.web_response import Response, json_response
from orjson import loads
from orjson.orjson import dumps

from storage import (
    clear_and_write_data_to_db,
    query_with_filters_and_search,
    get_last_updated_time,
)
from tools import get_data_from_source, grouping_data
import configs


async def update_cache():
    """
    Fetches data from the external source, processes it according to the schema,
    and writes it to the database. This function orchestrates the entire
    cache update process.
    """
    # Fetch raw data from the configured source URL
    status, result = await get_data_from_source(
        configs.source_url,
        configs.source_path,
    )
    if status != 200:
        return status, result.text, result.error

    # Group the flat data structure into sets of tuples based on the table schema
    result = await grouping_data(result, configs.tables_schema)

    # Set a flag to indicate that the database is being rebuilt
    configs.base_rebuild = True
    # Clear old data and write the new data to the database
    data_recorded = await clear_and_write_data_to_db(configs.tables_schema, result)
    # Unset the rebuild flag
    configs.base_rebuild = False

    # Prepare a summary of the recorded data for the response
    data_recorded_text = "\n".join([f"{t}: {rc}" for t, rc in data_recorded.items()])
    tables_schema_text = "\n".join(
        [f"{t}: {c}" for t, c in configs.tables_schema.items()]
    )
    return (
        200,
        f"OK\n\nDB configuration:\n{tables_schema_text}\n\nRecorded:\n{data_recorded_text}",
        None,
    )


async def post_update_router(request: Request) -> Response:
    """
    Handles POST /update.
    Updates the global table schema from the request JSON and triggers a cache update.
    """
    configs.tables_schema = await request.json(loads=loads)
    status, text, reason = await update_cache()
    return Response(status=status, text=text, reason=reason)


async def get_update_router(_request: Request) -> Response:
    """
    Handles GET /update.
    Triggers a cache update using the existing schema.
    """
    status, text, reason = await update_cache()
    return Response(status=status, text=text, reason=reason)


async def get_data_router(request: Request) -> Response:
    """
    Handles GET / and GET /{search_text}.
    Checks cache validity (TTL) and triggers a background update if stale.
    Returns filtered/searched data from the cache.
    """
    # Check if the cache is stale
    last_updated = await get_last_updated_time()
    if not last_updated or (time() - last_updated > configs.cache_ttl_seconds):
        # Trigger a non-blocking background task to update the cache
        create_task(update_cache())

    # If a rebuild is in progress, return a 503 Service Unavailable status
    if configs.base_rebuild:
        return Response(
            status=503, text="Rebuilding cache data, please try again later."
        )

    # Query the database with optional search text
    data = await query_with_filters_and_search(
        configs.tables_schema, searchstring=request.match_info.get("search_text")
    )
    return json_response(data=data, dumps=lambda x: dumps(x).decode())


async def post_form_data_router(request: Request) -> Response:
    """
    Handles POST /.
    Checks cache validity (TTL) and triggers a background update if stale.
    Returns data from the cache, filtered by form data and an optional search string.
    """
    # Check if the cache is stale
    last_updated = await get_last_updated_time()
    if not last_updated or (time() - last_updated > configs.cache_ttl_seconds):
        # Trigger a non-blocking background task to update the cache
        create_task(update_cache())

    # If a rebuild is in progress, return a 503 Service Unavailable status
    if configs.base_rebuild:
        return Response(
            status=503, text="Rebuilding cache data, please try again later."
        )

    # Extract filters and search string from the POST data
    filters = dict(await request.post())
    searchstring = filters.pop("searchstring", None)
    try:
        data = await query_with_filters_and_search(
            configs.tables_schema, filters=filters, searchstring=searchstring
        )
    except ValueError as er:
        return Response(status=404, text=str(er))
    return json_response(data=data, dumps=lambda x: dumps(x).decode())


async def health_router(_request: Request) -> Response:
    """
    Handles GET /health.
    A simple health check endpoint.
    """
    return Response(text="HEALTHY")
