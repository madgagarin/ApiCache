from aiohttp.web_request import Request
from aiohttp.web_response import Response, json_response
from orjson import loads
from orjson.orjson import dumps

from storage import clear_and_write_data_to_db, query_with_filters_and_search
from tools import get_data_from_source, grouping_data
import configs


async def post_update_router(request: Request) -> Response:
    configs.tables_schema = await request.json(loads=loads)
    return await get_update_router(request)


async def get_update_router(request: Request) -> Response:
    status, result = await get_data_from_source(
        configs.source_url,
        configs.source_path,
    )
    if status != 200:
        return Response(status=status, text=result.text, reason=result.error)
    result = await grouping_data(result, configs.tables_schema)
    configs.base_rebuild = True
    data_recorded = await clear_and_write_data_to_db(configs.tables_schema, result)
    configs.base_rebuild = False
    data_recorded_text = "\n".join([f"{t}: {rc}" for t, rc in data_recorded.items()])
    tables_schema = "\n".join([f"{t}: {c}" for t, c in configs.tables_schema.items()])
    return Response(
        text=f"OK\n\nDB configuration:\n{tables_schema}\n\nRecorded:\n{data_recorded_text}"
    )


async def get_data_router(request: Request) -> Response:
    if not configs.base_rebuild:
        data = await query_with_filters_and_search(
            configs.tables_schema, searchstring=request.match_info.get("search_text")
        )
        return json_response(data=data, dumps=lambda x: dumps(x).decode())
    Response(status=503, text="Rebuild data")


async def post_form_data_router(request: Request) -> Response:
    if not configs.base_rebuild:
        filters = dict(await request.post())
        searchstring = filters.pop("searchstring", None)
        try:
            data = await query_with_filters_and_search(
                configs.tables_schema, filters=filters, searchstring=searchstring
            )
        except ValueError as er:
            return Response(status=404, text=repr(er))
        return json_response(data=data, dumps=lambda x: dumps(x).decode())
    Response(status=503, text="Rebuild data")


async def health_router(request: Request) -> Response:
    return Response(text="HEALTHY")
