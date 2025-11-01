from unittest.mock import patch
from aiohttp.test_utils import AioHTTPTestCase
import os

from main import Application
from routers import (
    health_router,
    get_data_router,
    post_form_data_router,
    get_update_router,
    post_update_router,
)

# Set dummy env vars for tests
os.environ["SOURCE_URL"] = "dummy.url"
os.environ["SOURCE_PATH"] = "/dummy"


class MyAppTestCase(AioHTTPTestCase):
    async def get_application(self):
        app = Application()
        app.router.add_get("/health", health_router)
        app.router.add_get("/", get_data_router)
        app.router.add_get("/{search_text}", get_data_router)
        app.router.add_post("/", post_form_data_router)
        app.router.add_get("/update", get_update_router)
        app.router.add_post("/update", post_update_router)
        return app

    async def test_health(self):
        resp = await self.client.request("GET", "/health")
        assert resp.status == 200
        text = await resp.text()
        assert "HEALTHY" in text

    @patch("routers.get_data_from_source")
    async def test_update_flow(self, mock_get_data):
        # Mock the external API response
        mock_get_data.return_value = (
            200,
            [
                {"user_id": "1", "username": "test"},
                {"user_id": "2", "username": "test2"},
            ],
        )

        # 1. Post a new schema to trigger an update
        schema = {"users": ["user_id", "username"]}
        resp = await self.client.request("POST", "/update", json=schema)
        assert resp.status == 200
        text = await resp.text()
        assert "OK" in text
        assert "users: 2" in text

        # 2. Verify that the data is now in the cache
        resp = await self.client.request("GET", "/")
        assert resp.status == 200
        data = await resp.json()
        assert len(data) == 2
        # Sort by user_id to ensure consistent order
        data.sort(key=lambda x: x["user_id"])
        assert data[0]["username"] == "test"
        assert data[1]["username"] == "test2"

        # 3. Test search
        resp = await self.client.request("GET", "/test2")
        assert resp.status == 200
        data = await resp.json()
        assert len(data) == 1
        assert data[0]["username"] == "test2"

        # 4. Test filtering
        resp = await self.client.request("POST", "/", data={"username": "test"})
        assert resp.status == 200
        data = await resp.json()
        assert len(data) == 1
        assert data[0]["username"] == "test"
