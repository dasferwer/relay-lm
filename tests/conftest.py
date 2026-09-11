import json
import os
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID

import httpx
import jwt
import pytest_asyncio
from sqlalchemy import text

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from relaylm.config import settings  # noqa: E402
from relaylm.db import engine  # noqa: E402
from relaylm.main import app  # noqa: E402

USER = UUID("18000000-0000-0000-0000-000000000101")
OTHER = UUID("18000000-0000-0000-0000-000000000102")


def headers(user=USER):
    now = datetime.now(UTC)
    token = jwt.encode(
        {
            "sub": str(user),
            "iat": now,
            "exp": now + timedelta(minutes=5),
            "iss": "relaylm",
            "aud": "relaylm",
        },
        settings.jwt_secret,
        algorithm="HS256",
    )
    return {"Authorization": "Bearer " + token}


@pytest_asyncio.fixture(autouse=True)
async def database():
    if os.environ.get("TESTING") != "true" or not settings.database_url.endswith("/relaylm_test"):
        raise RuntimeError("Isolated test database required")
    async with engine.begin() as conn:
        await conn.execute(
            text("TRUNCATE users,generations,attempts,rate_windows,circuits CASCADE")
        )
        for user in [USER, OTHER]:
            await conn.execute(
                text("INSERT INTO users(id,email,password_hash) VALUES(:id,:email,'unused')"),
                {"id": user, "email": str(user) + "@example.com"},
            )


def frames(protocol):
    if protocol == "openai":
        rows = [
            {"choices": [{"delta": {"content": "hello"}}]},
            {"choices": [], "usage": {"prompt_tokens": 10, "completion_tokens": 2}},
            "[DONE]",
        ]
    else:
        rows = [
            {
                "type": "message_start",
                "message": {"usage": {"input_tokens": 10, "output_tokens": 1}},
            },
            {"type": "content_block_delta", "delta": {"type": "text_delta", "text": "hello"}},
            {"type": "message_delta", "usage": {"output_tokens": 1}},
            {"type": "message_delta", "usage": {"output_tokens": 2}},
            {"type": "message_stop"},
        ]
    return [
        ("data: " + (r if isinstance(r, str) else json.dumps(r)) + "\n\n").encode() for r in rows
    ]


class ByteStream(httpx.AsyncByteStream):
    def __init__(self, chunks):
        self.chunks = chunks
        self.closed = False

    async def __aiter__(self):
        for chunk in self.chunks:
            if isinstance(chunk, Exception):
                raise chunk
            yield chunk

    async def aclose(self):
        self.closed = True


@pytest_asyncio.fixture
async def upstream():
    state = {"calls": [], "modes": {}, "streams": []}

    async def handler(request):
        host = request.url.host
        state["calls"].append(host)
        mode = state["modes"].get(host, "ok")
        if isinstance(mode, int):
            return httpx.Response(mode, json={"error": "test"})
        protocol = "openai" if host == "provider-a" else "anthropic"
        chunks = frames(protocol)
        if mode == "cut":
            chunks = chunks[:1]
        if mode == "timeout":
            chunks = [httpx.ReadTimeout("test")]
        if mode == "mid_timeout":
            chunks = chunks[:1] + [httpx.ReadTimeout("test")]
        if mode == "bad":
            chunks = [b"data: invalid-json\n\n"]
        if mode == "omit_usage":
            chunks = [
                chunk
                for chunk in chunks
                if b"message_delta" not in chunk and b"prompt_tokens" not in chunk
            ]
        stream = ByteStream(chunks)
        state["streams"].append(stream)
        return httpx.Response(200, headers={"Content-Type": "text/event-stream"}, stream=stream)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as transport:
        app.state.http = transport
        state["http"] = transport
        yield state


@pytest_asyncio.fixture
async def client(upstream):
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test", headers=headers()
    ) as c:
        yield c
