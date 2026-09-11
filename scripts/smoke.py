"""Проверяем резервного провайдера, лимит бюджета и закрытие соединения при отмене клиента."""

import asyncio
import json
from uuid import uuid4

import httpx


async def login(client, email):
    response = await client.post(
        "/auth/login", json={"email": email, "password": "RelayLMDemo123!"}
    )
    response.raise_for_status()
    return {"Authorization": "Bearer " + response.json()["access_token"]}


async def mode(client, url, value):
    response = await client.post(
        url + "/control", json={"mode": value}, headers={"X-Control-Token": "local-demo-control"}
    )
    response.raise_for_status()


async def main():
    async with httpx.AsyncClient(base_url="http://localhost:8000", timeout=15) as client:
        email = f"smoke-{uuid4().hex}@example.com"
        user = (
            await client.post(
                "/auth/register", json={"email": email, "password": "RelayLMDemo123!"}
            )
        ).json()
        headers = await login(client, email)
        admin = await login(client, "demo@example.com")
        a = "http://provider-a:8000"
        b = "http://provider-b:8000"
        await mode(client, b, "normal")
        try:
            await mode(client, a, "reject429")
            body = {"messages": [{"role": "user", "content": "Hello"}], "max_tokens": 32}
            response = await client.post(
                "/v1/generations", json=body, headers={**headers, "Idempotency-Key": "fallback"}
            )
            response.raise_for_status()
            assert response.json()["provider"] == "backup"
            await mode(client, a, "stall_after")
            generation_id = None
            async with client.stream(
                "POST",
                "/v1/generations",
                json={**body, "stream": True},
                headers={**headers, "Idempotency-Key": "cancel"},
            ) as stream:
                async for line in stream.aiter_lines():
                    if line.startswith("data:"):
                        event = json.loads(line[5:])
                        if event["type"] == "start":
                            generation_id = event["id"]
                        if event["type"] == "delta":
                            break
            for _ in range(60):
                record = (await client.get("/generations/" + generation_id, headers=headers)).json()
                stats = (await client.get(a + "/stats")).json()
                if record["status"] == "cancelled" and stats["active"] == 0:
                    break
                await asyncio.sleep(0.1)
            assert record["status"] == "cancelled" and stats["active"] == 0, record
            usage = (await client.get("/usage", headers=headers)).json()
            assert usage["reserved"] == 0
            limited = await client.put(
                "/users/" + user["id"] + "/budget", json={"budget": usage["spent"]}, headers=admin
            )
            limited.raise_for_status()
            rejected = await client.post(
                "/v1/generations", json=body, headers={**headers, "Idempotency-Key": "budget"}
            )
            assert rejected.status_code == 402
            print(
                json.dumps(
                    {
                        "ok": True,
                        "fallback": "backup",
                        "client_cancelled": "upstream closed",
                        "budget_rejected": 402,
                        "reserved_after_cancel": 0,
                    },
                    indent=2,
                )
            )
        finally:
            await mode(client, a, "normal")
            await mode(client, b, "normal")


if __name__ == "__main__":
    asyncio.run(main())
