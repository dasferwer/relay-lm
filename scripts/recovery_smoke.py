"""После SIGKILL API восстанавливаем учёт незавершённой генерации из PostgreSQL."""

import asyncio
import json
import subprocess
import time
from pathlib import Path
from uuid import uuid4

import httpx
from smoke import login, mode

ROOT = Path(__file__).resolve().parents[1]


def compose(*args):
    subprocess.run(["docker", "compose", *args], cwd=ROOT, check=True, capture_output=True)


async def main():
    async with httpx.AsyncClient(base_url="http://localhost:8180", timeout=15) as client:
        email = f"recovery-{uuid4().hex}@example.com"
        await client.post("/auth/register", json={"email": email, "password": "RelayLMDemo123!"})
        headers = await login(client, email)
        body = {
            "messages": [{"role": "user", "content": "Recovery"}],
            "max_tokens": 32,
            "stream": True,
        }
        a = "http://localhost:8181"
        started = time.monotonic()
        try:
            await mode(client, a, "stall_after")
            async with client.stream(
                "POST",
                "/v1/generations",
                json=body,
                headers={**headers, "Idempotency-Key": "crash"},
            ) as response:
                async for line in response.aiter_lines():
                    if line.startswith("data:"):
                        event = json.loads(line[5:])
                        if event["type"] == "start":
                            generation_id = event["id"]
                        if event["type"] == "delta":
                            await asyncio.to_thread(compose, "kill", "--signal", "SIGKILL", "api")
                            break
        finally:
            await mode(client, a, "normal")
            await asyncio.to_thread(
                compose, "up", "-d", "--no-deps", "--wait", "--wait-timeout", "60", "api"
            )
        for _ in range(130):
            record = (await client.get("/generations/" + generation_id, headers=headers)).json()
            if record["status"] == "abandoned":
                break
            await asyncio.sleep(1)
        assert record["status"] == "abandoned" and len(record["attempts"]) == 1, record
        assert record["charge"] == record["attempts"][0]["cap"]
        usage = (await client.get("/usage", headers=headers)).json()
        assert usage["reserved"] == 0
        replay = await client.post(
            "/v1/generations", json=body, headers={**headers, "Idempotency-Key": "crash"}
        )
        assert replay.status_code == 409
        print(
            json.dumps(
                {
                    "ok": True,
                    "fault": "SIGKILL api during SSE",
                    "status": "abandoned",
                    "charged_started_attempts": 1,
                    "reserved_after_recovery": 0,
                    "duplicate_generation_rejected": 409,
                    "elapsed_seconds": round(time.monotonic() - started, 2),
                },
                indent=2,
            )
        )


if __name__ == "__main__":
    asyncio.run(main())
