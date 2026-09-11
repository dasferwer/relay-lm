import asyncio

import httpx
import pytest
from conftest import OTHER, USER, ByteStream, headers
from fastapi import HTTPException
from sqlalchemy import text

from relaylm import circuit, ledger, providers
from relaylm.config import settings
from relaylm.db import engine
from relaylm.gateway import run

BODY = {"messages": [{"role": "user", "content": "hello"}], "max_tokens": 20, "stream": False}


async def generate(client, key="req-1", body=None):
    return await client.post("/v1/generations", json=body or BODY, headers={"Idempotency-Key": key})


async def test_normal_usage_and_idempotent_replay(client, upstream):
    result = await generate(client)
    assert result.status_code == 200 and result.json()["text"] == "hello"
    replay = await generate(client)
    assert replay.json()["replayed"] and len(upstream["calls"]) == 1
    usage = (await client.get("/usage")).json()
    assert usage["spent"] == 16 and usage["reserved"] == 0


async def test_fallback_429_and_anthropic_cumulative_usage(client, upstream):
    upstream["modes"]["provider-a"] = 429
    result = await generate(client)
    assert result.json()["provider"] == "backup" and result.json()["output_tokens"] == 2
    usage = (await client.get("/usage")).json()
    assert usage["spent"] == 28 and usage["reserved"] == 0


async def test_pre_token_timeout_falls_back_and_keeps_conservative_cost(client, upstream):
    upstream["modes"]["provider-a"] = "timeout"
    result = await generate(client)
    assert result.json()["provider"] == "backup"
    detail = (await client.get("/generations/" + result.json()["id"])).json()
    assert detail["attempts"][0]["accounting"] == "conservative_reserve"
    assert detail["charge"] == ledger.reserve_for(settings.providers[0], BODY) + 28


@pytest.mark.parametrize("mode", ["cut", "mid_timeout"])
async def test_no_fallback_after_first_text(client, upstream, mode):
    upstream["modes"]["provider-a"] = mode
    result = await generate(client, body={**BODY, "stream": True})
    assert '"type": "delta"' in result.text and '"type": "error"' in result.text
    assert '"type": "done"' not in result.text
    assert upstream["calls"] == ["provider-a"]
    assert (await client.get("/usage")).json()["reserved"] == 0


async def test_non_retryable_rejection_does_not_contact_backup(client, upstream):
    upstream["modes"]["provider-a"] = 401
    assert (await generate(client)).status_code == 502
    assert upstream["calls"] == ["provider-a"]
    assert (await client.get("/usage")).json()["spent"] == 0


async def test_budget_reservation_is_atomic_under_concurrency():
    cap = sum(ledger.reserve_for(p, BODY) for p in settings.providers)
    async with engine.begin() as conn:
        await conn.execute(
            text("UPDATE users SET budget=:cap WHERE id=:id"), {"id": USER, "cap": cap}
        )

    async def attempt(i):
        try:
            return await ledger.begin(USER, str(i), BODY, settings.providers)
        except HTTPException as error:
            return error.status_code

    results = await asyncio.gather(*(attempt(i) for i in range(12)))
    assert sum(isinstance(r, tuple) for r in results) == 1
    assert [r for r in results if isinstance(r, int)] == [402] * 11


async def test_duplicate_inflight_request_never_starts_twice():
    await ledger.begin(USER, "same", BODY, settings.providers)
    with pytest.raises(HTTPException) as error:
        await ledger.begin(USER, "same", BODY, settings.providers)
    assert error.value.status_code == 409


async def test_replay_with_changed_payload_is_rejected(client):
    await generate(client)
    assert (await generate(client, body={**BODY, "max_tokens": 21})).status_code == 409


async def test_cancel_after_first_delta_closes_upstream_and_settles(upstream):
    generation, _ = await ledger.begin(USER, "cancel", BODY, settings.providers)
    stream = run(generation, BODY, upstream["http"])
    assert (await anext(stream))["type"] == "start"
    assert (await anext(stream))["type"] == "delta"
    await stream.aclose()
    assert upstream["streams"][0].closed
    async with engine.connect() as conn:
        row = (await conn.execute(text("SELECT status,charge FROM generations"))).mappings().one()
        assert row["status"] == "cancelled" and row["charge"] > 0
        assert (
            await conn.execute(text("SELECT reserved FROM users WHERE id=:id"), {"id": USER})
        ).scalar_one() == 0
        assert (await conn.execute(text("SELECT failures FROM circuits"))).scalar_one() == 0


async def test_cancel_before_upstream_releases_entire_reserve(upstream):
    generation, _ = await ledger.begin(USER, "cancel", BODY, settings.providers)
    stream = run(generation, BODY, upstream["http"])
    await anext(stream)
    await stream.aclose()
    async with engine.connect() as conn:
        assert (await conn.execute(text("SELECT charge FROM generations"))).scalar_one() == 0


async def test_crash_recovery_charges_only_started_attempts():
    generation, _ = await ledger.begin(USER, "crash", BODY, settings.providers)
    attempt = await ledger.start_attempt(generation, settings.providers[0], BODY)
    async with engine.begin() as conn:
        await conn.execute(text("UPDATE generations SET lease_until=now()-interval '1 second'"))
    assert await ledger.reap() == 1
    assert await ledger.reap() == 0
    assert not await ledger.settle(generation, "completed", result={"late": True})
    async with engine.connect() as conn:
        row = (await conn.execute(text("SELECT status,charge FROM generations"))).mappings().one()
        assert row["status"] == "abandoned" and row["charge"] == attempt["cap"]


async def test_circuit_allows_only_one_probe_and_ignores_old_success():
    old = await circuit.acquire("primary")
    for _ in range(3):
        permit = await circuit.acquire("primary")
        await circuit.report(permit, False)
    await circuit.report(old, True)
    assert await circuit.acquire("primary") is None
    async with engine.begin() as conn:
        await conn.execute(text("UPDATE circuits SET open_until=now()-interval '1 second'"))
    permits = await asyncio.gather(*(circuit.acquire("primary") for _ in range(8)))
    assert sum(p is not None for p in permits) == 1
    await circuit.report(next(p for p in permits if p), True)
    assert await circuit.acquire("primary") is not None


async def test_rate_limit_counts_completed_requests(client, monkeypatch):
    monkeypatch.setattr(settings, "requests_per_minute", 2)
    await generate(client, "a")
    await generate(client, "b")
    assert (await generate(client, "c")).status_code == 429


async def test_private_usage_and_generation_history(client):
    first = await generate(client)
    assert (
        await client.get("/generations/" + first.json()["id"], headers=headers(OTHER))
    ).status_code == 404
    assert (await client.get("/generations", headers=headers(OTHER))).json() == []
    assert (await client.get("/providers")).status_code == 403


async def test_malformed_pre_token_response_can_fall_back(client, upstream):
    upstream["modes"]["provider-a"] = "bad"
    assert (await generate(client)).json()["provider"] == "backup"


async def test_stream_replay_does_not_call_provider(client, upstream):
    await generate(client, body={**BODY, "stream": True})
    result = await generate(client, body={**BODY, "stream": True})
    assert '"replayed": true' in result.text and len(upstream["calls"]) == 1


async def test_settlement_is_idempotent(client):
    generation, _ = await ledger.begin(USER, "settle", BODY, settings.providers)
    attempt = await ledger.start_attempt(generation, settings.providers[0], BODY)
    await ledger.finish_attempt(attempt, "completed", usage=(10, 2))
    results = await asyncio.gather(
        *(ledger.settle(generation, "completed", result={}) for _ in range(8))
    )
    assert sum(results) == 1
    assert (await client.get("/usage")).json()["spent"] == 16


async def test_sse_multiline_comments_and_size_limit():
    response = httpx.Response(200, stream=ByteStream([b': heartbeat\n\ndata: {"a":\ndata: 1}\n\n']))
    assert [s async for s in providers.sse_events(response)] == ['{"a":\n1}']
    oversized = httpx.Response(200, stream=ByteStream([b"data: " + b"a" * 65537 + b"\n\n"]))
    with pytest.raises(providers.ProviderError):
        _ = [s async for s in providers.sse_events(oversized)]


async def test_input_limits_and_required_idempotency(client):
    assert (await client.post("/v1/generations", json=BODY)).status_code == 422
    assert (await generate(client, body={**BODY, "max_tokens": 100000})).status_code == 422
    assert (
        await generate(
            client, body={**BODY, "messages": [{"role": "system", "content": "only system"}]}
        )
    ).status_code == 422


@pytest.mark.parametrize("host", ["provider-a", "provider-b"])
async def test_terminal_event_without_final_usage_is_not_success(client, upstream, host):
    if host == "provider-b":
        upstream["modes"]["provider-a"] = 429
    upstream["modes"][host] = "omit_usage"
    response = await generate(client)
    assert response.status_code == 502
    record = (await client.get("/generations")).json()[0]
    detail = (await client.get("/generations/" + record["id"])).json()
    assert detail["attempts"][-1]["accounting"] == "conservative_reserve"
    assert (await client.get("/usage")).json()["reserved"] == 0


async def test_only_admin_can_change_budget_and_cannot_remove_active_hold(client):
    path = f"/users/{USER}/budget"
    assert (await client.put(path, json={"budget": 2000000})).status_code == 403
    async with engine.begin() as conn:
        await conn.execute(text("UPDATE users SET role='admin' WHERE id=:id"), {"id": OTHER})
    generation, _ = await ledger.begin(USER, "held", BODY, settings.providers)
    assert (await client.put(path, json={"budget": 0}, headers=headers(OTHER))).status_code == 409
    assert (
        await client.put(path, json={"budget": generation["reserve"]}, headers=headers(OTHER))
    ).status_code == 200
    await ledger.settle(generation, "cancelled")
    assert (await client.put(path, json={"budget": 0}, headers=headers(OTHER))).status_code == 200
    assert (await generate(client)).status_code == 402


async def test_cancelled_probe_releases_slot_without_new_failure():
    for _ in range(3):
        await circuit.report(await circuit.acquire("primary"), False)
    async with engine.begin() as conn:
        await conn.execute(text("UPDATE circuits SET open_until=now()-interval '1 second'"))
    permit = await circuit.acquire("primary")
    await circuit.release(permit)
    assert await circuit.acquire("primary") is not None
    async with engine.connect() as conn:
        assert (await conn.execute(text("SELECT failures FROM circuits"))).scalar_one() == 3
