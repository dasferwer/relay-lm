import json

import pytest
from conftest import frames
from sqlalchemy import text
from test_gateway import BODY, generate

from relaylm import ledger
from relaylm.config import settings
from relaylm.db import engine

CASES = [
    ("openai", {"usage": {"prompt_tokens": 2147483648, "completion_tokens": 2}}),
    ("openai", {"usage": {"prompt_tokens": 10, "completion_tokens": 2147483648}}),
    (
        "anthropic",
        {
            "type": "message_start",
            "message": {
                "usage": {
                    "input_tokens": 1073741824,
                    "cache_read_input_tokens": 1073741824,
                    "output_tokens": 0,
                }
            },
        },
    ),
    ("anthropic", {"type": "message_delta", "usage": {"output_tokens": 2147483648}}),
    ("openai", None),
    ("openai", []),
    ("openai", 7),
    ("openai", {"choices": None}),
    ("openai", {"choices": {}}),
    ("openai", {"choices": [None]}),
    ("openai", {"choices": [{"delta": None}]}),
    ("openai", {"choices": [{"delta": {"content": {"text": "bad"}}}]}),
    ("openai", {"choices": [{"delta": {"content": False}}]}),
    ("openai", {"choices": [{"delta": {"content": []}}]}),
    ("openai", {"usage": []}),
    ("openai", {"usage": {}}),
    ("openai", {"usage": {"prompt_tokens": True, "completion_tokens": 2}}),
    ("openai", {"usage": {"prompt_tokens": 1.5, "completion_tokens": 2}}),
    ("openai", {"usage": {"prompt_tokens": -1, "completion_tokens": 2}}),
    ("openai", {"choices": [{"delta": {"content": "bad"}}, None]}),
    ("anthropic", None),
    ("anthropic", []),
    ("anthropic", "bad"),
    ("anthropic", {"type": "message_start", "message": None}),
    ("anthropic", {"type": "message_start", "message": {"usage": None}}),
    (
        "anthropic",
        {"type": "message_start", "message": {"usage": {"input_tokens": True, "output_tokens": 0}}},
    ),
    ("anthropic", {"type": "content_block_delta", "delta": None}),
    ("anthropic", {"type": "content_block_delta", "delta": {"type": "text_delta", "text": {}}}),
    ("anthropic", {"type": "content_block_delta", "delta": {"type": "text_delta", "text": False}}),
    ("anthropic", {"type": "message_delta", "usage": []}),
    ("anthropic", {"type": "message_delta", "usage": {"output_tokens": 1.5}}),
]


def frame(event):
    return ("data: " + json.dumps(event) + "\n\n").encode()


@pytest.mark.parametrize("protocol,event", CASES)
@pytest.mark.parametrize("after_text", [False, True])
async def test_malformed_structure_settles_and_obeys_fallback_boundary(
    client, upstream, monkeypatch, protocol, event, after_text
):
    monkeypatch.setattr(settings.providers[0], "protocol", protocol)
    good = frames(protocol)
    prefix = good[: 1 if protocol == "openai" else 2] if after_text else []
    upstream["modes"]["provider-a"] = prefix + [frame(event)] + good
    response = await generate(client, body={**BODY, "stream": True})
    events = [
        json.loads(line[6:]) for line in response.text.splitlines() if line.startswith("data: ")
    ]
    assert response.status_code == 200
    assert [e["text"] for e in events if e["type"] == "delta"] == ["hello"]
    assert upstream["calls"] == (["provider-a"] if after_text else ["provider-a", "provider-b"])
    assert events[-1]["type"] == ("error" if after_text else "done")
    assert sum(e["type"] == "usage" for e in events) == (0 if after_text else 1)
    assert all(s.closed for s in upstream["streams"])
    usage = (await client.get("/usage")).json()
    cap = ledger.reserve_for(settings.providers[0], BODY)
    assert usage["reserved"] == 0 and usage["spent"] == cap + (0 if after_text else 28)
    async with engine.connect() as conn:
        row = (
            (
                await conn.execute(
                    text("SELECT status,error FROM attempts ORDER BY started_at LIMIT 1")
                )
            )
            .mappings()
            .one()
        )
        assert row["status"] == "failed" and row["error"] == "ProviderError"
        assert (
            await conn.execute(text("SELECT failures FROM circuits WHERE provider='primary'"))
        ).scalar_one() == 1
    # Нормальная следующая генерация подтверждает освобождение permit/резерва.
    upstream["modes"]["provider-a"] = good
    assert (await generate(client, key="next")).status_code == 200


@pytest.mark.parametrize("protocol", ["openai", "anthropic"])
async def test_permitted_empty_and_extension_events(client, upstream, monkeypatch, protocol):
    monkeypatch.setattr(settings.providers[0], "protocol", protocol)
    extras = (
        [
            {"choices": [{"delta": {"role": "assistant", "content": None}}], "usage": None},
            {"choices": [{"delta": {"content": ""}}]},
        ]
        if protocol == "openai"
        else [
            {"type": "ping"},
            {"type": "future_extension", "value": {}},
            {"type": "content_block_delta", "delta": {"type": "text_delta", "text": ""}},
        ]
    )
    upstream["modes"]["provider-a"] = [frame(e) for e in extras] + frames(protocol)
    result = await generate(client)
    assert result.status_code == 200 and result.json()["text"] == "hello"
    assert result.json()["input_tokens"] == 10 and result.json()["output_tokens"] == 2
    assert upstream["calls"] == ["provider-a"]
