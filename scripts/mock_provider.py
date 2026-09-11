"""Локальный стенд воспроизводит два SSE-протокола и управляемые отказы без платных API."""

import asyncio
import json
from typing import Literal

from fastapi import FastAPI, Header, HTTPException
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel

app = FastAPI(title="Local LLM protocol simulator")
state = {"mode": "normal", "requests": 0, "active": 0, "interrupted": 0}


class Control(BaseModel):
    mode: Literal[
        "normal",
        "reject429",
        "reject503",
        "reject401",
        "stall_before",
        "stall_after",
        "cut_after",
        "omit_usage",
    ]


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/stats")
async def stats():
    return dict(state)


@app.post("/control")
async def control(data: Control, x_control_token: str = Header(default="")):
    if x_control_token != "local-demo-control":
        raise HTTPException(403, "Control token required")
    state["mode"] = data.mode
    return dict(state)


def frame(value):
    return "data: " + (value if isinstance(value, str) else json.dumps(value)) + "\n\n"


async def response(body, protocol):
    state["requests"] += 1
    mode = state["mode"]
    if mode.startswith("reject"):
        return JSONResponse({"error": "simulated"}, status_code=int(mode[-3:]))
    words = ["Local", "simulated", "response", "for", "reliable", "streaming.", ""][
        : body.get("max_tokens", body.get("max_completion_tokens", 128))
    ]
    input_tokens = sum(len(m["content"].encode()) for m in body["messages"]) // 4 + 4

    async def generate():
        state["active"] += 1
        finished = False
        try:
            if mode == "stall_before":
                await asyncio.sleep(120)
            if protocol == "anthropic":
                yield frame(
                    {
                        "type": "message_start",
                        "message": {"usage": {"input_tokens": input_tokens, "output_tokens": 1}},
                    }
                )
            for i, word in enumerate(words):
                await asyncio.sleep(0.05)
                if protocol == "openai":
                    yield frame({"choices": [{"delta": {"content": word + " "}}]})
                else:
                    yield frame(
                        {
                            "type": "content_block_delta",
                            "delta": {"type": "text_delta", "text": word + " "},
                        }
                    )
                if i == 0 and mode == "stall_after":
                    await asyncio.sleep(120)
                if i == 0 and mode == "cut_after":
                    return
            if mode != "omit_usage":
                if protocol == "openai":
                    yield frame(
                        {
                            "choices": [],
                            "usage": {
                                "prompt_tokens": input_tokens,
                                "completion_tokens": len(words),
                            },
                        }
                    )
                else:
                    yield frame({"type": "message_delta", "usage": {"output_tokens": len(words)}})
            yield frame("[DONE]" if protocol == "openai" else {"type": "message_stop"})
            finished = True
        finally:
            state["active"] -= 1
            if not finished:
                state["interrupted"] += 1

    return StreamingResponse(generate(), media_type="text/event-stream")


@app.post("/v1/chat/completions")
async def openai(body: dict):
    return await response(body, "openai")


@app.post("/v1/messages")
async def anthropic(body: dict):
    return await response(body, "anthropic")
