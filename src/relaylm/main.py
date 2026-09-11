import asyncio
from contextlib import asynccontextmanager, suppress
from typing import Literal
from uuid import UUID

from fastapi import Depends, FastAPI, Header, HTTPException, Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field, model_validator
from sqlalchemy import text

from . import ledger
from .auth import User, admin_user, current_user, router
from .config import settings
from .db import engine
from .gateway import encode, run
from .observability import instrument
from .providers import make_client


async def sweep():
    while True:
        try:
            await ledger.reap()
        except Exception:
            import logging

            logging.getLogger("reaper").exception("recovery_failed")
        await asyncio.sleep(1)


@asynccontextmanager
async def lifespan(app):
    async with make_client(settings.read_timeout) as client:
        app.state.http = client
        task = asyncio.create_task(sweep())
        try:
            yield
        finally:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task


app = FastAPI(title="RelayLM", version="0.1.0", lifespan=lifespan)
app.include_router(router)
instrument(app)


class Message(BaseModel):
    role: Literal["system", "user", "assistant"]
    content: str = Field(min_length=1, max_length=8000)


class BudgetInput(BaseModel):
    budget: int = Field(ge=0, le=1000000000)


class Generation(BaseModel):
    messages: list[Message] = Field(min_length=1, max_length=16)
    max_tokens: int = Field(default=128, ge=1, le=1024)
    stream: bool = False

    @model_validator(mode="after")
    def validate_messages(self):
        if self.messages[-1].role != "user":
            raise ValueError("Last message must be from user")
        if any(m.role == "system" for m in self.messages[1:]):
            raise ValueError("Only the first message may be system")
        if sum(len(m.content.encode()) for m in self.messages) > 16384:
            raise ValueError("Maximum 16 KiB of message text")
        return self


@app.get("/health")
async def health():
    async with engine.connect() as conn:
        await conn.execute(text("SELECT 1"))
    return {"status": "ok"}


@app.post("/v1/generations")
async def generate(
    data: Generation,
    idempotency_key: str = Header(min_length=1, max_length=128, pattern=r"^[A-Za-z0-9_.:-]+$"),
    user: User = Depends(current_user),
):
    await ledger.reap()
    body = data.model_dump()
    generation, replayed = await ledger.begin(user.id, idempotency_key, body, settings.providers)
    if replayed:
        if generation["status"] != "completed":
            raise HTTPException(
                409,
                {
                    "id": str(generation["id"]),
                    "status": generation["status"],
                    "message": "Previous attempt ended; use a new key for a new generation",
                },
            )
        if data.stream:

            async def replay():
                yield encode({"type": "start", "id": str(generation["id"]), "replayed": True})
                yield encode({"type": "delta", "text": generation["result"]["text"]})
                yield encode({"type": "done", **generation["result"], "replayed": True})

            return StreamingResponse(
                replay(), media_type="text/event-stream", headers={"Cache-Control": "no-store"}
            )
        return {**generation["result"], "replayed": True}
    events = run(generation, body, app.state.http)
    if data.stream:

        async def encoded():
            try:
                async for item in events:
                    yield encode(item)
            finally:
                await events.aclose()

        return StreamingResponse(
            encoded(),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-store", "X-Accel-Buffering": "no"},
        )
    async for item in events:
        if item["type"] == "done":
            return {key: value for key, value in item.items() if key != "type"}
        if item["type"] == "error":
            await events.aclose()
            raise HTTPException(502, {"id": str(generation["id"]), **item})
    raise HTTPException(502, "Generation did not finish")


@app.get("/usage")
async def usage(user: User = Depends(current_user)):
    async with engine.connect() as conn:
        row = (
            (
                await conn.execute(
                    text(
                        "SELECT budget,spent,reserved,budget-spent-reserved AS available FROM users WHERE id=:id"
                    ),
                    {"id": user.id},
                )
            )
            .mappings()
            .one()
        )
    return {"unit": "configured accounting credit", **dict(row)}


@app.put("/users/{user_id}/budget")
async def set_budget(user_id: UUID, data: BudgetInput, admin: User = Depends(admin_user)):
    async with engine.begin() as conn:
        row = (
            (
                await conn.execute(
                    text("SELECT spent,reserved FROM users WHERE id=:id FOR UPDATE"),
                    {"id": user_id},
                )
            )
            .mappings()
            .first()
        )
        if row is None:
            raise HTTPException(404, "User not found")
        if data.budget < row["spent"] + row["reserved"]:
            raise HTTPException(409, "Budget cannot be below spent and reserved amounts")
        await conn.execute(
            text("UPDATE users SET budget=:budget WHERE id=:id"),
            {"id": user_id, "budget": data.budget},
        )
    return {"budget": data.budget}


@app.get("/generations")
async def generations(
    limit: int = Query(default=30, ge=1, le=100), user: User = Depends(current_user)
):
    async with engine.connect() as conn:
        return [
            dict(r)
            for r in (
                await conn.execute(
                    text(
                        "SELECT id,status,reserve,charge,error,created_at FROM generations WHERE user_id=:id ORDER BY created_at DESC LIMIT :limit"
                    ),
                    {"id": user.id, "limit": limit},
                )
            ).mappings()
        ]


@app.get("/generations/{generation_id}")
async def generation_detail(generation_id: UUID, user: User = Depends(current_user)):
    async with engine.connect() as conn:
        row = (
            (
                await conn.execute(
                    text("SELECT * FROM generations WHERE id=:id AND user_id=:user"),
                    {"id": generation_id, "user": user.id},
                )
            )
            .mappings()
            .first()
        )
        if row is None:
            raise HTTPException(404, "Generation not found")
        attempts = [
            dict(r)
            for r in (
                await conn.execute(
                    text("SELECT * FROM attempts WHERE generation_id=:id ORDER BY started_at"),
                    {"id": generation_id},
                )
            ).mappings()
        ]
    return {**dict(row), "attempts": attempts}


@app.get("/providers")
async def provider_status(user: User = Depends(admin_user)):
    async with engine.connect() as conn:
        circuits = [
            dict(r)
            for r in (
                await conn.execute(text("SELECT * FROM circuits ORDER BY provider"))
            ).mappings()
        ]
    return {
        "providers": [
            {
                "name": p.name,
                "protocol": p.protocol,
                "model": p.model,
                "input_price": p.input_price,
                "output_price": p.output_price,
            }
            for p in settings.providers
        ],
        "circuits": circuits,
    }
