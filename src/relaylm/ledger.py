import hashlib
import json
from uuid import uuid4

from fastapi import HTTPException
from sqlalchemy import text

from .config import settings
from .db import engine


def body_hash(body):
    return hashlib.sha256(
        json.dumps(body, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def estimate_input(messages):
    return sum(len(m["content"].encode()) + 64 for m in messages) + 64


def reserve_for(provider, body):
    return (
        estimate_input(body["messages"]) * provider.input_price
        + body["max_tokens"] * provider.output_price
    )


async def begin(user_id, key, body, providers):
    digest = body_hash(body)
    reserve = sum(reserve_for(p, body) for p in providers)
    async with engine.begin() as conn:
        user = (
            (
                await conn.execute(
                    text("SELECT * FROM users WHERE id=:id FOR UPDATE"), {"id": user_id}
                )
            )
            .mappings()
            .one()
        )
        previous = (
            (
                await conn.execute(
                    text("SELECT * FROM generations WHERE user_id=:user AND request_key=:key"),
                    {"user": user_id, "key": key},
                )
            )
            .mappings()
            .first()
        )
        if previous:
            if previous["body_hash"] != digest:
                raise HTTPException(409, "Idempotency key has different payload")
            if previous["status"] == "running":
                raise HTTPException(409, "Generation is still running")
            return dict(previous), True
        running = (
            await conn.execute(
                text("SELECT count(*) FROM generations WHERE user_id=:id AND status='running'"),
                {"id": user_id},
            )
        ).scalar_one()
        if running >= settings.max_concurrent:
            raise HTTPException(
                429, "Concurrent generation limit reached", headers={"Retry-After": "1"}
            )
        if user["spent"] + user["reserved"] + reserve > user["budget"]:
            raise HTTPException(402, "Budget cannot cover the configured provider attempts")
        window = (
            await conn.execute(
                text("""
            INSERT INTO rate_windows(user_id,minute,requests) VALUES(:id,date_trunc('minute',now()),1)
            ON CONFLICT(user_id,minute) DO UPDATE SET requests=rate_windows.requests+1 RETURNING requests
        """),
                {"id": user_id},
            )
        ).scalar_one()
        if window > settings.requests_per_minute:
            raise HTTPException(
                429, "Requests per minute limit reached", headers={"Retry-After": "60"}
            )
        await conn.execute(
            text("UPDATE users SET reserved=reserved+:reserve WHERE id=:id"),
            {"id": user_id, "reserve": reserve},
        )
        row = (
            (
                await conn.execute(
                    text("""
            INSERT INTO generations(id,user_id,request_key,body_hash,reserve,lease_until)
            VALUES(:id,:user,:key,:hash,:reserve,now()+(:lease * interval '1 second')) RETURNING *
        """),
                    {
                        "id": uuid4(),
                        "user": user_id,
                        "key": key,
                        "hash": digest,
                        "reserve": reserve,
                        "lease": settings.request_lease,
                    },
                )
            )
            .mappings()
            .one()
        )
    return dict(row), False


async def start_attempt(generation, provider, body):
    async with engine.begin() as conn:
        row = (
            (
                await conn.execute(
                    text("""
            INSERT INTO attempts(id,generation_id,provider,input_price,output_price,cap,price_version)
            SELECT :id,:generation,:provider,:input_price,:output_price,:cap,:price_version
            FROM generations WHERE id=:generation AND status='running' AND lease_until>now() RETURNING *
        """),
                    {
                        "id": uuid4(),
                        "generation": generation["id"],
                        "provider": provider.name,
                        "input_price": provider.input_price,
                        "output_price": provider.output_price,
                        "price_version": provider.price_version,
                        "cap": reserve_for(provider, body),
                    },
                )
            )
            .mappings()
            .first()
        )
    if row is None:
        raise RuntimeError("Generation lease expired")
    return dict(row)


async def finish_attempt(attempt, status, usage=None, error=None, definite_rejection=False):
    # При обрыве поток мог продолжить работу у провайдера. Не возвращаем весь резерв как будто вызова не было.
    if usage is not None:
        charge = usage[0] * attempt["input_price"] + usage[1] * attempt["output_price"]
        accounting = "provider_usage"
    elif definite_rejection:
        charge = 0
        accounting = "rejected"
    else:
        charge = attempt["cap"]
        accounting = "conservative_reserve"
    async with engine.begin() as conn:
        await conn.execute(
            text("""
            UPDATE attempts SET status=:status,charge=:charge,accounting=:accounting,input_tokens=:input,
                output_tokens=:output,error=:error,finished_at=now() WHERE id=:id AND status='running'
        """),
            {
                "id": attempt["id"],
                "status": status,
                "charge": charge,
                "accounting": accounting,
                "input": usage[0] if usage else None,
                "output": usage[1] if usage else None,
                "error": error,
            },
        )


async def settle(generation, status, result=None, error=None):
    async with engine.begin() as conn:
        # Везде сначала блокируем бюджет, затем запрос: одинаковый порядок предотвращает взаимные блокировки.
        await conn.execute(
            text("SELECT id FROM users WHERE id=:id FOR UPDATE"), {"id": generation["user_id"]}
        )
        row = (
            (
                await conn.execute(
                    text("SELECT * FROM generations WHERE id=:id FOR UPDATE"),
                    {"id": generation["id"]},
                )
            )
            .mappings()
            .one()
        )
        if row["status"] != "running":
            return False
        await conn.execute(
            text(
                "UPDATE attempts SET status='unknown',accounting='conservative_reserve',charge=cap,finished_at=now() WHERE generation_id=:id AND status='running'"
            ),
            {"id": generation["id"]},
        )
        charge = (
            await conn.execute(
                text("SELECT COALESCE(sum(charge),0) FROM attempts WHERE generation_id=:id"),
                {"id": generation["id"]},
            )
        ).scalar_one()
        await conn.execute(
            text("UPDATE users SET reserved=reserved-:reserve,spent=spent+:charge WHERE id=:id"),
            {"id": generation["user_id"], "reserve": row["reserve"], "charge": charge},
        )
        await conn.execute(
            text("""
            UPDATE generations SET status=:status,charge=:charge,result=CAST(:result AS jsonb),error=:error,finished_at=now()
            WHERE id=:id
        """),
            {
                "id": generation["id"],
                "status": status,
                "charge": charge,
                "result": json.dumps(result),
                "error": error,
            },
        )
    return True


async def reap():
    async with engine.connect() as conn:
        rows = (
            (
                await conn.execute(
                    text(
                        "SELECT * FROM generations WHERE status='running' AND lease_until<now() ORDER BY lease_until LIMIT 100"
                    )
                )
            )
            .mappings()
            .all()
        )
    for row in rows:
        await settle(dict(row), "abandoned", error="API stopped before final accounting")
    return len(rows)
