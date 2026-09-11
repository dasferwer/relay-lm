from sqlalchemy import text

from .config import settings
from .db import engine


async def acquire(name):
    async with engine.begin() as conn:
        await conn.execute(
            text("INSERT INTO circuits(provider) VALUES(:name) ON CONFLICT DO NOTHING"),
            {"name": name},
        )
        row = (
            (
                await conn.execute(
                    text(
                        "SELECT *,now() AS current_time FROM circuits WHERE provider=:name FOR UPDATE"
                    ),
                    {"name": name},
                )
            )
            .mappings()
            .one()
        )
        now = row["current_time"]
        if row["open_until"] is None:
            return {"provider": name, "epoch": row["epoch"]}
        if row["open_until"] > now or (row["probe_until"] and row["probe_until"] > now):
            return None
        epoch = row["epoch"] + 1
        await conn.execute(
            text(
                "UPDATE circuits SET epoch=:epoch,probe_until=now()+(:lease * interval '1 second') WHERE provider=:name"
            ),
            {"name": name, "epoch": epoch, "lease": settings.request_lease},
        )
        return {"provider": name, "epoch": epoch}


async def report(permit, success):
    async with engine.begin() as conn:
        row = (
            (
                await conn.execute(
                    text(
                        "SELECT * FROM circuits WHERE provider=:provider AND epoch=:epoch FOR UPDATE"
                    ),
                    permit,
                )
            )
            .mappings()
            .first()
        )
        if row is None:
            return
        if success:
            await conn.execute(
                text(
                    "UPDATE circuits SET failures=0,open_until=NULL,probe_until=NULL WHERE provider=:provider AND epoch=:epoch"
                ),
                permit,
            )
        else:
            failures = row["failures"] + 1
            if failures >= settings.circuit_threshold or row["open_until"] is not None:
                # Новое поколение не даст запоздалому успешному запросу закрыть уже открытый circuit.
                await conn.execute(
                    text(
                        "UPDATE circuits SET failures=:failures,epoch=epoch+1,open_until=now()+(:cooldown * interval '1 second'),probe_until=NULL WHERE provider=:provider AND epoch=:epoch"
                    ),
                    {**permit, "failures": failures, "cooldown": settings.circuit_cooldown},
                )
            else:
                await conn.execute(
                    text(
                        "UPDATE circuits SET failures=:failures WHERE provider=:provider AND epoch=:epoch"
                    ),
                    {**permit, "failures": failures},
                )


async def release(permit):
    # Отмена клиентом ничего не говорит о доступности провайдера. Освобождаем только пробный запрос.
    async with engine.begin() as conn:
        await conn.execute(
            text("UPDATE circuits SET probe_until=NULL WHERE provider=:provider AND epoch=:epoch"),
            permit,
        )
