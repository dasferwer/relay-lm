import asyncio
from uuid import UUID

from sqlalchemy import text

from relaylm.auth import hasher
from relaylm.db import engine


async def seed():
    async with engine.begin() as conn:
        await conn.execute(
            text(
                "INSERT INTO users(id,email,password_hash,role) VALUES(:id,'demo@example.com',:hash,'admin') ON CONFLICT DO NOTHING"
            ),
            {
                "id": UUID("18000000-0000-0000-0000-000000000001"),
                "hash": hasher.hash("RelayLMDemo123!"),
            },
        )


if __name__ == "__main__":
    asyncio.run(seed())
