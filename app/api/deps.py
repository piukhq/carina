from typing import AsyncGenerator

from app.db.session import AsyncSessionMaker


async def get_session() -> AsyncGenerator:
    session = AsyncSessionMaker()
    try:
        yield session
    finally:
        await session.close()
