from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool

from app.core.config import settings

if settings.USE_NULL_POOL or settings.TESTING:
    null_pool = {"poolclass": NullPool}
else:
    null_pool = {}  # pragma: no cover


# future=True enables sqlalchemy core 2.0
async_engine = create_async_engine(
    settings.SQLALCHEMY_DATABASE_URI_ASYNC, pool_pre_ping=True, future=True, echo=settings.SQL_DEBUG, **null_pool
)
sync_engine = create_engine(
    settings.SQLALCHEMY_DATABASE_URI,
    pool_pre_ping=True,
    poolclass=NullPool,
    echo=settings.SQL_DEBUG,
    future=True,
)
AsyncSessionMaker = sessionmaker(bind=async_engine, future=True, expire_on_commit=False, class_=AsyncSession)
SyncSessionMaker = sessionmaker(bind=sync_engine, future=True, expire_on_commit=False)
