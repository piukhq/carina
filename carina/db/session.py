from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool

from carina.core.config import settings

# application name
CONNECT_ARGS = {"application_name": "carina"}
null_pool = {"poolclass": NullPool} if settings.USE_NULL_POOL else {}

# future=True enables sqlalchemy core 2.0
async_engine = create_async_engine(
    settings.SQLALCHEMY_DATABASE_URI_ASYNC, pool_pre_ping=True, future=True, echo=settings.SQL_DEBUG, **null_pool
)
sync_engine = create_engine(
    settings.SQLALCHEMY_DATABASE_URI,
    connect_args=CONNECT_ARGS,
    pool_pre_ping=True,
    echo=settings.SQL_DEBUG,
    future=True,
    **null_pool
)
AsyncSessionMaker = sessionmaker(bind=async_engine, future=True, expire_on_commit=False, class_=AsyncSession)
SyncSessionMaker = sessionmaker(bind=sync_engine, future=True, expire_on_commit=False)
