from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool

from app.core.config import settings

if settings.USE_NULL_POOL or settings.TESTING:
    null_pool = {"poolclass": NullPool}
else:
    null_pool = {}  # pragma: no cover


# future=True enables sqlalchemy core 2.0
engine = create_engine(
    settings.SQLALCHEMY_DATABASE_URI, pool_pre_ping=True, poolclass=NullPool, echo=settings.SQL_DEBUG, future=True
)
SessionMaker = sessionmaker(bind=engine, future=True, expire_on_commit=False)
