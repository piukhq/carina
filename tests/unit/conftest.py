from typing import Generator

import pytest

from sqlalchemy.engine import Connection
from sqlalchemy.orm import Session

from app.db.base import Base
from app.db.session import sync_engine


@pytest.fixture(scope="module")
def connection() -> Connection:
    return sync_engine.connect()


@pytest.fixture(scope="module")
def unit_db_session(connection: Connection) -> Generator:
    session = Session(bind=connection)

    yield session

    session.rollback()

    # Close the connection that began the nested transaction that wraps everything
    connection.close()


@pytest.fixture(scope="function")
def db_session(unit_db_session: Session, connection: Connection) -> Generator:
    # Outer transaction
    connection.begin_nested()

    yield unit_db_session

    unit_db_session.rollback()


@pytest.fixture(scope="module", autouse=True)
def setup_tables() -> Generator:
    """
    autouse set to True so will be run before each test module, to set up tables
    and tear them down afterwards
    """
    Base.metadata.create_all(bind=sync_engine)

    yield

    # Drop all tables after each test
    Base.metadata.drop_all(bind=sync_engine)
