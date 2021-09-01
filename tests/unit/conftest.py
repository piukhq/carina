from collections import namedtuple
from typing import Generator

import pytest

from sqlalchemy.engine import Connection
from sqlalchemy.orm import Session

from app.db.base import Base
from app.db.session import sync_engine
from app.models import Voucher, VoucherConfig

SetupType = namedtuple("SetupType", ["db_session", "voucher_config", "voucher"])


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


@pytest.fixture(scope="function")
def setup(db_session: "Session", voucher_config: VoucherConfig, voucher: Voucher) -> Generator[SetupType, None, None]:
    yield SetupType(db_session, voucher_config, voucher)
