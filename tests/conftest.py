import uuid

from typing import TYPE_CHECKING, Callable, Generator

import pytest

from retry_task_lib.db.models import TaskType, TaskTypeKey
from sqlalchemy_utils import create_database, database_exists, drop_database
from testfixtures import LogCapture

from app.db.base import Base
from app.db.session import SyncSessionMaker, sync_engine
from app.enums import VoucherTypeStatuses
from app.models import Voucher, VoucherConfig

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


# Top-level conftest for tests, doing things like setting up DB


@pytest.fixture(scope="session", autouse=True)
def setup_db() -> Generator:
    if sync_engine.url.database != "carina_test":
        raise ValueError(f"Unsafe attempt to recreate database: {sync_engine.url.database}")

    if database_exists(sync_engine.url):
        drop_database(sync_engine.url)
    create_database(sync_engine.url)

    yield

    # At end of all tests, drop the test db
    drop_database(sync_engine.url)


@pytest.fixture(scope="session")
def main_db_session() -> Generator["Session", None, None]:
    with SyncSessionMaker() as db_session:
        yield db_session


@pytest.fixture(scope="function")
def db_session(main_db_session: "Session") -> Generator["Session", None, None]:
    yield main_db_session
    main_db_session.rollback()
    main_db_session.expunge_all()


@pytest.fixture(scope="function", autouse=True)
def setup_tables() -> Generator:
    """
    autouse set to True so will be run before each test function, to set up tables
    and tear them down after each test runs
    """
    Base.metadata.create_all(bind=sync_engine)

    yield

    # Drop all tables after each test
    Base.metadata.drop_all(bind=sync_engine)


@pytest.fixture(scope="function")
def voucher_config(db_session: "Session") -> VoucherConfig:
    config = VoucherConfig(
        voucher_type_slug="test-voucher",
        validity_days=15,
        retailer_slug="test-retailer",
        status=VoucherTypeStatuses.ACTIVE,
    )
    db_session.add(config)
    db_session.commit()
    return config


@pytest.fixture(scope="function")
def voucher(db_session: "Session", voucher_config: VoucherConfig) -> Voucher:
    vc = Voucher(
        voucher_code="TSTCD1234",
        retailer_slug=voucher_config.retailer_slug,
        voucher_config=voucher_config,
    )
    db_session.add(vc)
    db_session.commit()
    return vc


@pytest.fixture()
def create_vouchers(db_session: "Session", voucher_config: VoucherConfig) -> Callable:
    def fn(override_datas: list[dict]) -> dict[str, Voucher]:
        voucher_data = {
            "voucher_code": str(uuid.uuid4()),
            "deleted": False,
            "allocated": False,
            "voucher_config_id": voucher_config.id,
            "retailer_slug": voucher_config.retailer_slug,
        }
        vouchers = [Voucher(**voucher_data | override_data) for override_data in override_datas]
        db_session.add_all(vouchers)
        db_session.commit()
        return {voucher.voucher_code: voucher for voucher in vouchers}

    return fn


@pytest.fixture(scope="function")
def capture() -> Generator:
    with LogCapture() as capture:
        yield capture


@pytest.fixture(scope="function")
def voucher_issuance_task_type(db_session: "Session") -> TaskType:
    task = TaskType(name="voucher_issuance")
    db_session.add(task)
    db_session.flush()

    db_session.add_all(
        [
            TaskTypeKey(task_type_id=task.task_type_id, name=key_name, type=key_type)
            for key_name, key_type in (
                ("account_url", "STRING"),
                ("issued_date", "FLOAT"),
                ("expiry_date", "FLOAT"),
                ("voucher_config_id", "INTEGER"),
                ("voucher_type_slug", "STRING"),
                ("voucher_id", "STRING"),
                ("voucher_code", "STRING"),
            )
        ]
    )

    db_session.commit()
    return task


@pytest.fixture(scope="function")
def voucher_status_adjustment_task_type(db_session: "Session") -> TaskType:
    task = TaskType(name="voucher_status_adjustment")
    db_session.add(task)
    db_session.flush()

    db_session.add_all(
        [
            TaskTypeKey(task_type_id=task.task_type_id, name=key_name, type=key_type)
            for key_name, key_type in (
                ("voucher_id", "STRING"),
                ("retailer_slug", "STRING"),
                ("date", "FLOAT"),
                ("status", "STRING"),
            )
        ]
    )

    db_session.commit()
    return task
