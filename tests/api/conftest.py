from collections import namedtuple
from typing import TYPE_CHECKING, Generator

import pytest

from app.models import Voucher, VoucherConfig

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

SetupType = namedtuple("SetupType", ["db_session", "voucher_config", "voucher"])


@pytest.fixture(scope="function")
def setup(db_session: "Session", voucher_config: VoucherConfig, voucher: Voucher) -> Generator[SetupType, None, None]:
    yield SetupType(db_session, voucher_config, voucher)
