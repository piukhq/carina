from datetime import datetime, timedelta
from typing import TYPE_CHECKING

import pytest

from app.models import Voucher, VoucherAllocation

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

# conftest for API tests: tables will be dropped after each test to ensure a clean state


@pytest.fixture(scope="function")
def voucher_allocation(db_session: "Session", voucher: Voucher) -> VoucherAllocation:
    now = datetime.utcnow()
    allocation = VoucherAllocation(
        voucher=voucher,
        voucher_config=voucher.voucher_config,
        account_url="http://test.url/",
        issued_date=now.timestamp(),
        expiry_date=(now + timedelta(days=voucher.voucher_config.validity_days)).timestamp(),  # type: ignore [arg-type]
    )
    db_session.add(allocation)
    db_session.commit()
    return allocation


@pytest.fixture(scope="function")
def expected_payload(voucher_allocation: VoucherAllocation) -> dict:
    return {
        "voucher_code": voucher_allocation.voucher.voucher_code,
        "issued_date": voucher_allocation.issued_date,
        "expiry_date": voucher_allocation.expiry_date,
        "voucher_type_slug": voucher_allocation.voucher_config.voucher_type_slug,
        "voucher_id": str(voucher_allocation.voucher_id),
    }
