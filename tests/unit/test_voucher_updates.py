from typing import TYPE_CHECKING

from pytest_mock import MockerFixture

from app.enums import VoucherUpdateStatuses
from app.imports.agents.file_agent import VoucherUpdatesAgent
from app.models import Voucher, VoucherConfig, VoucherUpdate

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


def test_process_csv(
    db_session: "Session", voucher_config: VoucherConfig, voucher: Voucher, mocker: MockerFixture
) -> None:
    # GIVEN
    mocker.patch("app.imports.agents.file_agent.BlobServiceClient")
    voucher_agent = VoucherUpdatesAgent()
    blob_name = "/test-retailer/voucher-updates/test.csv"
    byte_content = b"A2eYRbcvF9yYpW2,2021-08-31,redeemed\n08oVk3czC9QYLBJ,2021-08-31,cancelled\n"

    # spy = mocker.spy(deps, "get_authorization_token")

    # WHEN
    voucher_agent.process_csv(
        db_session=db_session,
        blob_name=blob_name,
        byte_content=byte_content,
        voucher_config_id=voucher_config.id,
    )

    voucher_update_rows = db_session.query(VoucherUpdate).filter_by(voucher_config_id=voucher_config.id).all()

    assert len(voucher_update_rows) == 2
    assert voucher_update_rows[0].voucher_code == "A2eYRbcvF9yYpW2"
    assert voucher_update_rows[0].status == VoucherUpdateStatuses.REDEEMED
    assert voucher_update_rows[1].voucher_code == "08oVk3czC9QYLBJ"
    assert voucher_update_rows[1].status == VoucherUpdateStatuses.CANCELLED
