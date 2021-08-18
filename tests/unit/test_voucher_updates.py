import datetime

from functools import partial
from typing import TYPE_CHECKING

from pytest_mock import MockerFixture

from app.core.config import settings
from app.enums import VoucherUpdateStatuses
from app.imports.agents.file_agent import VoucherUpdatesAgent
from app.models import VoucherConfig, VoucherUpdate

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


def test_process_csv(db_session: "Session", voucher_config: VoucherConfig, mocker: MockerFixture) -> None:
    # GIVEN
    mocker.patch("app.imports.agents.file_agent.BlobServiceClient")
    voucher_agent = VoucherUpdatesAgent()
    blob_name = "/test-retailer/voucher-updates/test.csv"
    byte_content = b"A2eYRbcvF9yYpW2,2021-07-30,redeemed\n08oVk3czC9QYLBJ,2021-08-31,cancelled\n"

    # WHEN
    voucher_agent.process_csv(
        db_session=db_session,
        blob_name=blob_name,
        byte_content=byte_content,
        voucher_config_id=voucher_config.id,
    )
    voucher_update_rows = db_session.query(VoucherUpdate).filter_by(voucher_config_id=voucher_config.id).all()

    # THEN
    assert len(voucher_update_rows) == 2
    assert voucher_update_rows[0].voucher_code == "A2eYRbcvF9yYpW2"
    assert voucher_update_rows[0].status == VoucherUpdateStatuses.REDEEMED
    assert isinstance(voucher_update_rows[0].date, datetime.date)
    assert str(voucher_update_rows[0].date) == "2021-07-30"
    assert voucher_update_rows[1].voucher_code == "08oVk3czC9QYLBJ"
    assert voucher_update_rows[1].status == VoucherUpdateStatuses.CANCELLED
    assert isinstance(voucher_update_rows[1].date, datetime.date)
    assert str(voucher_update_rows[1].date) == "2021-08-31"


def test_archive(mocker: MockerFixture) -> None:
    # GIVEN
    blob_service_client = mocker.patch("app.imports.agents.file_agent.BlobServiceClient", autospec=True)
    blob_service_client.delete_blob = mocker.MagicMock
    voucher_agent = VoucherUpdatesAgent()
    blob_name = "/test-retailer/voucher-updates/test.csv"
    byte_content = b"A2eYRbcvF9yYpW2,2021-07-30,redeemed\n08oVk3czC9QYLBJ,2021-08-31,cancelled\n"

    # WHEN
    voucher_agent.archive(
        blob_name,
        byte_content,
        delete_callback=partial(blob_service_client.delete_blob, lease=mocker.MagicMock),
        blob_service_client=blob_service_client,
        logger=mocker.MagicMock,
    )
    blob = f"{datetime.datetime.now().strftime('%Y/%m/%d')}/{blob_name}"

    # THEN
    blob_service_client.get_blob_client.assert_called_once_with(settings.BLOB_ARCHIVE_CONTAINER, blob)
    blob_service_client.get_blob_client.return_value.upload_blob.assert_called_once_with(byte_content)
