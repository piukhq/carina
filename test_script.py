from datetime import datetime

import rq

from app.core.config import redis, settings
from app.db.session import SyncSessionMaker
from app.enums import QueuedRetryStatuses, VoucherUpdateStatuses
from app.models import VoucherUpdate
from app.tasks.status_adjustment import status_adjustment

with SyncSessionMaker() as db_session:
    voucher_update = VoucherUpdate(
        voucher_code="TSTCD123456",
        retailer_slug="test-retailer",
        date=datetime.utcnow().date(),
        status=VoucherUpdateStatuses.REDEEMED,
        retry_status=QueuedRetryStatuses.IN_PROGRESS,
    )
    db_session.add(voucher_update)
    db_session.commit()


q = rq.Queue(settings.VOUCHER_STATUS_UPDATE_TASK_QUEUE, connection=redis)
q.enqueue(
    status_adjustment,
    kwargs={"voucher_status_adjustment_id": voucher_update.id},
    failure_ttl=60 * 60 * 24 * 7,  # 1 week
)
