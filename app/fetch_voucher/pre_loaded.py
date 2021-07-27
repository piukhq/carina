from datetime import datetime, timedelta
from typing import Optional, Tuple

from sqlalchemy.ext.asyncio import AsyncSession

from app.crud import get_allocable_voucher
from app.models import Voucher, VoucherConfig


async def get_voucher(
    db_session: AsyncSession, voucher_config: VoucherConfig
) -> Tuple[Optional[Voucher], float, float]:
    now = datetime.utcnow()
    issued = now.timestamp()
    expiry = (now + timedelta(days=voucher_config.validity_days)).timestamp()  # type: ignore [arg-type]
    voucher = await get_allocable_voucher(db_session, voucher_config)

    return voucher, issued, expiry
