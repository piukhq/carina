from typing import Optional, Tuple

from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Voucher, VoucherConfig

from . import pre_loaded


async def get_allocable_voucher(
    db_session: AsyncSession, voucher_config: VoucherConfig
) -> Tuple[Optional[Voucher], float, float]:

    # placeholder for fetching based on voucher config type using "agents" type of logic
    # for now defaulting to "pre_loaded" agent

    return await pre_loaded.get_voucher(db_session, voucher_config)
