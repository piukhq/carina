from typing import List, Optional

from retry_task_lib.db.models import RetryTask, TaskType
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.orm import joinedload

from app.db.base_class import async_run_query
from app.enums import HttpErrors
from app.models import Voucher, VoucherConfig


async def get_voucher_config(
    db_session: AsyncSession,
    retailer_slug: str,
    voucher_type_slug: str,
    for_update: bool = False,
) -> VoucherConfig:
    async def _query(by_voucher_type_slug: bool = False) -> List[VoucherConfig]:
        stmt = select(VoucherConfig).where(VoucherConfig.retailer_slug == retailer_slug)
        if by_voucher_type_slug:
            stmt = stmt.where(VoucherConfig.voucher_type_slug == voucher_type_slug)
            if for_update:
                stmt = stmt.with_for_update()
        return await db_session.execute(stmt)

    retailer_voucher_configs = (await async_run_query(_query, db_session)).scalars().all()
    if not retailer_voucher_configs:
        raise HttpErrors.INVALID_RETAILER.value

    voucher_config = (await async_run_query(_query, db_session, by_voucher_type_slug=True)).scalar_one_or_none()
    if voucher_config is None:
        raise HttpErrors.UNKNOWN_VOUCHER_TYPE.value

    return voucher_config


async def get_allocable_voucher(db_session: AsyncSession, voucher_config: VoucherConfig) -> Optional[Voucher]:
    async def _query() -> Optional[Voucher]:
        return (
            (
                await db_session.execute(
                    select(Voucher)
                    .with_for_update()
                    .where(
                        Voucher.voucher_config_id == voucher_config.id,
                        Voucher.allocated == False,  # noqa
                        Voucher.deleted == False,  # noqa
                    )
                    .limit(1)
                )
            )
            .scalars()
            .first()
        )

    return await async_run_query(_query, db_session)


async def create_voucher_issuance_retry_task(
    db_session: AsyncSession,
    voucher: Optional[Voucher],
    issued_date: float,
    expiry_date: float,
    voucher_config: VoucherConfig,
    account_url: str,
) -> RetryTask:
    async def _query() -> RetryTask:

        task_type: TaskType = (
            (
                await db_session.execute(
                    select(TaskType)
                    .options(joinedload(TaskType.task_type_keys))
                    .where(TaskType.name == "voucher_issuance")
                )
            )
            .scalars()
            .first()
        )
        # move get keys to model as property
        retry_task = RetryTask(task_type_id=task_type.task_type_id)
        db_session.add(retry_task)
        await db_session.flush()

        # move task type keys value creation to model with dict required as param
        keys = task_type.key_ids_by_name
        values = [
            (keys["account_url"], account_url),
            (keys["issued_date"], str(issued_date)),
            (keys["expiry_date"], str(expiry_date)),
            (keys["voucher_config_id"], str(voucher_config.id)),
            (keys["voucher_type_slug"], voucher_config.voucher_type_slug),
        ]

        if voucher is not None:
            voucher.allocated = True
            values.extend(
                [
                    (keys["voucher_id"], str(voucher.id)),
                    (keys["voucher_code"], voucher.voucher_code),
                ]
            )

        db_session.add_all(retry_task.get_task_type_key_values(values))
        await db_session.commit()
        return retry_task

    return await async_run_query(_query, db_session)
