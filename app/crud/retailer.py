from typing import TYPE_CHECKING, Optional

from sqlalchemy.future import select

from app.db.base_class import async_run_query
from app.enums import HttpErrors
from app.models import Retailer

if TYPE_CHECKING:  # pragma: no cover

    from sqlalchemy.ext.asyncio import AsyncSession


async def get_retailer_by_slug(db_session: "AsyncSession", retailer_slug: str) -> Retailer:
    async def _query() -> Optional[Retailer]:
        return (await db_session.execute(select(Retailer).where(Retailer.slug == retailer_slug))).scalar_one_or_none()

    retailer = await async_run_query(_query, db_session, rollback_on_exc=False)
    if not retailer:
        raise HttpErrors.INVALID_RETAILER.value

    return retailer
