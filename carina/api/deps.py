import contextlib

from collections.abc import AsyncGenerator
from typing import TYPE_CHECKING
from uuid import UUID

from fastapi import Depends, Header
from sqlalchemy.ext.asyncio import AsyncSession

from carina import crud
from carina.core.config import settings
from carina.db.session import AsyncSessionMaker
from carina.enums import HttpErrors

if TYPE_CHECKING:  # pragma: no cover

    from carina.models import Retailer


async def get_session() -> AsyncGenerator:
    session = AsyncSessionMaker()
    try:
        yield session
    finally:
        await session.close()


def get_authorization_token(authorization: str = Header(None)) -> str:
    with contextlib.suppress(ValueError, AttributeError):
        token_type, token_value = authorization.split(" ")
        if token_type.lower() == "token":
            return token_value
    raise HttpErrors.INVALID_TOKEN.value


# user as in user of our api, not an account holder.
def user_is_authorised(token: str = Depends(get_authorization_token)) -> None:
    if token != settings.CARINA_API_AUTH_TOKEN:
        raise HttpErrors.INVALID_TOKEN.value


async def retailer_is_valid(retailer_slug: str, db_session: AsyncSession = Depends(get_session)) -> "Retailer":
    return await crud.get_retailer_by_slug(db_session, retailer_slug)


def get_idempotency_token(idempotency_token: str = Header(None)) -> UUID:
    try:
        return UUID(idempotency_token)
    except (TypeError, ValueError):
        raise HttpErrors.MISSING_OR_INVALID_IDEMPOTENCY_TOKEN_HEADER.value from None
