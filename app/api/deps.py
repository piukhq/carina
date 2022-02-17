from typing import TYPE_CHECKING, AsyncGenerator

from fastapi import Depends, Header
from sqlalchemy.ext.asyncio import AsyncSession

from app import crud
from app.core.config import settings
from app.db.session import AsyncSessionMaker
from app.enums import HttpErrors

if TYPE_CHECKING:

    from app.models import Retailer


async def get_session() -> AsyncGenerator:
    session = AsyncSessionMaker()
    try:
        yield session
    finally:
        await session.close()


def get_authorization_token(authorization: str = Header(None)) -> str:
    try:
        token_type, token_value = authorization.split(" ")
        if token_type.lower() == "token":
            return token_value
    except (ValueError, AttributeError):
        pass

    raise HttpErrors.INVALID_TOKEN.value


# user as in user of our api, not an account holder.
def user_is_authorised(token: str = Depends(get_authorization_token)) -> None:
    if not token == settings.CARINA_API_AUTH_TOKEN:
        raise HttpErrors.INVALID_TOKEN.value


async def retailer_is_valid(retailer_slug: str, db_session: AsyncSession = Depends(get_session)) -> "Retailer":
    return await crud.get_retailer_by_slug(db_session, retailer_slug)
