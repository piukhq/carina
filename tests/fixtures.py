from collections import namedtuple
from enum import Enum

from fastapi import status

HttpError = namedtuple("HttpError", ["detail", "status_code"])


class HttpErrors(Enum):
    INVALID_TOKEN = HttpError(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail={
            "display_message": "Supplied token is invalid.",
            "code": "INVALID_TOKEN",
        },
    )
    INVALID_RETAILER = HttpError(
        detail={
            "display_message": "Requested retailer is invalid.",
            "code": "INVALID_RETAILER",
        },
        status_code=status.HTTP_403_FORBIDDEN,
    )
    UNKNOWN_REWARD_TYPE = HttpError(
        detail={
            "display_message": "Reward Slug does not exist.",
            "code": "UNKNOWN_REWARD_SLUG",
        },
        status_code=status.HTTP_404_NOT_FOUND,
    )
    STATUS_UPDATE_FAILED = HttpError(
        detail={
            "display_message": "Status could not be updated as requested",
            "code": "STATUS_UPDATE_FAILED",
        },
        status_code=status.HTTP_409_CONFLICT,
    )
    MISSING_OR_INVALID_IDEMPOTENCY_TOKEN_HEADER = HttpError(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail={
            "display_message": "Submitted headers invalid.",
            "code": "HEADER_VALIDATION_ERROR",
            "fields": [
                "idempotency-token",
            ],
        },
    )
    DELETE_FAILED = HttpError(
        status_code=status.HTTP_409_CONFLICT,
        detail={
            "display_message": "The campaign could not be deleted.",
            "code": "DELETE_FAILED",
        },
    )
