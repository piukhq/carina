from collections import namedtuple
from enum import Enum

from fastapi import status

HttpError = namedtuple("HttpError", ["detail", "status_code"])


class HttpErrors(Enum):
    INVALID_TOKEN = HttpError(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail={
            "display_message": "Supplied token is invalid.",
            "error": "INVALID_TOKEN",
        },
    )
    INVALID_RETAILER = HttpError(
        detail={
            "display_message": "Requested retailer is invalid.",
            "error": "INVALID_RETAILER",
        },
        status_code=status.HTTP_403_FORBIDDEN,
    )
    UNKNOWN_VOUCHER_TYPE = HttpError(
        detail={
            "display_message": "Voucher Type Slug does not exist.",
            "error": "UNKNOWN_VOUCHER_TYPE",
        },
        status_code=status.HTTP_404_NOT_FOUND,
    )
    STATUS_UPDATE_FAILED = HttpError(
        detail={
            "display_message": "Status could not be updated as requested",
            "error": "STATUS_UPDATE_FAILED",
        },
        status_code=status.HTTP_409_CONFLICT,
    )
