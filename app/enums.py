from enum import Enum

from fastapi import HTTPException, status


class EventSignals(Enum):
    INBOUND_HTTP_REQ = "inbound-http-request"
    RECORD_HTTP_REQ = "record-http-request"


class HttpErrors(Enum):
    INVALID_TOKEN = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail={
            "display_message": "Supplied token is invalid.",
            "code": "INVALID_TOKEN",
        },
    )
    INVALID_RETAILER = HTTPException(
        detail={
            "display_message": "Requested retailer is invalid.",
            "code": "INVALID_RETAILER",
        },
        status_code=status.HTTP_403_FORBIDDEN,
    )
    UNKNOWN_VOUCHER_TYPE = HTTPException(
        detail={
            "display_message": "Reward Slug does not exist.",
            "code": "UNKNOWN_VOUCHER_TYPE",
        },
        status_code=status.HTTP_404_NOT_FOUND,
    )
    STATUS_UPDATE_FAILED = HTTPException(
        detail={
            "display_message": "Status could not be updated as requested",
            "code": "STATUS_UPDATE_FAILED",
        },
        status_code=status.HTTP_409_CONFLICT,
    )


class RewardTypeStatuses(str, Enum):
    ACTIVE = "active"
    CANCELLED = "cancelled"
    ENDED = "ended"


class RewardUpdateStatuses(Enum):
    ISSUED = "issued"
    CANCELLED = "cancelled"
    REDEEMED = "redeemed"


class RewardFetchType(Enum):
    PRE_LOADED = "pre_loaded"


class FileAgentType(Enum):
    IMPORT = "import"
    UPDATE = "update"
