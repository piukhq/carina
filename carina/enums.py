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
    UNKNOWN_REWARD_SLUG = HTTPException(
        detail={
            "display_message": "Reward Slug does not exist.",
            "code": "UNKNOWN_REWARD_SLUG",
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
    MISSING_OR_INVALID_IDEMPOTENCY_TOKEN_HEADER = HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail={
            "display_message": "Submitted headers invalid.",
            "code": "HEADER_VALIDATION_ERROR",
            "fields": [
                "idempotency-token",
            ],
        },
    )
    DELETE_FAILED = HTTPException(
        status_code=status.HTTP_409_CONFLICT,
        detail={
            "display_message": "The campaign could not be deleted.",
            "code": "DELETE_FAILED",
        },
    )


class RewardTypeStatuses(str, Enum):
    ACTIVE = "active"
    CANCELLED = "cancelled"
    ENDED = "ended"
    DELETED = "deleted"


class RewardUpdateStatuses(Enum):
    ISSUED = "issued"
    CANCELLED = "cancelled"
    REDEEMED = "redeemed"


class RewardCampaignStatuses(Enum):
    ACTIVE = "active"
    CANCELLED = "cancelled"
    DRAFT = "draft"
    ENDED = "ended"


class RewardFetchType(Enum):
    PRE_LOADED = "pre_loaded"


class FileAgentType(Enum):
    IMPORT = "import"
    UPDATE = "update"
