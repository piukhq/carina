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
            "error": "INVALID_TOKEN",
        },
    )
    INVALID_RETAILER = HTTPException(
        detail={
            "display_message": "Requested retailer is invalid.",
            "error": "INVALID_RETAILER",
        },
        status_code=status.HTTP_403_FORBIDDEN,
    )
    UNKNOWN_VOUCHER_TYPE = HTTPException(
        detail={
            "display_message": "Voucher Type Slug does not exist.",
            "error": "UNKNOWN_VOUCHER_TYPE",
        },
        status_code=status.HTTP_404_NOT_FOUND,
    )
    STATUS_UPDATE_FAILED = HTTPException(
        detail={
            "display_message": "Status could not be updated as requested",
            "error": "STATUS_UPDATE_FAILED",
        },
        status_code=status.HTTP_409_CONFLICT,
    )


class QueuedRetryStatuses(Enum):
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    FAILED = "failed"
    SUCCESS = "success"
    WAITING = "waiting"


class VoucherTypeStatuses(str, Enum):
    ACTIVE = "active"
    CANCELLED = "cancelled"
    ENDED = "ended"


class VoucherUpdateStatuses(Enum):
    ISSUED = "issued"
    CANCELLED = "cancelled"
    REDEEMED = "redeemed"


class VoucherFetchType(Enum):
    PRE_LOADED = "pre_loaded"
