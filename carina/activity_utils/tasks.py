from cosmos_message_lib import get_connection_and_exchange, verify_payload_and_send_activity

from carina.core.config import settings

connection, exchange = get_connection_and_exchange(
    rabbitmq_dsn=settings.RABBITMQ_DSN,
    message_exchange_name=settings.MESSAGE_EXCHANGE_NAME,
)


def sync_send_activity(payload: dict, *, routing_key: str) -> None:
    verify_payload_and_send_activity(connection, exchange, payload, routing_key)
