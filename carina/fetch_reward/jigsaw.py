from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, Callable, cast
from uuid import uuid4

import requests

from cryptography.fernet import Fernet
from fastapi import status

from carina.core.config import redis_raw, settings
from carina.db.base_class import sync_run_query
from carina.fetch_reward.base import AgentError, BaseAgent, RewardData
from carina.models import Reward

if TYPE_CHECKING:  # pragma: no cover
    from inspect import Traceback

    from retry_tasks_lib.db.models import RetryTask
    from sqlalchemy.orm import Session
    from typing_extensions import TypedDict

    from carina.models import RewardConfig

    SpecialActionsMap = TypedDict(
        "SpecialActionsMap",
        {
            "message_ids": list[str],
            "action": Callable,
            "max_retries": int,
            "retried": int,
        },
    )


class Jigsaw(BaseAgent):
    """
    Handles fetching a Reward from Jigsaw.

    Sample agent_config:
    ```yaml
    base_url: "https://dev.jigsaw360.com"
    brand_id: 30
    ```

    Sample getToken success response payload:
    ```json
    {
        "status": 2000,
        "status_description": "OK",
        "messages": [],
        "PartnerRef": "",
        "data": {
            "__type": "Response.getToken:#Jigsaw.API.Service",
            "Token": "sample-auth-token",
            "Expires": "2022-01-19 09:38:00",
            "TestMode": true
        }
    }
    ```

    Sample register success response payload:
    ```json
    {
        "status": 2000,
        "status_description": "OK",
        "messages": [],
        "PartnerRef": "",
        "data": {
            "__type": "Response_Data.cardData:#Order_V4",
            "customer_card_ref": "UUID generated by us",
            "reference": "339069",
            "number": "Reward.code Jigsaw send us back",
            "pin": "",
            "transaction_value": 10,
            "expiry_date": "2024-01-19T23:59:59+00:00",
            "balance": 10,
            "voucher_url": "https://egift.jigsawgiftcardteam.com/evoucher/download?...",
            "card_status": 1
        }
    }
    ```

    Sample reversal success response payload:
    ```json
    {
        "status": 2000,
        "status_description": "OK",
        "messages": [],
        "PartnerRef": "",
        "data": null
    }
    ```

    """

    CARD_REF_KEY = "customer_card_ref"
    REVERSAL_CARD_REF_KEY = "reversal_customer_card_ref"
    REVERSAL_FLAG_KEY = "might_need_reversal"
    REDIS_TOKEN_KEY = f"{settings.REDIS_KEY_PREFIX}:agent:jigsaw:auth_token"
    STATUS_CODE_MAP = {
        "5000": status.HTTP_500_INTERNAL_SERVER_ERROR,
        "5003": status.HTTP_503_SERVICE_UNAVAILABLE,
        "4003": status.HTTP_403_FORBIDDEN,
        "4001": status.HTTP_401_UNAUTHORIZED,
    }

    def __init__(
        self, db_session: "Session", reward_config: "RewardConfig", config: dict, *, retry_task: "RetryTask"
    ) -> None:
        if retry_task is None:
            raise AgentError("Jigsaw: RetryTask object not provided.")

        super().__init__(db_session=db_session, reward_config=reward_config, config=config, retry_task=retry_task)
        self.base_url: str = self.config["base_url"]
        self.customer_card_ref: str | None = None
        self.reward_config_required_values = reward_config.load_required_fields_values()
        self.fernet = Fernet(settings.JIGSAW_AGENT_ENCRYPTION_KEY.encode())
        self.special_actions_map: dict[str, "SpecialActionsMap"] = {
            # BPL-439: If Jigsaw returns a 4000 status with a message with isError set as True and the id equal to
            # 40028 the customer_card_ref we provided is not unique and we need to generate a new one and try again.
            # BPL-668: If this happens after we got another error from the register endpoint we need to send a
            # reversal request before trying again with the new card ref
            "4000": {
                "message_ids": ["40028"],
                "action": self.try_again_with_new_card_ref,
                "max_retries": 3,
                "retried": 0,
            },
            # BPL-437: If jigsaw returns a 4001 status with a message with isError set as True and the id equal to
            # one of the below ids, we need to fetch a new authorisation token and try again.
            "4001": {
                "message_ids": ["10003", "10006", "10007"],
                "action": self.wipe_cached_token_and_try_again,
                "max_retries": 3,
                "retried": 0,
            },
        }

    @staticmethod
    def _collect_response_data(resp: requests.Response) -> tuple[dict, str, str, str, str]:
        """tries to collect json payload, jigsaw status, status description, and error message if present."""
        try:
            response_payload = resp.json()
            jigsaw_status = str(response_payload["status"])
            description = response_payload["status_description"]
            error_msg = next((msg for msg in response_payload.get("messages", []) if msg["isError"]), None)

            if error_msg is not None:
                msg_id = str(error_msg["id"])
                msg_info = error_msg.get("Info")
            else:
                msg_id = "N/A"
                msg_info = ""

        except (requests.exceptions.JSONDecodeError, KeyError) as ex:
            raise requests.HTTPError(f"Jigsaw: unexpected response format. info: {ex}", response=resp)

        return response_payload, jigsaw_status, description, msg_id, msg_info

    def wipe_cached_token_and_try_again(self, try_again_call: Callable[..., requests.Response]) -> dict:
        redis_raw.delete(self.REDIS_TOKEN_KEY)
        new_resp = try_again_call()
        return self._get_response_body_or_raise_for_status(new_resp, try_again_call)

    def _get_reversal_customer_card_ref(self) -> str:
        try:
            return cast(str, self.agent_state_params[self.REVERSAL_CARD_REF_KEY])
        except KeyError as ex:
            raise AgentError("Jigsaw: Trying to execute a reversal without a reversal_customer_card_ref.") from ex

    def try_again_with_new_card_ref(self, try_again_call: Callable[..., requests.Response]) -> dict:
        execute_reversal = self.agent_state_params.get(self.REVERSAL_FLAG_KEY, False)
        msg = f"Jigsaw: non unique customer card ref: {self.customer_card_ref}, "
        agent_params_updates: dict = {self.CARD_REF_KEY: str(uuid4())}

        if execute_reversal:
            agent_params_updates[self.REVERSAL_CARD_REF_KEY] = self.customer_card_ref
            msg += "sending reversal request and "

        self.customer_card_ref = agent_params_updates[self.CARD_REF_KEY]
        self.set_agent_state_params(self.agent_state_params | agent_params_updates)
        msg += f"trying again with new customer card ref: {self.customer_card_ref}."
        self.logger.error(msg)

        if execute_reversal:
            resp = self._send_reversal_request()
            self._get_response_body_or_raise_for_status(resp, self._send_reversal_request)
            self.set_agent_state_params(self.agent_state_params | {self.REVERSAL_FLAG_KEY: False})

        new_resp = try_again_call()
        return self._get_response_body_or_raise_for_status(new_resp, try_again_call)

    def _flag_for_reversal_if_needed(self, resp: requests.Response, unknown_status: bool = False) -> None:
        # we will need to try a reversal later when retrying the register request only if we got a 3XX or 5XX
        # from the register endpoint or an unknown jigsaw status.
        is_3xx_or_5xx = 300 <= resp.status_code < 400 or 500 <= resp.status_code < 600

        if "register" in resp.request.path_url and (is_3xx_or_5xx or unknown_status):
            self.set_agent_state_params(self.agent_state_params | {self.REVERSAL_FLAG_KEY: True})

    def _requires_special_action(self, try_again_call: Callable | None, jigsaw_status: str, msg_id: str) -> bool:
        return (
            try_again_call is not None
            and jigsaw_status in self.special_actions_map
            and msg_id in self.special_actions_map[jigsaw_status]["message_ids"]
            and self.special_actions_map[jigsaw_status]["retried"]
            < self.special_actions_map[jigsaw_status]["max_retries"]
        )

    def _format_error_message_details(self, msg_id: str, msg_info: str, path_url: str) -> str:
        error_msg_details = f"endpoint: {path_url}, message: {msg_id} {msg_info}"
        if self.customer_card_ref is not None and "getToken" not in path_url:
            error_msg_details += f", customer card ref: {self.customer_card_ref}"

        return error_msg_details

    def _get_response_body_or_raise_for_status(
        self,
        resp: requests.Response,
        try_again_call: Callable[..., requests.Response] = None,
        reversal_allowed: bool = True,
    ) -> dict:
        """Validates a http response based on Jigsaw specific status codes and errors ids."""

        response_payload, jigsaw_status, description, msg_id, msg_info = self._collect_response_data(resp)
        execute_special_action = self._requires_special_action(try_again_call, jigsaw_status, msg_id)

        if not execute_special_action and jigsaw_status in self.STATUS_CODE_MAP:

            if resp.status_code == 200:
                resp.status_code = self.STATUS_CODE_MAP[jigsaw_status]

            self._flag_for_reversal_if_needed(resp)
            raise requests.HTTPError(
                f"Received a {jigsaw_status} {description} response. "
                + self._format_error_message_details(msg_id, msg_info, resp.request.path_url),
                response=resp,
            )

        self.retry_task.update_task(
            db_session=self.db_session,
            response_audit={
                "timestamp": datetime.now(tz=timezone.utc).isoformat(),
                "request": {"method": resp.request.method, "url": resp.request.url},
                "response": {
                    "status": resp.status_code,
                    "jigsaw_status": f"{jigsaw_status} {description}",
                    "message": f"{msg_id} {msg_info}",
                },
            },
        )

        # we want to capture the failed response's audit before trying again.
        if execute_special_action:
            self.special_actions_map[jigsaw_status]["retried"] += 1
            return self.special_actions_map[jigsaw_status]["action"](try_again_call)

        if jigsaw_status != "2000":
            if reversal_allowed:
                self._flag_for_reversal_if_needed(resp, unknown_status=True)

            raise AgentError(
                f"Jigsaw: unknown error returned. status: {jigsaw_status} {description}, "
                + self._format_error_message_details(msg_id, msg_info, resp.request.path_url)
            )

        return response_payload

    def _get_tz_aware_datetime_from_isoformat(self, date_time_str: str) -> datetime:
        """Returns a UTC timezone aware datetime from an isoformat string, assumes UTC if timezone is not specified"""

        dt = datetime.fromisoformat(date_time_str)
        if dt.tzinfo is None:
            self.logger.info("Jigsaw: Received naive datetime, assuming UTC timezone.")
            dt = dt.replace(tzinfo=timezone.utc)

        return dt.astimezone(tz=timezone.utc)

    def _get_and_decrypt_token(self) -> str | None:
        """tries to fetch and decrypt token from redis, returns the token as a string on success and None on failure."""

        raw_token = redis_raw.get(self.REDIS_TOKEN_KEY)
        if raw_token is None:
            return None

        try:
            return self.fernet.decrypt(raw_token).decode()
        except Exception as ex:  # pylint: disable=broad-except
            self.logger.exception(
                f"Jigsaw: Unexpected value retrieved from redis for {self.REDIS_TOKEN_KEY}.", exc_info=ex
            )
            return None

    def _encrypt_and_set_token(self, token: str, expires_in: timedelta) -> None:
        """tries to encrypt the provided token and store it in redis."""

        try:
            redis_raw.set(
                self.REDIS_TOKEN_KEY,
                self.fernet.encrypt(token.encode()),
                expires_in,
            )
        except Exception as ex:  # pylint: disable=broad-except
            self.logger.exception("Jigsaw: Unexpected error while encrypting and saving token to redis.", exc_info=ex)

    def _get_auth_token(self) -> str:
        """
        Fetches a Jigsaw's authorisation token from redis.
        If it cannot find it cached, requests a new one to Jigsaw, caches it, and returns it.
        """

        token = self._get_and_decrypt_token()
        if token is not None:
            return token

        resp = self.send_request(
            "POST",
            url_template="{base_url}/order/V4/getToken",
            url_kwargs={"base_url": self.base_url},
            exclude_from_label_url=[],
            json={
                "Username": settings.JIGSAW_AGENT_USERNAME,
                "Password": settings.JIGSAW_AGENT_PASSWORD,
            },
        )

        response_payload = self._get_response_body_or_raise_for_status(resp)
        expires_in = self._get_tz_aware_datetime_from_isoformat(response_payload["data"]["Expires"]) - datetime.now(
            tz=timezone.utc
        )
        if expires_in.total_seconds() <= 0:
            raise AgentError("Jigsaw: Jigsaw returned an already expired token.")

        token = response_payload["data"]["Token"]
        self._encrypt_and_set_token(token, expires_in)
        return token

    def _generate_customer_card_ref(self) -> datetime:
        """
        Generates a new customer_card_ref uuid and a datetime now utc.
        If a customer_card_ref is stored as task param, returns that instead of creating a new uuid.
        """

        customer_card_ref = self.agent_state_params.get(self.CARD_REF_KEY, None)
        self.customer_card_ref = str(uuid4()) if customer_card_ref is None else customer_card_ref
        return datetime.now(tz=timezone.utc)

    def _save_reward(self, customer_card_ref: str, reward_code: str) -> Reward:
        """Stores the Reward data returned by Jigsaw in the DB"""

        def _query() -> Reward:
            reward = Reward(
                id=customer_card_ref,
                code=reward_code,
                allocated=False,
                deleted=False,
                reward_config_id=self.reward_config.id,
                retailer_id=self.reward_config.retailer_id,
            )
            self.db_session.add(reward)
            self.db_session.commit()
            return reward

        return sync_run_query(_query, self.db_session)

    def _register_reward(self) -> requests.Response:
        """
        Registers our customer_card_ref to Jigsaw and returns a new Reward code.
        """
        try:
            return self.send_request(
                "POST",
                url_template="{base_url}/order/V4/register",
                url_kwargs={"base_url": self.base_url},
                exclude_from_label_url=[],
                json={
                    "customer_card_ref": self.customer_card_ref,
                    "brand_id": self.config["brand_id"],
                    "transaction_value": self.reward_config_required_values["transaction_value"],
                },
                headers={"Token": self._get_auth_token()},
            )
        except requests.ConnectionError:
            self.set_agent_state_params(self.agent_state_params | {self.REVERSAL_FLAG_KEY: True})
            raise

    def _send_reversal_request(self) -> requests.Response:
        return self.send_request(
            "POST",
            url_template="{base_url}/order/V4/reversal",
            url_kwargs={"base_url": self.base_url},
            exclude_from_label_url=[],
            json={
                "original_customer_card_ref": self._get_reversal_customer_card_ref(),
            },
            headers={"Token": self._get_auth_token()},
        )

    def fetch_reward(self) -> RewardData:
        """
        Fetch jigsaw reward

        issued_date is set at the time of generating a new customer card ref
        expiry date is provided by jigsaw in a successful request to register reward

        returns (Reward data, issued_date, expirty_date, validity_days = None)
        """
        issued = self._generate_customer_card_ref()
        if not self.customer_card_ref:
            raise AgentError("Jigsaw: failed to create or fetch customer_card_ref")

        resp = self._register_reward()
        response_payload = self._get_response_body_or_raise_for_status(resp, try_again_call=self._register_reward)

        if response_payload["data"]["balance"] != self.reward_config_required_values["transaction_value"]:
            raise AgentError("Jigsaw: fetched reward balance and transaction value do not match.")

        expiry = self._get_tz_aware_datetime_from_isoformat(response_payload["data"]["expiry_date"])
        self.set_agent_state_params(
            self.agent_state_params | {self.ASSOCIATED_URL_KEY: response_payload["data"]["voucher_url"]}
        )
        reward = self._save_reward(self.customer_card_ref, response_payload["data"]["number"])
        return RewardData(
            reward=reward, issued_date=issued.timestamp(), expiry_date=expiry.timestamp(), validity_days=None
        )

    def fetch_balance(self) -> Any:  # pragma: no cover
        raise NotImplementedError

    def cleanup_reward(self) -> None:
        reward_uuid: str | None = self.retry_task.get_params().get("reward_uuid", None)
        if reward_uuid:
            self.set_agent_state_params(self.agent_state_params | {self.REVERSAL_CARD_REF_KEY: reward_uuid})
            self.update_reward_and_remove_references_from_task(reward_uuid, {"deleted": True})

        if self.agent_state_params.get(self.REVERSAL_CARD_REF_KEY):
            resp = self._send_reversal_request()
            self._get_response_body_or_raise_for_status(resp)

    def __exit__(self, exc_type: type, exc_value: Exception, exc_traceback: "Traceback") -> None:

        if exc_value is not None:
            self.logger.exception(
                "Exception occurred while fetching a new Jigsaw reward or cleaning up an existing task, "
                "exiting agent gracefully.",
                exc_info=exc_value,
            )

            if self.customer_card_ref is not None and self.CARD_REF_KEY not in self.agent_state_params:
                self.set_agent_state_params(self.agent_state_params | {self.CARD_REF_KEY: self.customer_card_ref})
