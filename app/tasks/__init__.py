import logging

from typing import Any, Dict, Optional, Tuple

import requests

from tenacity import retry
from tenacity.before import before_log
from tenacity.retry import retry_if_exception_type, retry_if_result
from tenacity.stop import stop_after_attempt
from tenacity.wait import wait_fixed

from .prometheus import update_metrics_exception_handler, update_metrics_hook

logger = logging.getLogger("tasks")


@retry(
    stop=stop_after_attempt(2),
    wait=wait_fixed(1),
    reraise=True,
    before=before_log(logger, logging.INFO),
    retry_error_callback=lambda retry_state: retry_state.outcome.result(),
    retry=retry_if_result(lambda resp: 501 <= resp.status_code < 600)
    | retry_if_exception_type(requests.RequestException),
)
def send_request_with_metrics(
    method: str,
    url: str,
    *,
    headers: Optional[Dict[str, Any]] = None,
    json: Optional[Dict[str, Any]] = None,
    timeout: Tuple[float, int],
) -> requests.Response:

    try:
        return requests.request(
            method, url, hooks={"response": update_metrics_hook}, headers=headers, json=json, timeout=timeout
        )
    except requests.HTTPError as ex:
        update_metrics_hook(ex.response)
        raise
    except requests.RequestException as ex:
        update_metrics_exception_handler(ex, method, url)
        raise
