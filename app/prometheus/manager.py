import typing as t

from blinker import signal
from prometheus_client import Counter, Histogram

from app.core.utils import Singleton
from app.enums import EventSignals


class PrometheusManager(metaclass=Singleton):
    def __init__(self) -> None:
        self.metric_types = self._get_metric_types()
        signal(EventSignals.INBOUND_HTTP_REQ).connect(self.inbound_http_request)
        signal(EventSignals.RECORD_HTTP_REQ).connect(self.record_http_request)

    def inbound_http_request(
        self, sender: t.Union[object, str], endpoint: str, response_code: int, method: str
    ) -> None:
        """
        :param sender: Could be a class instance, or a string description of who the sender is
        :param endpoint: inbound URL stripped of mutable values
        :param response_code: HTTP status code e.g. 200
        :param method: HTTP method e.g. "GET"
        """
        counter = self.metric_types["counters"]["inbound_http_request"]
        labels = {"endpoint": endpoint, "response_code": response_code, "method": method}
        self._increment_counter(counter=counter, increment_by=1, labels=labels)

    def record_http_request(
        self,
        sender: t.Union[object, str],
        endpoint: str,
        response_code: int,
        method: str,
        latency: t.Union[int, float],
    ) -> None:
        """
        :param sender: Could be a class instance, or a string description of who the sender is
        :param endpoint: inbound URL stripped of mutable values
        :param retailer: retailer slug
        :param response_code: HTTP status code e.g. 200
        :param method: HTTP method e.g. "POST"
        :param latency: HTTP request time in seconds
        """

        histogram = self.metric_types["histograms"]["request_latency"]
        histogram.labels(endpoint=endpoint, response_code=response_code, method=method).observe(latency)

    @staticmethod
    def _increment_counter(counter: Counter, increment_by: t.Union[int, float], labels: t.Dict) -> None:
        counter.labels(**labels).inc(increment_by)

    @staticmethod
    def _get_metric_types() -> t.Dict:
        """
        Define metric types here (see https://prometheus.io/docs/concepts/metric_types/),
        with the name, description and a list of the labels they expect.
        """

        metric_types = {
            "counters": {
                "inbound_http_request": Counter(
                    name="inbound_http_request",
                    documentation="Incremental count of inbound HTTP requests",
                    labelnames=("endpoint", "response_code", "method"),
                ),
            },
            "histograms": {
                "request_latency": Histogram(
                    name="request_latency_seconds",
                    documentation="Request latency seconds",
                    labelnames=("endpoint", "response_code", "method"),
                )
            },
        }

        return metric_types
