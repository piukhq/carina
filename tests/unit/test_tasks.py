from uuid import uuid4

import httpretty

from pytest_mock import MockerFixture

from app.tasks import send_request_with_metrics


@httpretty.activate
def test_send_request_with_metrics_exclude_val(mocker: MockerFixture, run_task_with_metrics: None) -> None:
    uuid_val = str(uuid4())
    base_url = "http://sample-domain"

    httpretty.register_uri("GET", f"{base_url}/{uuid_val}/test/url", body="OK", status=200)
    mocked_metric = mocker.patch("app.tasks.prometheus.outgoing_http_requests_total")

    resp = send_request_with_metrics(
        "GET",
        "{base_url}/{uuid_val}/test/url",
        {
            "base_url": base_url,
            "uuid_val": uuid_val,
        },
        exclude_from_label_url=["uuid_val"],
        timeout=(3.03, 10),
    )
    assert resp.status_code == 200
    mocked_metric.labels.assert_called_once_with(
        app="carina", method="GET", response="HTTP_200", exception=None, url=f"{base_url}/[uuid_val]/test/url"
    )


@httpretty.activate
def test_send_request_with_metrics_no_excluded_val(mocker: MockerFixture, run_task_with_metrics: None) -> None:
    uuid_val = str(uuid4())
    base_url = "http://sample-domain"

    httpretty.register_uri("GET", f"{base_url}/{uuid_val}/test/url", body="OK", status=200)
    mocked_metric = mocker.patch("app.tasks.prometheus.outgoing_http_requests_total")

    resp = send_request_with_metrics(
        "GET",
        "{base_url}/{uuid_val}/test/url",
        {
            "base_url": base_url,
            "uuid_val": uuid_val,
        },
        exclude_from_label_url=[],
        timeout=(3.03, 10),
    )
    assert resp.status_code == 200
    mocked_metric.labels.assert_called_once_with(
        app="carina", method="GET", response="HTTP_200", exception=None, url=f"{base_url}/{uuid_val}/test/url"
    )
