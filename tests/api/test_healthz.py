from fastapi.testclient import TestClient

from asgi import app

client = TestClient(app)


def test_healthz_routes() -> None:
    paths = ("/readyz", "/livez")
    for path in paths:
        resp = client.get(path, headers={})
        assert resp.status_code == 200
