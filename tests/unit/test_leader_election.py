import socket

from uuid import uuid4

from app.core.config import redis, settings
from app.scheduled_tasks.scheduler import run_only_if_leader


class FakeScheduler:
    name = "fake-scheduler"

    def __init__(self, id: str) -> None:
        self.id = id


def test_run_only_if_leader_is_leader() -> None:
    call_count = 0

    @run_only_if_leader(runner=FakeScheduler(id=str(uuid4())))
    def test_func() -> None:
        nonlocal call_count
        call_count += 1

    test_func()
    assert call_count == 1
    test_func()
    assert call_count == 2


def test_run_only_if_leader_is_leader__not_leader() -> None:
    call_count = 0

    @run_only_if_leader(runner=FakeScheduler(id=str(uuid4())))
    def test_func() -> None:
        nonlocal call_count
        call_count += 1

    redis.set(f"{settings.REDIS_KEY_PREFIX}{FakeScheduler.name}:{test_func.__qualname__}", "something-else")
    test_func()
    assert call_count == 0
    test_func()
    assert call_count == 0


def test_run_only_if_leader_is_leader__lost_leadership() -> None:
    call_count = 0

    def test_func() -> None:
        nonlocal call_count
        call_count += 1

    try:
        runner_1 = FakeScheduler(id=str(uuid4()))
        runner_2 = FakeScheduler(id=str(uuid4()))

        run_only_if_leader(runner=runner_1)(test_func)()
        assert call_count == 1

        run_only_if_leader(runner=runner_2)(test_func)()
        assert call_count == 1

        run_only_if_leader(runner=runner_1)(test_func)()
        assert call_count == 2

        redis.set(
            f"{settings.REDIS_KEY_PREFIX}{FakeScheduler.name}:{test_func.__qualname__}",
            f"{socket.gethostname()}-{runner_2.id}",
        )
        run_only_if_leader(runner=runner_2)(test_func)()
        assert call_count == 3

        run_only_if_leader(runner=runner_1)(test_func)()
        assert call_count == 3

    finally:
        redis.delete(f"{settings.REDIS_KEY_PREFIX}{FakeScheduler.name}:{test_func.__qualname__}")
