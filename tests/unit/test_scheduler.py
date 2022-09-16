from uuid import uuid4

from carina.core.config import redis, settings
from carina.scheduled_tasks.scheduler import acquire_lock


class FakeScheduler:
    name = "fake-scheduler"

    def __init__(self, uid: str) -> None:
        self.uid = uid


def test_acquire_lock_ok() -> None:
    call_count = 0

    @acquire_lock(runner=FakeScheduler(uid=str(uuid4())))
    def test_func() -> None:
        nonlocal call_count
        call_count += 1

    test_func()
    assert call_count == 1
    test_func()
    assert call_count == 2


def test_acquire_lock_not_ok() -> None:
    call_count = 0

    @acquire_lock(runner=FakeScheduler(uid=str(uuid4())))
    def test_func() -> None:
        nonlocal call_count
        call_count += 1

    redis.set(f"{settings.REDIS_KEY_PREFIX}{FakeScheduler.name}:{test_func.__qualname__}", "something-else")
    try:
        test_func()
        assert call_count == 0
        test_func()
        assert call_count == 0
    finally:
        redis.delete(f"{settings.REDIS_KEY_PREFIX}{FakeScheduler.name}:{test_func.__qualname__}")


def test_acquire_lock_expired() -> None:
    call_count = 0

    def test_func() -> None:
        nonlocal call_count
        call_count += 1

    runner_1 = FakeScheduler(uid=str(uuid4()))
    runner_2 = FakeScheduler(uid=str(uuid4()))

    acquire_lock(runner=runner_1)(test_func)()
    assert call_count == 1

    redis.delete(f"{settings.REDIS_KEY_PREFIX}{FakeScheduler.name}:{test_func.__qualname__}")
    acquire_lock(runner=runner_2)(test_func)()
    assert call_count == 2
