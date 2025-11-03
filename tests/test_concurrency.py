import os
import random
import threading
import time
from collections import Counter

import pytest


def _random_payload(size: int) -> bytes:
    return os.urandom(size)


def test_concurrent_writers(skyshelve_factory):
    worker_count = 16
    items_per_worker = 200
    written = {}
    write_lock = threading.Lock()

    with skyshelve_factory() as store:
        errors = []

        def worker(worker_id: int) -> None:
            try:
                for i in range(items_per_worker):
                    key = f"worker-{worker_id}-{i}".encode()
                    value = _random_payload(64)
                    store[key] = value
                    roundtrip = store[key]
                    assert roundtrip == value
                    with write_lock:
                        written[key] = value
            except Exception as exc:  # pragma: no cover - debugging aid
                errors.append(exc)

        threads = [threading.Thread(target=worker, args=(wid,)) for wid in range(worker_count)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        if errors:
            raise AssertionError(f"worker failures: {errors!r}")

        # Force durability
        store.sync()

    # Reopen to verify persistence
    with skyshelve_factory() as reopened:
        for key, expected in written.items():
            assert reopened[key] == expected


@pytest.mark.parametrize("worker_count,ops_per_worker", [(8, 500), (4, 1000)])
def test_mixed_random_workload(worker_count, ops_per_worker, skyshelve_factory):
    keys = [f"key-{i}".encode() for i in range(64)]
    with skyshelve_factory(in_memory=True) as store:
        errors = []
        stats = Counter()
        lock = threading.Lock()

        def worker(worker_id: int) -> None:
            rng = random.Random(worker_id)
            try:
                for _ in range(ops_per_worker):
                    key = rng.choice(keys)
                    coin = rng.random()
                    if coin < 0.5:
                        payload = _random_payload(128)
                        store[key] = payload
                        result = store.get(key, default=None)
                        if result is not None:
                            assert isinstance(result, bytes)
                        with lock:
                            stats["writes"] += 1
                    elif coin < 0.8:
                        _ = store.get(key, default=None)
                        with lock:
                            stats["reads"] += 1
                    else:
                        store.delete(key)
                        with lock:
                            stats["deletes"] += 1
            except Exception as exc:  # pragma: no cover
                errors.append(exc)

        threads = [threading.Thread(target=worker, args=(wid,)) for wid in range(worker_count)]
        start = time.perf_counter()
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        duration = time.perf_counter() - start

        if errors:
            raise AssertionError(f"worker failures: {errors!r}")

        total_ops = sum(stats.values())
        assert total_ops == worker_count * ops_per_worker
        ops_per_second = total_ops / duration if duration else float("inf")
        # Ensure we made progress and throughput is sensible.
        assert ops_per_second > 10
