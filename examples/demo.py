import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from skyshelve import SkyShelve


def simple_demo(store: SkyShelve) -> None:
    store["greeting"] = "hello world"
    store["count"] = b"\x00\x01"
    store["settings"] = {"theme": "dark", "features": ["badger", "dict"]}

    print("greeting:", store["greeting"])
    print("count bytes:", store["count"])
    print("settings object:", store["settings"])
    print("contains 'missing'?", "missing" in store)


def run_benchmark(store: SkyShelve, workers: int = 8, ops_per_worker: int = 1_000) -> None:
    def worker(worker_id: int) -> None:
        for i in range(ops_per_worker):
            key = f"bench-{worker_id}-{i}".encode()
            payload = os.urandom(128)
            store[key] = payload
            _ = store[key]
            store.sync()

    start = time.perf_counter()
    with ThreadPoolExecutor(max_workers=workers) as executor:
        for wid in range(workers):
            executor.submit(worker, wid)
    duration = time.perf_counter() - start

    total_ops = workers * ops_per_worker * 2  # set + get
    throughput = total_ops / duration if duration else float("inf")
    print(f"benchmark: workers={workers}, ops/worker={ops_per_worker}")
    print(f"elapsed={duration:.3f}s, total ops={total_ops}, throughput={throughput:,.0f} ops/s")


def main() -> None:
    db_path = Path("data").resolve()
    db_path.mkdir(parents=True, exist_ok=True)

    with SkyShelve(str(db_path)) as store:
        simple_demo(store)
        run_benchmark(store)
        store.sync()


if __name__ == "__main__":
    main()
