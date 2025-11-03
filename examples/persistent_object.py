"""Demonstrate using PersistentObject for cross-process state sharing."""

import multiprocessing
import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from skyshelve import PersistentObject


class TaskQueue(PersistentObject):
    def __init__(self, key: str, tasks=None) -> None:
        super().__init__(key)
        self.tasks = list(tasks or [])
        self.completed = []

    def enqueue(self, item: str) -> None:
        self.tasks.append(item)

    def dequeue(self) -> str | None:
        if not self.tasks:
            return None
        item = self.tasks.pop(0)
        self.completed.append(item)
        return item


def _worker(db_path: str, lib_path: str, worker_id: int) -> None:
    TaskQueue.configure_storage(db_path, lib_path=lib_path)

    def mutator(queue: TaskQueue) -> None:
        job = queue.dequeue()
        if job is not None:
            print(f"worker {worker_id} processed {job}")

    TaskQueue.update("jobs", default_factory=lambda: TaskQueue("jobs"), mutator=mutator)


def main() -> None:
    db_path = PROJECT_ROOT / "data" / "tasks"
    lib_path = SRC_ROOT / "skyshelve" / "libskyshelve.so"
    TaskQueue.configure_storage(str(db_path), lib_path=str(lib_path))

    queue = TaskQueue("jobs")
    for value in ("write docs", "run benchmarks", "publish release"):
        queue.enqueue(value)
    queue.save()

    ctx = multiprocessing.get_context("spawn")
    procs = [ctx.Process(target=_worker, args=(str(db_path), str(lib_path), i)) for i in range(3)]
    for proc in procs:
        proc.start()
    for proc in procs:
        proc.join()

    final = TaskQueue.load("jobs")
    print("remaining tasks:", final.tasks)
    print("completed tasks:", final.completed)


if __name__ == "__main__":
    main()
