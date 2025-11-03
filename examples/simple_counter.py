"""Simple counter using PersistentObject that increments on every run."""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from skyshelve import PersistentObject


class RunCounter(PersistentObject):
    def __init__(self, key: str, value: int = 0) -> None:
        super().__init__(key)
        self.value = value

    def to_record(self):  # type: ignore[override]
        return {"value": self.value}

    @classmethod
    def from_record(cls, key, record):  # type: ignore[override]
        return cls(key, int(record.get("value", 0)))


def main() -> None:
    db_path = PROJECT_ROOT / "data" / "counter"
    lib_path = SRC_ROOT / "skyshelve" / "libskyshelve.so"
    RunCounter.configure_storage(str(db_path), lib_path=str(lib_path))
    counter = RunCounter.load("counter", default=RunCounter("counter"))
    counter.value += 1
    counter.save()

    print(f"This script has been run {counter.value} times.")


if __name__ == "__main__":
    main()
