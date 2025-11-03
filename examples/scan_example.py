"""Showcase scanning keys and objects stored in SkyShelve/PersistentObject."""

from __future__ import annotations

import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from skyshelve import PersistentObject, SkyShelve


class UserProfile(PersistentObject):
    def __init__(self, key: str, name: str = "", email: str = "") -> None:
        super().__init__(key)
        self.name = name
        self.email = email


def demo_skyshelve_scan(db_path: Path, lib_path: Path) -> None:
    with SkyShelve(str(db_path), lib_path=str(lib_path)) as store:
        store["user:alice"] = {"role": "admin", "active": True}
        store["user:bob"] = {"role": "analyst", "active": False}
        store["session:123"] = "temporary"

        print("-- SkyShelve scan for prefix 'user:' --")
        for key_bytes, value in store.scan("user:"):
            key = key_bytes.decode("utf-8", "replace")
            print(f"{key} -> {value}")


def demo_persistent_object_scan(db_path: Path, lib_path: Path) -> None:
    UserProfile.configure_storage(str(db_path), lib_path=str(lib_path))

    alice = UserProfile("alice", name="Alice", email="alice@example.com")
    bob = UserProfile("bob", name="Bob", email="bob@example.com")
    alice.save()
    bob.save()

    print("-- PersistentObject scan --")
    for profile in UserProfile.scan():
        print(f"{profile.key}: {profile.name} <{profile.email}>")


def main() -> None:
    data_root = PROJECT_ROOT / "data" / "scan-demo"
    data_root.mkdir(parents=True, exist_ok=True)
    lib_path = SRC_ROOT / "skyshelve" / "libskyshelve.so"

    demo_skyshelve_scan(data_root / "kv", lib_path)
    demo_persistent_object_scan(data_root / "objects", lib_path)


if __name__ == "__main__":
    main()
