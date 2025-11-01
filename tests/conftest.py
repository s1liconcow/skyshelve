import os
import platform
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


def _shared_library_name() -> str:
    system = platform.system()
    if system == "Windows":
        return "libbadgerdict.dll"
    if system == "Darwin":
        return "libbadgerdict.dylib"
    return "libbadgerdict.so"


def _build_shared_library(tmp_path_factory) -> Path:
    lib_name = _shared_library_name()
    pkg_dir = SRC_ROOT / "badgerdict"
    pkg_dir.mkdir(parents=True, exist_ok=True)
    lib_path = pkg_dir / lib_name
    if lib_path.exists():
        return lib_path

    if shutil.which("go") is None:
        pytest.skip("Go toolchain is not available in PATH; skipping BadgerDict tests.")

    env = os.environ.copy()
    gocache = tmp_path_factory.mktemp("gocache")
    env["GOCACHE"] = str(gocache)
    result = subprocess.run(
        ["go", "build", "-buildmode=c-shared", "-o", str(lib_path)],
        cwd=PROJECT_ROOT,
        env=env,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        msg = "\n".join(
            [
                "Unable to build badgerdict shared library.",
                result.stdout.strip(),
                result.stderr.strip(),
                "Run `go mod tidy` (network required) and retry.",
            ]
        )
        pytest.skip(msg)
    return lib_path


@pytest.fixture(scope="session")
def shared_library(tmp_path_factory) -> Path:
    return _build_shared_library(tmp_path_factory)


@pytest.fixture
def badger_dict_factory(tmp_path, shared_library):
    from badgerdict import BadgerDict

    db_root = tmp_path / "badger"
    db_root.mkdir()
    created = []

    def factory(*, in_memory: bool = False):
        if in_memory:
            store = BadgerDict(None, in_memory=True, lib_path=str(shared_library))
        else:
            store = BadgerDict(str(db_root), lib_path=str(shared_library))
        created.append(store)
        return store

    yield factory

    for store in created:
        try:
            store.close()
        except Exception:
            pass
