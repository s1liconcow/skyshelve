import ctypes
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


_SLATEDB_HANDLE = None


def _shared_library_name() -> str:
    system = platform.system()
    if system == "Windows":
        return "libskyshelve.dll"
    if system == "Darwin":
        return "libskyshelve.dylib"
    return "libskyshelve.so"


def _build_shared_library(tmp_path_factory) -> Path:
    lib_name = _shared_library_name()
    pkg_dir = SRC_ROOT / "skyshelve"
    pkg_dir.mkdir(parents=True, exist_ok=True)
    lib_path = pkg_dir / lib_name
    slate_dir = PROJECT_ROOT / "external" / "slatedb" / "target" / "release"
    if lib_path.exists():
        lib_path.unlink()

    if shutil.which("go") is None:
        pytest.skip("Go toolchain is not available in PATH; skipping Skyshelve tests.")

    env = os.environ.copy()
    gocache = tmp_path_factory.mktemp("gocache")
    env["GOCACHE"] = str(gocache)
    if slate_dir.exists():
        existing = env.get("CGO_LDFLAGS")
        flag = f"-L{slate_dir} -Wl,-rpath,{slate_dir}"
        env["CGO_LDFLAGS"] = f"{existing} {flag}".strip() if existing else flag
        _ensure_runtime_search_path(slate_dir)

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
                "Unable to build skyshelve shared library.",
                result.stdout.strip(),
                result.stderr.strip(),
                "Run `go mod tidy` (network required) and retry.",
            ]
        )
        pytest.skip(msg)
    return lib_path


def _ensure_runtime_search_path(slate_dir: Path) -> None:
    if not slate_dir.exists():
        return

    if os.name == "nt":
        key = "PATH"
    elif sys.platform == "darwin":
        key = "DYLD_LIBRARY_PATH"
    else:
        key = "LD_LIBRARY_PATH"

    current = os.environ.get(key)
    pieces = [] if not current else current.split(os.pathsep)
    str_dir = str(slate_dir)
    if str_dir not in pieces:
        os.environ[key] = os.pathsep.join([str_dir, *pieces]) if pieces else str_dir

    # Preload the SlateDB native library so libskyshelve can link against it.
    global _SLATEDB_HANDLE

    lib_names = {
        "win32": "slatedb_go.dll",
        "darwin": "libslatedb_go.dylib",
    }
    shared = lib_names.get(sys.platform, "libslatedb_go.so")
    candidate = slate_dir / shared
    if candidate.exists():
        mode = getattr(ctypes, "RTLD_GLOBAL", 0)
        if hasattr(ctypes, "RTLD_NOW"):
            mode = mode | ctypes.RTLD_NOW
        handle = ctypes.CDLL(str(candidate), mode=mode) if mode else ctypes.CDLL(str(candidate))
        _SLATEDB_HANDLE = handle


@pytest.fixture(scope="session")
def shared_library(tmp_path_factory) -> Path:
    return _build_shared_library(tmp_path_factory)


@pytest.fixture
def skyshelve_factory(tmp_path, shared_library):
    from skyshelve import SkyShelve

    db_root = tmp_path / "badger"
    db_root.mkdir()
    created = []

    def factory(*, in_memory: bool = False):
        if in_memory:
            store = SkyShelve(None, in_memory=True, lib_path=str(shared_library))
        else:
            store = SkyShelve(str(db_root), lib_path=str(shared_library))
        created.append(store)
        return store

    yield factory

    for store in created:
        try:
            store.close()
        except Exception:
            pass
