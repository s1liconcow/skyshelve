"""Demonstrate selecting the SlateDB backend via the ``slatedb:`` URI.

This example expects the SlateDB Go bindings to be built so that
``libslatedb_go`` is available in ``external/slatedb/target/release`` (see the
README instructions). When running on macOS or Linux, the script
opportunistically injects that directory into the appropriate dynamic library
search path so the compiled ``libskyshelve`` can locate SlateDB at import time.

Environment overrides:

- ``SKYSHELVE_PROVIDER``: Force ``local`` or ``aws`` (defaults to auto-detect).
- ``SKYSHELVE_CACHE_PATH``: Local cache directory for SlateDB (defaults to
  ``./data/slatedb-demo``).
- ``BUCKET_NAME``: Target S3 bucket when using the AWS provider.
- ``AWS_REGION`` / ``AWS_DEFAULT_REGION``: Region for the bucket.
- ``AWS_ENDPOINT_URL_S3``: Optional S3-compatible endpoint.
- ``AWS_ACCESS_KEY_ID`` / ``AWS_SECRET_ACCESS_KEY``: Standard AWS credentials
  used by SlateDB when hitting S3.
"""

from __future__ import annotations

import ctypes
import json
import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

SLATE_LIB_DIR = PROJECT_ROOT / "external" / "slatedb" / "target" / "release"
DEFAULT_CACHE_PATH = PROJECT_ROOT / "data" / "slatedb-demo"


def _ensure_native_library_visible() -> None:
    """Inject the SlateDB build output into the dynamic loader path."""

    if not SLATE_LIB_DIR.exists():
        print(
            "SlateDB native library not found. Build it with `cargo build -p slatedb-go --release`",
            file=sys.stderr,
        )
        raise SystemExit(1)

    if sys.platform.startswith("linux"):
        env_var = "LD_LIBRARY_PATH"
    elif sys.platform == "darwin":
        env_var = "DYLD_LIBRARY_PATH"
    elif os.name == "nt":  # pragma: no cover - Windows path handling differs
        env_var = "PATH"
    else:  # pragma: no cover - unsupported platform
        env_var = None

    if not env_var:
        return

    current = os.environ.get(env_var)
    entries = [] if not current else current.split(os.pathsep)
    slate_entry = str(SLATE_LIB_DIR)
    if slate_entry not in entries:
        if not os.environ.get("SKYSHELVE_SLATE_RERUN"):
            os.environ[env_var] = os.pathsep.join([slate_entry, *entries]) if entries else slate_entry
            os.environ["SKYSHELVE_SLATE_RERUN"] = "1"
            os.execvpe(sys.executable, [sys.executable, *sys.argv], os.environ)
        print(
            f"Add {slate_entry} to {env_var} before running this script",
            file=sys.stderr,
        )
        raise SystemExit(1)


_ensure_native_library_visible()

SLATE_LIB_HANDLE = None


def _preload_slate_library() -> None:
    """Attempt to load libslatedb_go eagerly so ctypes can resolve symbols."""

    global SLATE_LIB_HANDLE

    shared_name = {
        "win32": "slatedb_go.dll",
        "darwin": "libslatedb_go.dylib",
    }.get(sys.platform, "libslatedb_go.so")

    candidate = SLATE_LIB_DIR / shared_name
    if not candidate.exists():
        return

    if SLATE_LIB_HANDLE is None:
        try:
            mode = getattr(ctypes, "RTLD_GLOBAL", 0)
            SLATE_LIB_HANDLE = ctypes.CDLL(str(candidate), mode=mode) if mode else ctypes.CDLL(str(candidate))
        except OSError as exc:  # pragma: no cover - depends on environment setup
            print(f"Unable to preload {candidate}: {exc}", file=sys.stderr)


_preload_slate_library()

from skyshelve import PersistentObject, SkyShelve, SkyshelveError, slatedb_uri


def _slatedb_config_uri() -> str:
    provider_override = os.environ.get("SKYSHELVE_PROVIDER")
    cache_path = os.environ.get("SKYSHELVE_CACHE_PATH", str(DEFAULT_CACHE_PATH)).strip()

    if provider_override:
        provider = provider_override.strip().lower()
    else:
        has_aws_creds = bool(os.environ.get("AWS_ACCESS_KEY_ID") and os.environ.get("AWS_SECRET_ACCESS_KEY"))
        bucket_env = (os.environ.get("BUCKET_NAME") or os.environ.get("AWS_S3_BUCKET") or "").strip()
        provider = "aws" if has_aws_creds and bucket_env else "local"

    if provider not in {"local", "aws"}:
        raise SystemExit(f"Unsupported SKYSHELVE_PROVIDER '{provider}' (use 'local' or 'aws')")

    if provider == "aws":
        bucket = (os.environ.get("BUCKET_NAME") or os.environ.get("AWS_S3_BUCKET") or "").strip()
        region = (os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION") or "").strip()
        if not bucket or not region:
            raise SystemExit("BUCKET_NAME and AWS_REGION (or AWS_DEFAULT_REGION) must be set for AWS provider")
        aws_cfg = {"bucket": bucket, "region": region}
        endpoint = (os.environ.get("AWS_ENDPOINT_URL_S3") or "").strip()
        if endpoint:
            aws_cfg["endpoint"] = endpoint
        store_cfg = {"provider": "aws", "aws": aws_cfg}
    else:
        store_cfg = {"provider": "local"}

    return slatedb_uri(cache_path, store=store_cfg)


SLATE_URI = _slatedb_config_uri()
print(f"Using SlateDB at {SLATE_URI}")


class LoginCounter(PersistentObject):
    """Track login counts per user, stored inside SlateDB."""

    __persistent_path__ = SLATE_URI
    __persistent_secondary_indexes__ = {"region": lambda counter: [counter.region]}

    def __init__(self, username: str, *, region: str, logins: int = 0) -> None:
        super().__init__(username)
        self.region = region
        self.logins = logins

    @property
    def username(self) -> str:
        return self.key


def log_login(username: str, region: str) -> None:
    """Increment a user's login counter, creating the record if missing."""

    def default_factory() -> LoginCounter:
        return LoginCounter(username, region=region)

    def mutator(record: LoginCounter) -> None:
        record.region = region
        record.logins += 1

    LoginCounter.update(username, default_factory=default_factory, mutator=mutator)


def main() -> None:
    with SkyShelve(SLATE_URI) as store:
        total_logins = store.get("total_logins", 0)
        store["total_logins"] = total_logins + 1
        store.sync()
        print(f"Global login counter incremented: {total_logins} -> {total_logins + 1}")

    log_login("alice", "us-west")
    log_login("bob", "eu-central")
    log_login("alice", "us-west")
    west_coast = LoginCounter.scan_index("region", "us-west")
    for user in west_coast:
        print(f"{user.username} has logged in {user.logins} time(s) from {user.region}")


if __name__ == "__main__":
    try:
        main()
    except SkyshelveError as exc:
        detail = SkyShelve._last_error()
        if detail:
            print(f"SkyShelve reported: {detail}", file=sys.stderr)
        raise
