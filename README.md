## Skyshelve: Backend-Agnostic Persistent Python Mapping

Skyshelve exposes a minimal dictionary-shaped interface to embedded key-value
stores such as [BadgerDB](https://github.com/dgraph-io/badger) and
[SlateDB](https://slatedb.io/). The core is a Go library compiled in
`c-shared` mode which provides a handful of exported functions that manage the
selected backend and offer CRUD primitives. A small `ctypes` shim
(`src/skyshelve/__init__.py`) loads the shared object and presents a
Python-friendly API.

### Layout
- `skyshelve.go` &mdash; Go implementation of the shared library exports.
- `src/skyshelve/__init__.py` &mdash; Python package exposing the `SkyShelve` class.
- `src/skyshelve/libskyshelve.*` &mdash; Platform-specific shared library produced by the Go compiler.
- `PersistentObject` base class (in `src/skyshelve/__init__.py`) offers an
  inheritable ORM-style helper that uses file locks so multiple processes can
  safely read and mutate shared records.
- `examples/demo.py` &mdash; Minimal usage example.
- `examples/scan_example.py` &mdash; Demonstrates scanning keys and persistent objects.
- `examples/indexed_profiles.py` &mdash; Pydantic-backed parent/child models with secondary indexes.
- `examples/simple_counter.py` &mdash; Uses `PersistentObject` to track run counts.
- `examples/slatedb_backend.py` &mdash; Demonstrates opting into the SlateDB backend (local and AWS).
- `scripts/build_shared.py` &mdash; Helper script for compiling the Go shared library with SlateDB linkage.

### Prerequisites
- Go 1.20 or newer (Go 1.25 used while developing the library).
- Python 3.9 or newer.
- A C toolchain for building cgo shared objects (e.g. `build-essential` on Debian/Ubuntu, Xcode Command Line Tools on macOS, or MSYS2 on Windows).

### Installation

The package is published on PyPI; install it with:

```bash
pip install skyshelve
```

You will still need a platform-appropriate Go build of the shared library if
you are building from source or developing locally (see below).

### Getting backend dependencies

The Go module requires `github.com/dgraph-io/badger/v4` (always) and
optionally `slatedb.io/slatedb-go` when you intend to use SlateDB. With
network access, run:

```bash
go mod tidy
```

This will download both dependencies and produce an up-to-date `go.sum`.

### Building the shared library

```bash
# Cross-platform helper (detects the output filename and wires in SlateDB linkage)
python scripts/build_shared.py

# Linux / macOS (run from the repository root)
go build -buildmode=c-shared -o src/skyshelve/libskyshelve.so

# Windows (PowerShell)
go build -buildmode=c-shared -o src/skyshelve/libskyshelve.dll
```

The command produces two files inside `src/skyshelve/`: the shared library (`.so`/`.dll`) and a matching C header (`libskyshelve.h`). Keep the header if you plan to integrate through other FFI layers.

### Using from Python

Place the compiled shared library next to `src/skyshelve/__init__.py`, then interact with the store:

```python
from skyshelve import SkyShelve

with SkyShelve("data") as store:
    store["username"] = "alice"          # stored as UTF-8 text
    store["profile"] = {"plan": "pro"}  # auto-pickled
    store["avatar"] = b"\x89PNG"        # raw bytes stay bytes

    print(store["username"])           # 'alice'
    print(store["profile"])            # {'plan': 'pro'}
    print(store["avatar"])             # b'\x89PNG'
    store.sync()                        # flush to disk
```

By default the wrapper expects the shared library to be named `libskyshelve.so`/`.dylib`/`.dll` in the same directory as the `skyshelve` package. If you relocate it, pass `lib_path="..."` when constructing `SkyShelve`.

**Compatibility note:** the class is also exported as `BadgerDict` for projects
that previously depended on the old package name.

Values that are bytes-like or `str` are stored as-is; everything else is serialized with `pickle.dumps` by default. Disable that behaviour with `SkyShelve(..., auto_pickle=False)` if you need stricter type enforcement.

For richer models, inherit from `PersistentObject` and call
`YourModel.configure_storage(...)` once per process, then use `save()`,
`load()`, and `update()` to modify state atomically across processes.

Alternatively, set private configuration attributes on your model. When no path
is provided, a default of `./data/<model-name-lowercase>` is used. The helper
`PersistentBaseModel` combines Pydantic with `PersistentObject`, automatically
serializing models (and stdlib dataclasses) while keeping secondary indexes in
sync:

```python
from skyshelve import PersistentBaseModel


class User(PersistentBaseModel):
    __persistent_key_field__ = "username"
    __persistent_path__ = "data/users"
    __persistent_secondary_indexes__ = {"email": lambda u: [u.email]}

    username: str
    email: str


User(username="alice", email="alice@example.com").save()
print(User.scan_index("email", "alice@example.com"))  # -> [User(...)]
print(User.children("email", "alice@example.com"))     # same as scan_index
```

### Using the SlateDB backend

The same Python API can target [SlateDB](https://slatedb.io/) by passing a
`slatedb:` URI-like path (or JSON payload) to `SkyShelve` or any
`PersistentObject` configuration. The helper `examples/slatedb_backend.py`
demonstrates both the default local provider and AWS configuration.

```python
from skyshelve import SkyShelve

with SkyShelve("slatedb://") as store:  # defaults to ./data/slatedb
    store["answer"] = 42
```

The path segment after `slatedb://` points to the SlateDB data directory. Use a
JSON payload for advanced configuration—e.g.
`"slatedb:{\"path\":\"/srv/slate\",\"store\":{\"provider\":\"local\"}}"`—which
is forwarded to the SlateDB Go client. When no path is supplied, the library
stores data under `./data/slatedb` by default. Badger-backed stores still
expect an explicit path unless you use `PersistentObject`, which defaults to
`./data/<model-name>`.

To target AWS S3 (or an S3-compatible provider) supply the AWS store
configuration:

```python
import json
from skyshelve import SkyShelve

config = {
    "path": "/tmp/slatedb-cache",
    "store": {
        "provider": "aws",
        "aws": {
            "bucket": "my-bucket",
            "region": "us-west-2",
            # Optional: "endpoint": "https://s3.us-west-2.amazonaws.com",
        },
    },
}

with SkyShelve(f"slatedb:{json.dumps(config)}") as store:
    store["key"] = "value"
```

The `examples/slatedb_backend.py` script reads the standard AWS environment
variables shown in `PROD_ENV.sh` (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`,
`AWS_REGION`/`AWS_DEFAULT_REGION`, `AWS_ENDPOINT_URL_S3`, and `BUCKET_NAME`) to
configure the AWS provider automatically. Override `SKYSHELVE_PROVIDER` or
`SKYSHELVE_CACHE_PATH` if you need to force a provider or change the local
cache location.

SlateDB support relies on the upstream Go bindings. Build the native library in
the SlateDB repository with `cargo build -p slatedb-go --release`, then ensure
the resulting shared object (often `libslatedb_go.so`/`.dylib`/`.dll`) is
discoverable at run time—typically by adding it to your system library path or
placing it next to `libskyshelve` before running `go build`. The helper script
`scripts/build_shared.py` automatically wires in the correct linker flags and
`rpath` settings when SlateDB is present under `external/slatedb/`.

To use an in-memory Badger store without touching disk, call
`SkyShelve(None, in_memory=True)`.

### Quick demo & throughput glimpse

```bash
python examples/demo.py
```

The demo populates a store in `./data`, reads a few values, and runs a small threaded benchmark, reporting elapsed time and effective operations-per-second before flushing the data to disk.

### Running the concurrent stress tests

```bash
pytest tests/test_concurrency.py
```

The suite spins up multiple worker threads that hammer a single `SkyShelve`
instance with random read/write/delete workloads, then verifies durability by
reopening the store. Building the shared library is attempted automatically;
if the Go dependencies have not been downloaded yet you will see a skip
message reminding you to run `go mod tidy`.

### Building wheels / sdists

This project uses a `pyproject.toml` with a `setuptools` backend and `src/` layout. After building the Go shared library for your platform, create distributable artifacts with:

```bash
python -m pip install build
python -m build
```

The resulting wheel embeds the shared object located in `src/skyshelve/`.

### Automated releases

A GitHub Actions workflow (`.github/workflows/publish.yml`) builds the shared
library, runs the test suite, and publishes wheels/sdists to PyPI whenever a
release is published (or the workflow is triggered manually). Configure the
repository secret `PYPI_API_TOKEN` with an API token generated from your PyPI
account before running the workflow.

### Cleanup & caveats
- Always call `close()` (or use the context manager) to release the underlying handle; the backend flushes outstanding writes on close.
- Empty string keys are not supported by the wrapper.
- If you need advanced backend features (TTL, transactions, iteration), extend `skyshelve.go` with additional exported functions and surface them through `src/skyshelve/__init__.py`.
