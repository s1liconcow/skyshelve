## BadgerDict: Persistent Python Mapping Backed by BadgerDB

This project exposes a minimal dictionary-shaped interface to [BadgerDB](https://github.com/dgraph-io/badger) from Python. The core is a Go library compiled in `c-shared` mode which provides a handful of exported functions that manage a Badger key-value store and offer CRUD primitives. A small `ctypes` shim (`src/badgerdict/__init__.py`) loads the shared object and presents a Python-friendly API.

### Layout
- `badgerdict.go` &mdash; Go implementation of the shared library exports.
- `src/badgerdict/__init__.py` &mdash; Python package exposing the `BadgerDict` class.
- `src/badgerdict/libbadgerdict.*` &mdash; Platform-specific shared library produced by the Go compiler.
- `examples/demo.py` &mdash; Minimal usage example.

### Prerequisites
- Go 1.20 or newer (Go 1.25 used while developing the library).
- Python 3.9 or newer.
- A C toolchain for building cgo shared objects (e.g. `build-essential` on Debian/Ubuntu, Xcode Command Line Tools on macOS, or MSYS2 on Windows).

### Getting the Badger dependency

The Go module requires `github.com/dgraph-io/badger/v4`. If you have network access, run:

```bash
go mod tidy
```

This will download Badger and produce an up-to-date `go.sum`.

### Building the shared library

```bash
# Linux / macOS (run from the repository root)
go build -buildmode=c-shared -o src/badgerdict/libbadgerdict.so

# Windows (PowerShell)
go build -buildmode=c-shared -o src/badgerdict/libbadgerdict.dll
```

The command produces two files inside `src/badgerdict/`: the shared library (`.so`/`.dll`) and a matching C header (`libbadgerdict.h`). Keep the header if you plan to integrate through other FFI layers.

### Using from Python

Place the compiled shared library next to `src/badgerdict/__init__.py`, then interact with the store:

```python
from badgerdict import BadgerDict

with BadgerDict("data") as store:
    store["username"] = "alice"          # stored as UTF-8 text
    store["profile"] = {"plan": "pro"}  # auto-pickled
    store["avatar"] = b"\x89PNG"        # raw bytes stay bytes

    print(store["username"])           # 'alice'
    print(store["profile"])            # {'plan': 'pro'}
    print(store["avatar"])             # b'\x89PNG'
    store.sync()                        # flush to disk
```

By default the wrapper expects the shared library to be named `libbadgerdict.so`/`.dylib`/`.dll` in the same directory as the `badgerdict` package. If you relocate it, pass `lib_path="..."` when constructing `BadgerDict`.

Values that are bytes-like or `str` are stored as-is; everything else is serialized with `pickle.dumps` by default. Disable that behaviour with `BadgerDict(..., auto_pickle=False)` if you need stricter type enforcement.

To use an in-memory Badger store without touching disk, call `BadgerDict(None, in_memory=True)`.

### Quick demo & throughput glimpse

```bash
python examples/demo.py
```

The demo populates a store in `./data`, reads a few values, and runs a small threaded benchmark, reporting elapsed time and effective operations-per-second before flushing the data to disk.

### Running the concurrent stress tests

```bash
pytest tests/test_concurrency.py
```

The suite spins up multiple worker threads that hammer a single `BadgerDict` instance with random read/write/delete workloads, then verifies durability by reopening the store. Building the shared library is attempted automatically; if the Badger dependency has not been downloaded yet you will see a skip message reminding you to run `go mod tidy`.

### Building wheels / sdists

This project uses a `pyproject.toml` with a `setuptools` backend and `src/` layout. After building the Go shared library for your platform, create distributable artifacts with:

```bash
python -m pip install build
python -m build
```

The resulting wheel embeds the shared object located in `src/badgerdict/`.

### Cleanup & caveats
- Always call `close()` (or use the context manager) to release the underlying Badger handle; Badger flushes outstanding writes on close.
- Empty string keys are not supported by the wrapper.
- If you need advanced Badger features (TTL, transactions, iteration), extend `badgerdict.go` with additional exported functions and surface them through `src/badgerdict/__init__.py`.
