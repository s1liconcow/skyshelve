import ctypes
import importlib
import dataclasses
import json
import os
import pickle
import struct
import tempfile
import threading
from contextlib import contextmanager, nullcontext
from pathlib import Path
from typing import Any, Callable, ClassVar, Dict, Iterable, List, Optional, Sequence, Tuple, Union

try:  # POSIX-only import guarded for portability.
    import fcntl  # type: ignore[attr-defined]
except ImportError:  # pragma: no cover - Windows fallback handled separately.
    fcntl = None

try:  # Windows-specific lock helpers.
    import msvcrt  # type: ignore[attr-defined]
except ImportError:  # pragma: no cover
    msvcrt = None


BytesLike = Union[bytes, bytearray, memoryview, str]
_MISSING = object()
_VALUE_RAW = 0x00
_VALUE_STR = 0x01
_VALUE_PICKLED = 0x02

try:  # Optional dependency
    from pydantic import BaseModel as _PydanticBaseModel  # type: ignore
    from pydantic import PrivateAttr as _PydanticPrivateAttr  # type: ignore
except ImportError:  # pragma: no cover - pydantic optional
    _PydanticBaseModel = None  # type: ignore
    _PydanticPrivateAttr = None  # type: ignore


class _FileLock:
    """Minimal cross-platform advisory file lock for inter-process coordination."""

    def __init__(self, path: Path) -> None:
        self._path = path
        self._fh: Optional[Any] = None

    def acquire(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(self._path, os.O_RDWR | os.O_CREAT, 0o666)
        self._fh = os.fdopen(fd, "r+b", buffering=0)
        if fcntl is not None:
            fcntl.flock(self._fh.fileno(), fcntl.LOCK_EX)
        elif msvcrt is not None:  # pragma: no cover - Windows only
            # Ensure there is at least one byte to lock on Windows.
            if self._fh.tell() == 0:
                self._fh.write(b"\0")
                self._fh.flush()
            msvcrt.locking(self._fh.fileno(), msvcrt.LK_LOCK, 1)
        else:  # pragma: no cover - platforms without locking support
            raise RuntimeError("file locking is not supported on this platform")

    def release(self) -> None:
        if not self._fh:
            return
        try:
            if fcntl is not None:
                fcntl.flock(self._fh.fileno(), fcntl.LOCK_UN)
            elif msvcrt is not None:  # pragma: no cover
                msvcrt.locking(self._fh.fileno(), msvcrt.LK_UNLCK, 1)
        finally:
            self._fh.close()
            self._fh = None

    def __enter__(self) -> "_FileLock":
        self.acquire()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.release()


__all__ = [
    "SkyShelve",
    "SkyshelveError",
    "PersistentObject",
    "persistent_model",
    "BadgerDict",
    "BadgerError",
    "slatedb_uri",
]


class SkyshelveError(Exception):
    """Raised when the underlying storage interaction fails."""


class SkyShelve:
    """Minimal dictionary-style wrapper backed by pluggable Go-backed stores."""

    _init_lock = threading.Lock()
    _lib: Optional[ctypes.CDLL] = None

    def __init__(
        self,
        path: Optional[str] = None,
        *,
        in_memory: bool = False,
        lib_path: Optional[str] = None,
        auto_pickle: bool = True,
    ) -> None:
        self._ensure_library(lib_path)
        self._handle = self._open(path, in_memory)
        self._auto_pickle = auto_pickle

    @classmethod
    def _ensure_library(cls, lib_path: Optional[str]) -> None:
        if cls._lib is not None:
            return
        with cls._init_lock:
            if cls._lib is not None:
                return
            inferred_path = lib_path or cls._default_library_path()
            cls._lib = ctypes.CDLL(inferred_path)
            cls._configure_signatures()

    @classmethod
    def _configure_signatures(cls) -> None:
        assert cls._lib is not None
        lib = cls._lib
        lib.Open.argtypes = [ctypes.c_char_p, ctypes.c_int]
        lib.Open.restype = ctypes.c_size_t

        lib.Close.argtypes = [ctypes.c_size_t]
        lib.Close.restype = ctypes.c_int

        lib.Set.argtypes = [ctypes.c_size_t, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p, ctypes.c_int]
        lib.Set.restype = ctypes.c_int

        lib.Get.argtypes = [ctypes.c_size_t, ctypes.c_char_p, ctypes.c_int, ctypes.POINTER(ctypes.c_int)]
        lib.Get.restype = ctypes.c_void_p

        lib.Delete.argtypes = [ctypes.c_size_t, ctypes.c_char_p, ctypes.c_int]
        lib.Delete.restype = ctypes.c_int

        lib.Sync.argtypes = [ctypes.c_size_t]
        lib.Sync.restype = ctypes.c_int

        lib.Scan.argtypes = [ctypes.c_size_t, ctypes.c_char_p, ctypes.c_int, ctypes.POINTER(ctypes.c_int)]
        lib.Scan.restype = ctypes.c_void_p

        lib.Apply.argtypes = [ctypes.c_size_t, ctypes.c_void_p, ctypes.c_int]
        lib.Apply.restype = ctypes.c_int

        lib.LastError.argtypes = []
        lib.LastError.restype = ctypes.c_void_p

        lib.FreeCString.argtypes = [ctypes.c_void_p]
        lib.FreeCString.restype = None

        lib.FreeBuffer.argtypes = [ctypes.c_void_p]
        lib.FreeBuffer.restype = None

    @staticmethod
    def _default_library_path() -> str:
        base_dir = os.path.dirname(__file__)
        suffix = {
            "win32": ".dll",
            "darwin": ".dylib",
        }.get(os.sys.platform, ".so")
        return os.path.join(base_dir, f"libskyshelve{suffix}")

    @classmethod
    def _last_error(cls) -> Optional[str]:
        assert cls._lib is not None
        err_ptr = cls._lib.LastError()
        if not err_ptr:
            return None
        try:
            msg = ctypes.string_at(err_ptr).decode("utf-8", "replace")
        finally:
            cls._lib.FreeCString(err_ptr)
        return msg or None

    @classmethod
    def _check_status(cls, status: int) -> None:
        if status == 0:
            return
        msg = cls._last_error() or "unknown skyshelve error"
        raise SkyshelveError(msg)

    @classmethod
    def _open(cls, path: Optional[str], in_memory: bool) -> int:
        assert cls._lib is not None
        if in_memory:
            encoded_path = b""
        else:
            if not path:
                raise ValueError("A filesystem path is required unless in_memory=True")
            encoded_path = path.encode("utf-8")
        handle = cls._lib.Open(encoded_path, int(bool(in_memory)))
        if handle == 0:
            msg = cls._last_error() or "failed to open skyshelve store"
            raise SkyshelveError(msg)
        return int(handle)

    def __getitem__(self, key: Any) -> Any:
        return self.get(key, raise_missing=True)

    def __setitem__(self, key: Any, value: Any) -> None:
        self.set(key, value)

    def __delitem__(self, key: Any) -> None:
        if not self.delete(key):
            raise KeyError(key)

    def __contains__(self, key: Any) -> bool:
        result = self.get(key, default=_MISSING)
        return result is not _MISSING

    def __enter__(self) -> "SkyShelve":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def _call(self, func_name: str, *args) -> int:
        if self._handle == 0:
            raise SkyshelveError("skyshelve store is closed")
        assert self._lib is not None
        func = getattr(self._lib, func_name)
        return func(*args)

    def _encode_key(self, key: Any) -> bytes:
        if isinstance(key, (bytes, bytearray, memoryview)):
            data = bytes(key)
        elif isinstance(key, str):
            data = key.encode("utf-8")
        else:
            data = pickle.dumps(key, protocol=pickle.HIGHEST_PROTOCOL)
        if not data:
            raise ValueError("empty keys are not supported")
        return data

    def _encode_value(self, value: Any) -> bytes:
        if isinstance(value, (bytes, bytearray, memoryview)):
            payload = bytes(value)
            return bytes([_VALUE_RAW]) + payload
        if isinstance(value, str):
            payload = value.encode("utf-8")
            return bytes([_VALUE_STR]) + payload
        if not self._auto_pickle:
            raise TypeError(f"Value type {type(value)!r} is not bytes/str and auto_pickle=False.")
        payload = pickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL)
        return bytes([_VALUE_PICKLED]) + payload

    def _decode_value(self, data: bytes) -> Any:
        if not data:
            return b""
        type_tag = data[0]
        payload = data[1:]
        if type_tag == _VALUE_RAW:
            return payload
        if type_tag == _VALUE_STR:
            return payload.decode("utf-8")
        if type_tag == _VALUE_PICKLED:
            return pickle.loads(payload)
        return data

    def set(self, key: Any, value: Any) -> None:
        key_bytes = self._encode_key(key)
        value_bytes = self._encode_value(value)
        status = self._call(
            "Set",
            ctypes.c_size_t(self._handle),
            ctypes.c_char_p(key_bytes),
            ctypes.c_int(len(key_bytes)),
            ctypes.c_char_p(value_bytes),
            ctypes.c_int(len(value_bytes)),
        )
        self._check_status(status)

    def get(self, key: Any, default: Any = None, *, raise_missing: bool = False) -> Any:
        key_bytes = self._encode_key(key)
        value_len = ctypes.c_int()
        ptr = self._call(
            "Get",
            ctypes.c_size_t(self._handle),
            ctypes.c_char_p(key_bytes),
            ctypes.c_int(len(key_bytes)),
            ctypes.byref(value_len),
        )

        if not ptr and value_len.value == 0:
            msg = self._last_error()
            if msg:
                if "not found" in msg.lower():
                    if raise_missing:
                        raise KeyError(key)
                    return default
                raise SkyshelveError(msg)
            if raise_missing:
                raise KeyError(key)
            return default

        try:
            raw = ctypes.string_at(ptr, value_len.value)
        finally:
            self._lib.FreeBuffer(ptr)
        return self._decode_value(raw)

    def delete(self, key: Any) -> bool:
        key_bytes = self._encode_key(key)
        status = self._call(
            "Delete",
            ctypes.c_size_t(self._handle),
            ctypes.c_char_p(key_bytes),
            ctypes.c_int(len(key_bytes)),
        )
        if status == 0:
            return True
        msg = self._last_error()
        if msg and "not found" in msg.lower():
            return False
        self._check_status(status)
        return True

    def sync(self) -> None:
        status = self._call("Sync", ctypes.c_size_t(self._handle))
        self._check_status(status)

    def scan(self, prefix: Any = None) -> List[Tuple[bytes, Any]]:
        if prefix is None:
            prefix_bytes = b""
        else:
            prefix_bytes = self._encode_key(prefix)

        result_len = ctypes.c_int()
        ptr = self._call(
            "Scan",
            ctypes.c_size_t(self._handle),
            ctypes.c_char_p(prefix_bytes),
            ctypes.c_int(len(prefix_bytes)),
            ctypes.byref(result_len),
        )

        entries: List[Tuple[bytes, Any]] = []
        try:
            length = result_len.value
            if not ptr or length == 0:
                return entries

            raw = ctypes.string_at(ptr, length)
            offset = 0
            while offset < length:
                key_len, value_len = struct.unpack_from("<II", raw, offset)
                offset += 8
                key = raw[offset : offset + key_len]
                offset += key_len
                value_raw = raw[offset : offset + value_len]
                offset += value_len
                entries.append((bytes(key), self._decode_value(value_raw)))
            return entries
        finally:
            if ptr:
                self._lib.FreeBuffer(ptr)

    def _apply(self, operations: Sequence[Tuple[str, bytes, Optional[Any]]]) -> None:
        if not operations:
            return

        buffer = bytearray()
        for op, key, value in operations:
            if not isinstance(key, (bytes, bytearray, memoryview)):
                raise TypeError("operation key must be bytes-like")
            key_bytes = bytes(key)
            if op == "set":
                encoded = self._encode_value(value)
                buffer.append(0)
                buffer += struct.pack("<I", len(key_bytes))
                buffer += key_bytes
                buffer += struct.pack("<I", len(encoded))
                buffer += encoded
            elif op == "delete":
                buffer.append(1)
                buffer += struct.pack("<I", len(key_bytes))
                buffer += key_bytes
            else:
                raise ValueError(f"unknown operation '{op}'")

        arr = (ctypes.c_char * len(buffer)).from_buffer_copy(buffer)
        status = self._call("Apply", ctypes.c_size_t(self._handle), arr, ctypes.c_int(len(buffer)))
        self._check_status(status)

    def close(self) -> None:
        if self._handle == 0:
            return
        status = self._call("Close", ctypes.c_size_t(self._handle))
        self._handle = 0
        if status != 0:
            self._check_status(status)

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:
            pass


BadgerDict = SkyShelve
BadgerError = SkyshelveError


def slatedb_uri(path: str, *, store: Optional[Dict[str, Any]] = None) -> str:
    """Utility to format a SlateDB configuration string for :class:`SkyShelve`.

    Args:
        path: Local cache directory passed to SlateDB.
        store: Optional store configuration dictionary mirroring
            :class:`slatedb.StoreConfig`. Use ``{"provider": "aws", "aws": {...}}``
            for AWS.

    Returns:
        A ``slatedb:`` URI string suitable for ``SkyShelve`` or
        ``PersistentObject`` configuration.
    """

    payload: Dict[str, Any] = {"path": path}
    if store:
        payload["store"] = store
    return f"slatedb:{json.dumps(payload)}"


def _extract_slatedb_cache_root(uri: str) -> Optional[str]:
    if not uri.startswith("slatedb:"):
        return None

    remainder = uri[len("slatedb:") :]
    if remainder.startswith("//"):
        return remainder[2:]

    remainder = remainder.strip()
    if remainder.startswith("{"):
        try:
            payload = json.loads(remainder)
        except json.JSONDecodeError:
            return None
        cache_path = payload.get("path")
        if isinstance(cache_path, str) and cache_path:
            return cache_path
        return None

    return remainder or None


class PersistentObject:
    """Base class for SkyShelve-backed persistent records with inter-process safety.

    Subclasses should override :meth:`to_record` / :meth:`from_record` when the
    default dictionary representation is insufficient. Storage is configured per
    subclass via :meth:`configure_storage` and each operation acquires a file
    lock so processes coordinate access safely.
    """

    _storage_path: ClassVar[Optional[Path]] = None
    _storage_in_memory: ClassVar[bool] = False
    _storage_lib_path: ClassVar[Optional[str]] = None
    _storage_auto_pickle: ClassVar[bool] = True
    _lock_path: ClassVar[Optional[Path]] = None
    _namespace: ClassVar[Optional[str]] = None
    _secondary_indexes: ClassVar[Dict[str, Callable[["PersistentObject"], Iterable[Any]]]] = {}
    _auto_configured: ClassVar[bool] = False

    def __init__(self, key: Any) -> None:
        self._set_persistent_key(key)

    def _set_persistent_key(self, key: Any) -> None:
        object.__setattr__(self, "key", key)

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        if cls is PersistentObject:
            return
        if getattr(cls, "_auto_configured", False):
            return

        path = getattr(cls, "__persistent_path__", None)
        in_memory = getattr(cls, "__persistent_in_memory__", False)
        lib_path = getattr(cls, "__persistent_lib_path__", None)
        auto_pickle = getattr(cls, "__persistent_auto_pickle__", True)
        lock_path = getattr(cls, "__persistent_lock_path__", None)
        namespace = getattr(cls, "__persistent_namespace__", None)
        secondary = getattr(cls, "__persistent_secondary_indexes__", None)

        has_config = (
            path is not None
            or in_memory
            or lib_path is not None
            or lock_path is not None
            or namespace is not None
            or secondary is not None
        )
        if not has_config:
            return

        cls.configure_storage(
            path,
            in_memory=in_memory,
            lib_path=lib_path,
            auto_pickle=auto_pickle,
            lock_path=lock_path,
            namespace=namespace,
            secondary_indexes=secondary,
        )

    # ------------------------------------------------------------------
    # Configuration helpers
    # ------------------------------------------------------------------
    @classmethod
    def configure_storage(
        cls,
        path: Optional[str],
        *,
        in_memory: bool = False,
        lib_path: Optional[str] = None,
        auto_pickle: bool = True,
        lock_path: Optional[str] = None,
        namespace: Optional[str] = None,
        secondary_indexes: Optional[Dict[str, Callable[["PersistentObject"], Iterable[Any]]]] = None,
    ) -> None:
        """Configure the datastore backing this subclass.

        Args:
            path: Filesystem path for the underlying database. Required unless
                ``in_memory`` is True.
            in_memory: Whether to use an in-memory instance.
            lib_path: Optional override pointing at the compiled shared library.
            auto_pickle: Whether values should be automatically pickled.
            lock_path: Optional explicit path to a file used for inter-process
                locking. Defaults to ``<path>.lock`` or a temp file for in-memory
                stores.
            namespace: Optional namespace prefix for keys. Defaults to the
                class name.
        """

        cache_root: Optional[Path] = None

        if not in_memory:
            if not path:
                namespace_hint = namespace or cls.__name__
                default_root = Path.cwd() / "data" / namespace_hint.lower()
                path = str(default_root)
            if path.startswith("slatedb:"):
                cls._storage_path = path
                cache_root_str = _extract_slatedb_cache_root(path)
                if cache_root_str:
                    cache_root = Path(cache_root_str).expanduser().resolve()
                    cache_root.mkdir(parents=True, exist_ok=True)
            else:
                cache_root = Path(path).expanduser().resolve()
                cache_root.mkdir(parents=True, exist_ok=True)
                cls._storage_path = cache_root
        else:
            cache_root = None
            cls._storage_path = None

        cls._storage_in_memory = in_memory
        cls._storage_lib_path = lib_path
        cls._storage_auto_pickle = auto_pickle
        cls._namespace = namespace or cls.__name__

        if lock_path:
            cls._lock_path = Path(lock_path).expanduser().resolve()
        elif in_memory:
            temp_dir = Path(tempfile.gettempdir())
            cls._lock_path = temp_dir / f"skyshelve-{cls.__name__}.lock"
        elif cache_root is not None:
            cls._lock_path = cache_root / f".{cls.__name__.lower()}.lock"
        else:
            temp_dir = Path(tempfile.gettempdir())
            cls._lock_path = temp_dir / f"skyshelve-{cls.__name__}.lock"

        if secondary_indexes:
            cls._ensure_index_dict()
            cls._secondary_indexes.update(secondary_indexes)

        cls._auto_configured = True

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def save(self) -> "PersistentObject":
        """Persist the current state."""

        cls = type(self)

        def _writer(_: "PersistentObject") -> "PersistentObject":
            return self

        cls.update(self.key, default_factory=lambda: self, mutator=_writer)
        return self

    # Index utilities -------------------------------------------------
    @classmethod
    def _ensure_index_dict(cls) -> None:
        if "_secondary_indexes" not in cls.__dict__:
            cls._secondary_indexes = dict(getattr(cls, "_secondary_indexes", {}))

    @classmethod
    def register_index(
        cls,
        name: str,
        extractor: Callable[["PersistentObject"], Iterable[Any]],
    ) -> None:
        cls._ensure_index_dict()
        cls._secondary_indexes[name] = extractor

    @classmethod
    def _index_entries(cls, obj: "PersistentObject") -> Dict[Tuple[str, bytes], Any]:
        entries: Dict[Tuple[str, bytes], Any] = {}
        if not cls._secondary_indexes:
            return entries
        for name, extractor in cls._secondary_indexes.items():
            values = extractor(obj)
            if values is None:
                continue
            if isinstance(values, (str, bytes)) or not isinstance(values, Iterable):
                values_iter = [values]
            else:
                values_iter = list(values)
            for value in values_iter:
                sig = (name, pickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL))
                entries[sig] = value
        return entries

    @classmethod
    def _index_prefix_bytes(cls, index_name: str, value: Any) -> bytes:
        namespace = (cls._namespace or cls.__name__).encode("utf-8")
        index_bytes = index_name.encode("utf-8")
        value_bytes = pickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL)
        buf = bytearray()
        buf.extend(b"IDX")
        buf.extend(struct.pack("<H", len(namespace)))
        buf.extend(namespace)
        buf.extend(struct.pack("<H", len(index_bytes)))
        buf.extend(index_bytes)
        buf.extend(struct.pack("<I", len(value_bytes)))
        buf.extend(value_bytes)
        return bytes(buf)

    @classmethod
    def _index_key_bytes(cls, index_name: str, value: Any, primary_key_bytes: bytes) -> bytes:
        prefix = bytearray(cls._index_prefix_bytes(index_name, value))
        prefix.extend(struct.pack("<I", len(primary_key_bytes)))
        prefix.extend(primary_key_bytes)
        return bytes(prefix)

    @classmethod
    def load(cls, key: Any, default: Any = _MISSING) -> "PersistentObject":
        """Load an instance by key.

        Args:
            key: Identifier originally supplied to the constructor.
            default: Optional fallback returned when the key is missing. If the
                default is not provided a :class:`KeyError` is raised.
        """

        record = cls._get_record(key)
        if record is _MISSING:
            if default is _MISSING:
                raise KeyError(key)
            return default
        return cls.from_record(key, record)

    @classmethod
    def exists(cls, key: Any) -> bool:
        return cls._get_record(key) is not _MISSING

    @classmethod
    def delete(cls, key: Any) -> bool:
        cls._ensure_configured()
        full_key = cls._format_key(key)
        with cls._locked_store() as store:
            record = store.get(full_key, default=_MISSING)
            if record is _MISSING:
                return False
            obj = cls.from_record(key, record)
            primary_bytes = bytes(full_key if isinstance(full_key, (bytes, bytearray, memoryview)) else cls._format_key(key))
            index_entries = cls._index_entries(obj)
            operations: List[Tuple[str, bytes, Optional[Any]]] = []
            for sig, value in index_entries.items():
                index_name = sig[0]
                operations.append(("delete", cls._index_key_bytes(index_name, value, primary_bytes), None))
            operations.append(("delete", primary_bytes, None))
            store._apply(operations)
            return True

    @classmethod
    def scan(cls, predicate: Optional[Callable[[Any], bool]] = None) -> List["PersistentObject"]:
        cls._ensure_configured()
        namespace = cls._namespace or cls.__name__
        results: List[PersistentObject] = []
        with cls._locked_store() as store:
            for raw_key, record in store.scan():
                try:
                    stored_ns, obj_key = pickle.loads(raw_key)
                except Exception:
                    continue
                if stored_ns != namespace:
                    continue
                if predicate and not predicate(obj_key):
                    continue
                results.append(cls.from_record(obj_key, record))
        return results

    @classmethod
    def scan_index(cls, index_name: str, value: Any) -> List["PersistentObject"]:
        cls._ensure_configured()
        if index_name not in cls._secondary_indexes:
            raise KeyError(f"index '{index_name}' is not registered")
        prefix = cls._index_prefix_bytes(index_name, value)
        results: List[PersistentObject] = []
        with cls._locked_store() as store:
            for _, stored_key in store.scan(prefix):
                full_key = cls._format_key(stored_key)
                record = store.get(full_key, default=_MISSING)
                if record is _MISSING:
                    continue
                results.append(cls.from_record(stored_key, record))
        return results

    @classmethod
    def children(cls, index_name: str, value: Any) -> List["PersistentObject"]:
        """Convenience alias for scan_index when treating the index as a foreign key."""

        return cls.scan_index(index_name, value)

    @classmethod
    def update(
        cls,
        key: Any,
        *,
        default_factory: Optional[Any] = None,
        mutator: Optional[Any] = None,
    ) -> "PersistentObject":
        """Atomically load, mutate, and persist an object.

        ``mutator`` receives the current object (creating one via
        ``default_factory`` when missing). Returning ``None`` implies in-place
        mutation and the same object is re-written.
        """

        cls._ensure_configured()
        full_key = cls._format_key(key)

        with cls._locked_store() as store:
            record = store.get(full_key, default=_MISSING)
            if record is _MISSING:
                if default_factory is None:
                    raise KeyError(key)
                candidate = default_factory() if callable(default_factory) else default_factory
                if not isinstance(candidate, cls):
                    raise TypeError("default_factory must produce an instance of the subclass")
                candidate._set_persistent_key(key)
                current = candidate
                previous_entries: Dict[Tuple[str, bytes], Any] = {}
            else:
                current = cls.from_record(key, record)
                previous_entries = cls._index_entries(current)

            if mutator is not None:
                updated = mutator(current)
                if updated is not None:
                    current = updated

            if not isinstance(current, cls):
                raise TypeError("mutator must return an instance of the subclass or None")

            new_entries = cls._index_entries(current)

            primary_bytes = bytes(full_key if isinstance(full_key, (bytes, bytearray, memoryview)) else cls._format_key(key))

            operations: List[Tuple[str, bytes, Optional[Any]]] = []

            for sig, value in previous_entries.items():
                if sig not in new_entries:
                    operations.append(("delete", cls._index_key_bytes(sig[0], value, primary_bytes), None))

            operations.append(("set", primary_bytes, current.to_record()))

            for sig, value in new_entries.items():
                if sig not in previous_entries:
                    operations.append(("set", cls._index_key_bytes(sig[0], value, primary_bytes), key))

            store._apply(operations)
            return current

    # ------------------------------------------------------------------
    # Extensibility hooks
    # ------------------------------------------------------------------
    def to_record(self) -> Any:
        """Convert the instance to a storable representation."""

        payload = dict(self.__dict__)
        payload.pop("key", None)
        return {k: _serialize_field(v) for k, v in payload.items()}

    @classmethod
    def from_record(cls, key: Any, record: Any) -> "PersistentObject":
        instance = cls.__new__(cls)
        try:
            cls.__init__(instance, key)  # type: ignore[misc]
        except TypeError:
            pass
        if isinstance(record, dict):
            instance.__dict__.update({k: _deserialize_field(v) for k, v in record.items()})
        else:
            instance.value = record
        object.__setattr__(instance, "key", key)
        return instance

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    @classmethod
    def _ensure_configured(cls) -> None:
        if cls._storage_path is None and not cls._storage_in_memory:
            raise RuntimeError("PersistentObject storage is not configured")

    @classmethod
    def _format_key(cls, key: Any) -> bytes:
        namespace = cls._namespace or cls.__name__
        return pickle.dumps((namespace, key), protocol=pickle.HIGHEST_PROTOCOL)

    @classmethod
    @contextmanager
    def _locked_store(cls):
        cls._ensure_configured()
        lock_cm = cls._lock_context()
        with lock_cm:
            with cls._open_store() as store:
                yield store

    @classmethod
    def _lock_context(cls):
        if cls._lock_path is None:
            return nullcontext()
        return _FileLock(cls._lock_path)

    @classmethod
    def _open_store(cls):
        if cls._storage_in_memory:
            path = None
        elif cls._storage_path is not None:
            path = str(cls._storage_path)
        else:
            path = None
        return SkyShelve(
            path,
            in_memory=cls._storage_in_memory,
            lib_path=cls._storage_lib_path,
            auto_pickle=cls._storage_auto_pickle,
        )

    @classmethod
    def _get_record(cls, key: Any) -> Any:
        cls._ensure_configured()
        full_key = cls._format_key(key)
        with cls._locked_store() as store:
            result = store.get(full_key, default=_MISSING)
        return result


def _is_pydantic_model(value: Any) -> bool:
    if _PydanticBaseModel is None:
        return False
    return isinstance(value, _PydanticBaseModel)


def _serialize_field(value: Any) -> Any:
    if _is_pydantic_model(value):
        model = value
        return {
            "__type__": "pydantic",
            "__module__": model.__class__.__module__,
            "__qualname__": model.__class__.__qualname__,
            "data": model.model_dump(),
        }
    if dataclasses.is_dataclass(value):
        return {
            "__type__": "dataclass",
            "__module__": value.__class__.__module__,
            "__qualname__": value.__class__.__qualname__,
            "data": dataclasses.asdict(value),
        }
    return value


def _import_qualname(module: str, qualname: str) -> Any:
    mod = importlib.import_module(module)
    obj = mod
    for part in qualname.split("."):
        obj = getattr(obj, part)
    return obj


def _deserialize_field(value: Any) -> Any:
    if isinstance(value, dict) and value.get("__type__") == "pydantic":
        if _PydanticBaseModel is None:  # pragma: no cover - optional dependency missing
            raise RuntimeError("Pydantic is required to deserialize this record")
        model_cls = _import_qualname(value["__module__"], value["__qualname__"])
        if not issubclass(model_cls, _PydanticBaseModel):
            raise TypeError("Serialized model is not a Pydantic BaseModel")
        return model_cls.model_construct(**value["data"])  # type: ignore[attr-defined]
    if isinstance(value, dict) and value.get("__type__") == "dataclass":
        cls = _import_qualname(value["__module__"], value["__qualname__"])
        if not dataclasses.is_dataclass(cls):
            raise TypeError("Serialized object is not a dataclass")
        return cls(**value["data"])
    return value


if _PydanticBaseModel is not None:

    class PersistentBaseModel(PersistentObject, _PydanticBaseModel):
        """Mixin combining PersistentObject with Pydantic BaseModel."""

        __persistent_key_field__ = "id"
        _persistent_key: Any = _PydanticPrivateAttr(None)  # type: ignore[misc]

        def __init__(self, **data):
            key_field = self.__persistent_key_field__
            if key_field not in data:
                raise ValueError(f"Missing primary key field '{key_field}'")
            key_value = data[key_field]
            PersistentObject.__init__(self, key_value)
            _PydanticBaseModel.__init__(self, **data)
            self._persistent_key = key_value

        def _set_persistent_key(self, key: Any) -> None:  # type: ignore[override]
            self._persistent_key = key
            key_field = self.__persistent_key_field__
            if hasattr(self, "__pydantic_fields_set__"):
                try:
                    super().__setattr__(key_field, key)
                except ValueError:
                    pass
            else:
                object.__setattr__(self, key_field, key)

        @property
        def key(self) -> Any:  # type: ignore[override]
            return self._persistent_key

        @key.setter
        def key(self, value: Any) -> None:  # type: ignore[override]
            self._set_persistent_key(value)


    __all__.append("PersistentBaseModel")
def persistent_model(
    *,
    path: Optional[str] = None,
    in_memory: bool = False,
    lib_path: Optional[str] = None,
    auto_pickle: bool = True,
    lock_path: Optional[str] = None,
    namespace: Optional[str] = None,
    secondary_indexes: Optional[Dict[str, Callable[[Any], Iterable[Any]]]] = None,
):
    """Decorator that configures a PersistentObject subclass at definition time."""

    def decorator(cls):
        if not issubclass(cls, PersistentObject):
            raise TypeError("persistent_model decorator requires a PersistentObject subclass")

        cls.configure_storage(
            path,
            in_memory=in_memory,
            lib_path=lib_path,
            auto_pickle=auto_pickle,
            lock_path=lock_path,
            namespace=namespace,
            secondary_indexes=secondary_indexes,
        )
        cls._auto_configured = True
        return cls

    return decorator
