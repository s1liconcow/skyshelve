import ctypes
import os
import pickle
import threading
from typing import Any, Optional, Union


BytesLike = Union[bytes, bytearray, memoryview, str]
_MISSING = object()
_VALUE_RAW = 0x00
_VALUE_STR = 0x01
_VALUE_PICKLED = 0x02


class BadgerError(Exception):
    """Raised when the underlying Badger interaction fails."""


class BadgerDict:
    """Minimal dictionary-style wrapper backed by Badger through a Go shared library."""

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
        return os.path.join(base_dir, f"libbadgerdict{suffix}")

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
        msg = cls._last_error() or "unknown badger error"
        raise BadgerError(msg)

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
            msg = cls._last_error() or "failed to open badger dictionary"
            raise BadgerError(msg)
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

    def __enter__(self) -> "BadgerDict":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def _call(self, func_name: str, *args) -> int:
        if self._handle == 0:
            raise BadgerError("badger dictionary is closed")
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
                raise BadgerError(msg)
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
