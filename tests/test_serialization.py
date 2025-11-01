from datetime import datetime

import pytest


def test_store_various_types(badger_dict_factory):
    payload = {"numbers": [1, 2, 3], "ts": datetime(2025, 11, 1, 12, 0, 0)}
    with badger_dict_factory(in_memory=True) as store:
        store["raw-bytes"] = b"\x00\x01"
        store["text"] = "hello"
        store["object"] = payload
        store[("tuple", 1)] = {"foo": "bar"}

        assert store["raw-bytes"] == b"\x00\x01"
        assert store["text"] == "hello"
        assert store["object"] == payload
        assert store[("tuple", 1)] == {"foo": "bar"}
        assert ("tuple", 1) in store


def test_auto_pickle_disabled(shared_library, tmp_path):
    from badgerdict import BadgerDict

    store = BadgerDict(None, in_memory=True, lib_path=str(shared_library), auto_pickle=False)
    store["raw"] = b"bytes-ok"
    store["text"] = "hi"
    assert store["raw"] == b"bytes-ok"
    assert store["text"] == "hi"
    with pytest.raises(TypeError):
        store["obj"] = {"a": 1}
    store.close()
