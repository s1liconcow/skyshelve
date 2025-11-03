import multiprocessing
from pathlib import Path

import pytest

from skyshelve import PersistentObject


class Note(PersistentObject):
    def __init__(self, key: str, text: str = "") -> None:
        super().__init__(key)
        self.text = text
        self.tags = []

def note_tags(obj: Note):
    return obj.tags


def counter_value_index(obj: "Counter"):
    return [obj.value]


class Counter(PersistentObject):
    def __init__(self, key: str, value: int = 0) -> None:
        super().__init__(key)
        self.value = value

    def to_record(self):  # type: ignore[override]
        return {"value": self.value}

    @classmethod
    def from_record(cls, key, record):  # type: ignore[override]
        return cls(key, int(record.get("value", 0)))

    @classmethod
    def increment(cls, key: str) -> "Counter":
        def mutator(obj: "Counter") -> None:
            obj.value += 1

        return cls.update(key, default_factory=lambda: cls(key, 0), mutator=mutator)


def _counter_worker(db_path: str, lib_path: str, iterations: int) -> None:
    Counter.configure_storage(db_path, lib_path=lib_path, secondary_indexes={"value": counter_value_index})
    for _ in range(iterations):
        Counter.increment("global")


def test_persistent_object_roundtrip(tmp_path, shared_library):
    db_path = tmp_path / "notes"
    Note.configure_storage(str(db_path), lib_path=str(shared_library), secondary_indexes={"tag": note_tags})

    note = Note("n1", "hello world")
    note.tags = ["demo", "test"]
    note.save()

    loaded = Note.load("n1")
    assert loaded.text == "hello world"
    assert loaded.tags == ["demo", "test"]

    scans = Note.scan()
    assert len(scans) == 1
    assert scans[0].text == "hello world"

    tag_matches = Note.scan_index("tag", "demo")
    assert len(tag_matches) == 1
    assert tag_matches[0].text == "hello world"
    via_children = Note.children("tag", "demo")
    assert len(via_children) == 1
    assert via_children[0].text == "hello world"

    assert Note.exists("n1")
    assert Note.delete("n1")
    with pytest.raises(KeyError):
        Note.load("n1")


def test_persistent_object_multi_process(tmp_path, shared_library):
    db_path = tmp_path / "counter"
    Counter.configure_storage(str(db_path), lib_path=str(shared_library), secondary_indexes={"value": counter_value_index})

    ctx = multiprocessing.get_context("spawn")
    procs = [
        ctx.Process(target=_counter_worker, args=(str(db_path), str(shared_library), 50))
        for _ in range(4)
    ]
    for proc in procs:
        proc.start()
    for proc in procs:
        proc.join()
        assert proc.exitcode == 0

    result = Counter.load("global")
    assert result.value == 4 * 50

    multiples = Counter.scan(predicate=lambda k: isinstance(k, str))
    assert multiples and multiples[0].value == result.value

    value_matches = Counter.scan_index("value", result.value)
    assert len(value_matches) == 1
    assert value_matches[0].value == result.value
    child_matches = Counter.children("value", result.value)
    assert child_matches and child_matches[0].value == result.value
