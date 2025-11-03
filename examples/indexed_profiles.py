"""Demonstrate parent/child relationships with Pydantic models and secondary indexes."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import List
from uuid import uuid4

from pydantic import BaseModel, Field, field_validator

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from skyshelve import PersistentBaseModel


class ToDo(PersistentBaseModel):
    __persistent_key_field__ = "id"
    __persistent_path__ = str(PROJECT_ROOT / "data" / "profiles" / "todos")
    __persistent_secondary_indexes__ = {"user": lambda todo: [todo.user_id]}

    id: str
    user_id: str
    title: str
    done: bool = False

    def __init__(self, **data):
        data.setdefault("id", str(uuid4()))
        super().__init__(**data)


class UserProfile(PersistentBaseModel):
    __persistent_key_field__ = "username"
    __persistent_path__ = str(PROJECT_ROOT / "data" / "profiles" / "users")
    __persistent_secondary_indexes__ = {
        "email": lambda profile: [profile.email],
        "tag": lambda profile: profile.tags,
    }

    username: str
    email: str
    tags: List[str] = Field(default_factory=list)

    @field_validator("email")
    @classmethod
    def normalize_email(cls, value: str) -> str:
        return value.lower()

    def todos(self) -> List[ToDo]:
        return ToDo.children("user", self.username)


def seed_data() -> None:
    for profile in (
        UserProfile(username="alice", email="ALICE@example.com", tags=["admin", "team-a"]),
        UserProfile(username="bob", email="bob@example.com", tags=["team-b"]),
        UserProfile(username="charlie", email="charlie@example.com", tags=["team-a"]),
    ):
        profile.save()

    todos = [
        ToDo(user_id="alice", title="Review pull requests"),
        ToDo(user_id="alice", title="Plan sprint"),
        ToDo(user_id="charlie", title="Write documentation"),
        ToDo(user_id="charlie", title="Fix bug #123", done=True),
    ]
    for todo in todos:
        todo.save()


def main() -> None:
    seed_data()

    print("-- Scan all profiles --")
    for profile in UserProfile.scan():
        print(profile.model_dump())

    print("\n-- Lookup by normalized email --")
    for profile in UserProfile.scan_index("email", "alice@example.com"):
        print(profile.model_dump())

    print("\n-- Lookup by tag 'team-a' via children helper --")
    for profile in UserProfile.children("tag", "team-a"):
        print(profile.model_dump())

    print("\n-- Todos for alice --")
    for todo in UserProfile.load("alice").todos():
        print(todo.model_dump())


if __name__ == "__main__":
    main()
