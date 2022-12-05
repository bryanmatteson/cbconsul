from __future__ import annotations

import contextlib
from base64 import b64decode
from dataclasses import asdict, dataclass, field, fields
from typing import Any, Callable, Dict, Generic, Iterator, List, Literal, Mapping, Optional, Tuple, TypeVar, cast

from cbasyncio import Asyncer

from ._adapter import Adapter, Request
from ._utils import camel_to_snake, flatdict_to_dict

_T = TypeVar("_T")


class KV:
    def __init__(self, adapter: Adapter, *, prefix: Optional[str] = None) -> None:
        self._adapter = adapter
        self._prefix = prefix

    @property
    def prefix(self) -> Optional[str]:
        return self._prefix

    @prefix.setter
    def prefix(self, prefix: Optional[str]) -> None:
        self._prefix = prefix

    @contextlib.contextmanager
    def prefixed(self, prefix: str) -> Iterator[KV]:
        yield KV(self._adapter, prefix=prefix)

    def get(self, key: str, *, raw: Optional[bool] = None) -> Record:
        return self.apply(Operation.get(key, raw=raw))

    def get_records(self, prefix: str, *, recurse: bool = False, separator: str = "/") -> List[Record]:
        return self.apply(Operation.get_tree(prefix, recurse=recurse, separator=separator))

    def get_tree(
        self,
        prefix: str,
        *,
        recurse: bool = False,
        separator: str = "/",
        key_transform: Optional[Callable[[str], str]] = None,
    ) -> Dict[str, Any]:
        prefix = prefix + separator if not prefix.endswith(separator) else prefix
        index = prefix.count(separator)
        records = self.get_records(prefix, recurse=recurse, separator=separator)
        tree = flatdict_to_dict(
            {
                tuple(record.key.split(separator)[index:]): b64decode(record.value).decode("utf-8")
                if record.value is not None
                else None
                for record in records
            }
        )
        if key_transform is not None:
            tree = {key_transform(k): v for k, v in tree.items()}
        return tree

    def list_tree(self, prefix: str, *, recurse: bool = False, separator: str = "/") -> List[str]:
        prefix = prefix + separator if not prefix.endswith(separator) else prefix
        return self.apply(Operation.list_tree(prefix, recurse, separator))

    def set(self, key: str, value: Any, *, flags: Optional[int] = None) -> bool:
        return self.apply(Operation.set(key, value, flags=flags))

    def set_cas(self, key: str, value: Any, *, index: int, flags: Optional[int] = None) -> bool:
        return self.apply(Operation.set_cas(key, value, flags=flags, index=index))

    def lock(self, key: str, value: str, flags: Optional[int] = None) -> bool:
        return self.apply(Operation.lock(key, value, flags=flags))

    def unlock(self, key: str, value: str, flags: Optional[int] = None) -> bool:
        return self.apply(Operation.unlock(key, value, flags=flags))

    def delete(self, key: str) -> bool:
        return self.apply(Operation.delete(key))

    def delete_cas(self, key: str, *, index: int) -> bool:
        return self.apply(Operation.delete_cas(key, index=index))

    def delete_tree(self, prefix: str) -> bool:
        return self.apply(Operation.delete_tree(prefix))

    def apply(self, op: Operation[_T], default: Optional[_T] = None) -> _T:
        key = self.prefix or "" + op.key
        request = Request(_get_method(op), "kv", key, params=op.params_dict(), content=op.value)
        response = self._adapter.request(request)
        if response.content is None:
            if default is None:
                raise KeyError(op.key)
            return default
        return cast(_T, _decode_content(op, response.content))


class AsyncKV(Asyncer[KV]):
    @contextlib.contextmanager
    def prefixed(self, prefix: str) -> Iterator[AsyncKV]:
        with self.raw.prefixed(prefix) as kv:
            yield AsyncKV(kv)

    async def get(self, key: str, *, raw: Optional[bool] = None) -> Record:
        return await self.run_sync(self.raw.get, key, raw=raw)

    async def get_records(self, prefix: str, *, recurse: bool = False, separator: str = "/") -> List[Record]:
        return await self.run_sync(self.raw.get_records, prefix, recurse=recurse, separator=separator)

    async def get_tree(
        self,
        prefix: str,
        *,
        recurse: bool = False,
        separator: str = "/",
        key_transform: Optional[Callable[[str], str]] = None,
    ) -> Dict[str, Any]:
        return await self.run_sync(
            self.raw.get_tree,
            prefix,
            recurse=recurse,
            separator=separator,
            key_transform=key_transform,
        )

    async def list_tree(self, prefix: str, *, recurse: bool = False, separator: str = "/") -> List[str]:
        return await self.run_sync(self.raw.list_tree, prefix, recurse=recurse, separator=separator)

    async def set(self, key: str, value: Any, *, flags: Optional[int] = None) -> bool:
        return await self.run_sync(self.raw.set, key, value, flags=flags)

    async def set_cas(self, key: str, value: Any, *, index: int, flags: Optional[int] = None) -> bool:
        return await self.run_sync(self.raw.set_cas, key, value, flags=flags, index=index)

    async def lock(self, key: str, value: str, flags: Optional[int] = None) -> bool:
        return await self.run_sync(self.raw.lock, key, value, flags=flags)

    async def unlock(self, key: str, value: str, flags: Optional[int] = None) -> bool:
        return await self.run_sync(self.raw.unlock, key, value, flags=flags)

    async def delete(self, key: str) -> bool:
        return await self.run_sync(self.raw.delete, key)

    async def delete_cas(self, key: str, *, index: int) -> bool:
        return await self.run_sync(self.raw.delete_cas, key, index=index)

    async def delete_tree(self, prefix: str) -> bool:
        return await self.run_sync(self.raw.delete_tree, prefix)


Verb = Literal[
    "set",
    "cas",
    "lock",
    "unlock",
    "get",
    "acquire",
    "release",
    "get-tree",
    "delete",
    "delete-tree",
    "delete-cas",
]


def _get_method(op: Operation) -> str:
    return (
        "DELETE"
        if op.verb in ("delete-cas", "delete-tree", "delete")
        else "PUT"
        if op.verb in ("set", "cas", "lock", "unlock")
        else "GET"
    )


@dataclass(frozen=True)
class Operation(Generic[_T]):
    verb: Verb
    key: str
    value: Optional[bytes] = field(default=None)

    raw: Optional[bool] = field(default=None)
    separator: Optional[str] = field(default=None)
    keys: Optional[bool] = field(default=None)
    recurse: Optional[bool] = field(default=None)
    index: Optional[int] = field(default=None)
    wait: Optional[str] = field(default=None)
    flags: Optional[int] = field(default=None)
    cas: Optional[int] = field(default=None)
    acquire: Optional[str] = field(default=None)
    release: Optional[str] = field(default=None)

    def params_dict(self) -> Dict[str, Any]:
        exportable = set(f.name for f in fields(self)).difference(("verb", "key", "value"))

        def export(items: List[Tuple[str, Any]]) -> Dict[str, Any]:
            return dict((k, v) for k, v in items if k in exportable and v is not None)

        return asdict(self, dict_factory=export)

    @staticmethod
    def get(key: str, *, raw: Optional[bool] = None) -> Operation[Record]:
        return Operation("get", key, raw=raw)

    @staticmethod
    def get_tree(prefix: str, recurse: bool = False, separator: str = "/") -> Operation[List[Record]]:
        return Operation("get-tree", prefix, recurse=True, separator="" if recurse else separator)

    @staticmethod
    def list_tree(prefix: str, recurse: bool = False, separator: str = "/") -> Operation[List[str]]:
        return Operation("get-tree", prefix, keys=True, separator="" if recurse else separator)

    @staticmethod
    def set(key: str, value: Any, flags: Optional[int] = None) -> Operation[bool]:
        return Operation("set", key, value, flags=flags)

    @staticmethod
    def set_cas(key: str, value: Any, index: int, flags: Optional[int] = None) -> Operation[bool]:
        return Operation("cas", key, value, flags=flags, cas=index)

    @staticmethod
    def lock(key: str, value: str, flags: Optional[int] = None) -> Operation[bool]:
        return Operation("acquire", key, value.encode("utf-8"), acquire=value, flags=flags)

    @staticmethod
    def unlock(key: str, value: str, flags: Optional[int] = None) -> Operation[bool]:
        return Operation("release", key, value.encode("utf-8"), release=value, flags=flags)

    @staticmethod
    def delete(key: str) -> Operation[bool]:
        return Operation("delete", key)

    @staticmethod
    def delete_cas(key: str, *, index: int) -> Operation[bool]:
        return Operation("delete-cas", key, cas=index)

    @staticmethod
    def delete_tree(prefix: str) -> Operation[bool]:
        return Operation("delete-tree", prefix, recurse=True)


def _decode_content(op: Operation, content: Any):
    if isinstance(content, (bool, str, bytes)):
        return content
    if isinstance(content, (list, set, tuple)):
        if all(isinstance(o, str) for o in content):
            return cast(List[str], list(content))
        if all(isinstance(o, Mapping) for o in content):
            c = cast(List[Dict[str, Any]], content)
            content_list = [Record(**{camel_to_snake(k): v for k, v in o.items()}) for o in c]
            if op.verb in ("get", "watch"):
                return next(iter(content_list), content_list)
            return content_list

    return None


@dataclass
class Record:
    key: str
    create_index: int = 0
    modify_index: int = 0
    lock_index: int = 0
    flags: int = 0
    value: str = ""
    session: str = ""
