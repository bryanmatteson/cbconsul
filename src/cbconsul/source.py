import os
from typing import Any, Callable, Dict, Iterable, List, Optional, Union

import dpath.util
from pydantic import BaseModel
from pydantic.fields import ModelField
from pydantic.utils import deep_update

from .consul import Config, Consul


class ConsulSource:
    def __init__(
        self,
        *,
        case_sensitive: bool = False,
        consul_config: Optional[Config] = None,
        consul_address: Optional[str] = None,
        consul_token: Optional[str] = None,
        consul_basic_auth: Optional[str] = None,
        consul_timeout: Optional[int] = None,
        consul_namespace: Optional[str] = None,
        consul_path: Union[str, Iterable[str], None] = None,
        consul_prefix: Optional[str] = None,
    ) -> None:
        paths = [consul_path] if isinstance(consul_path, str) else list(consul_path or [])
        if consul_prefix:
            paths = ["/".join((p, consul_prefix)) for p in paths]
        self.paths = paths

        if not consul_config:
            consul_config = Config(
                address=consul_address or os.getenv("CONSUL_HTTP_ADDR", "http://localhost:8500"),
                token=consul_token or os.getenv("CONSUL_HTTP_TOKEN", None),
                timeout=consul_timeout or int(os.getenv("CONSUL_HTTP_TIMEOUT", 5)),
                basic_auth=consul_basic_auth or os.getenv("CONSUL_HTTP_AUTH", None),
                namespace=consul_namespace or os.getenv("CONSUL_NAMESPACE", None),
            )

        self.config = consul_config
        self.case_sensitive = case_sensitive

    def __call__(self, model: BaseModel) -> Dict[str, Any]:
        if not self.paths:
            return {}
        result: Dict[str, Any] = {}

        xform = str.lower if not self.case_sensitive else lambda x: x
        with Consul(self.config) as client:
            tree = deep_update(*(client.kv.get_tree(key, recurse=True, key_transform=xform) for key in self.paths))

        for field in model.__fields__.values():
            for name in map(xform, _get_source_names(field, "consul")):
                if value := dpath.util.get(tree, name, separator="/", default=None):
                    result[field.alias] = value
                    break

        return result


def _get_source_names(field: ModelField, extra: str, *, transform: Optional[Callable[[str], str]] = None) -> List[str]:
    source_names: Union[str, Iterable[str]] = field.field_info.extra.get(extra, field.name)
    if isinstance(source_names, str):
        source_names = [source_names]
    else:
        source_names = list(source_names)
    if transform is not None:
        source_names = [transform(name) for name in source_names]
    return source_names
