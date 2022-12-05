from __future__ import annotations

import os
from typing import Any, Optional, Union, overload

import httpx
from cbasyncio import AsyncerContextManager
from pydantic import BaseModel

from ._adapter import Adapter
from ._auth import TokenAuth
from .kv import KV, AsyncKV


class Config(BaseModel):
    address: str
    token: Optional[str] = None
    timeout: Optional[int] = None
    namespace: Optional[str] = None
    basic_auth: Optional[str] = None
    prefix: Optional[str] = None


class Consul:
    _adapter: Adapter
    _kv: KV

    @overload
    def __init__(
        self,
        address: Optional[str] = ...,
        /,
        *,
        token: Optional[str] = ...,
        timeout: Optional[int] = ...,
        basic_auth: Optional[str] = ...,
        namespace: Optional[str] = ...,
        prefix: Optional[str] = ...,
    ) -> None:
        ...

    @overload
    def __init__(self, config: Config, /) -> None:
        ...

    def __init__(
        self,
        config: Union[str, Config, None] = None,
        /,
        *,
        token: Optional[str] = None,
        timeout: Optional[int] = None,
        basic_auth: Optional[str] = None,
        namespace: Optional[str] = None,
        prefix: Optional[str] = None,
    ) -> None:
        if isinstance(config, Config):
            config = config
        else:
            address = config or os.getenv("CONSUL_HTTP_ADDR", "http://localhost:8500")
            token = token or os.getenv("CONSUL_HTTP_TOKEN", None)
            timeout = timeout or int(os.getenv("CONSUL_HTTP_TIMEOUT", 5))
            basic_auth = basic_auth or os.getenv("CONSUL_HTTP_AUTH", None)
            namespace = namespace or os.getenv("CONSUL_NAMESPACE", None)
            config = Config(address=address, token=token, basic_auth=basic_auth)

        if config.token:
            auth = TokenAuth(config.token)
        elif config.basic_auth:
            auth = httpx.BasicAuth(*config.basic_auth.split(":", 1))
        else:
            auth = None

        address = config.address.rstrip("/") + "/v1"
        self._adapter = Adapter(base_url=address, auth=auth, namespace=namespace, timeout=config.timeout)
        self._kv = KV(self._adapter, prefix=config.prefix)

    @property
    def kv(self) -> KV:
        return self._kv

    def __enter__(self) -> Consul:
        self._adapter.__enter__()
        return self

    def __exit__(self, *args):
        self._adapter.__exit__(*args)

    def close(self) -> None:
        self._adapter.close()


class AsyncConsul(AsyncerContextManager[Consul]):
    _kv: AsyncKV

    @overload
    def __init__(
        self,
        address: Optional[str] = ...,
        /,
        *,
        token: Optional[str] = ...,
        timeout: Optional[int] = ...,
        basic_auth: Optional[str] = ...,
        namespace: Optional[str] = ...,
        prefix: Optional[str] = ...,
    ) -> None:
        ...

    @overload
    def __init__(self, config: Config, /) -> None:
        ...

    @overload
    def __init__(self, consul: Consul, /) -> None:
        ...

    def __init__(
        self,
        consul: Union[Consul, Config, str, None] = None,
        /,
        **kwargs: Any,
    ) -> None:
        if not isinstance(consul, Consul):
            consul = Consul(consul, **kwargs)
        super().__init__(consul)
        self._kv = AsyncKV(self.raw.kv)

    @property
    def kv(self) -> AsyncKV:
        return self._kv
