from __future__ import annotations

import json
from copy import deepcopy
from typing import Any, Dict, List, Optional, Tuple, TypeVar, Union

import httpx

from . import _utils as utils

_T = TypeVar("_T")


class Request:
    def __init__(self, method: str, *paths: str, **kwargs: Any) -> None:
        self.method = method
        self.path = path_join(paths)
        self.content = kwargs.pop("content", None)

        params = kwargs.pop("params", {})
        cleaned_params = {k: v for k, v in params.items() if v is not None}
        self.params = deepcopy(cleaned_params)
        self.headers = deepcopy(kwargs.pop("headers", {}))
        self.json = kwargs.pop("json", None)


class Response:
    def __init__(self, method: str, path: str, status: int, content: Any, headers: Dict[str, Any]):
        self.method = method
        self.path = path
        self.status = status
        self.content = content
        self.headers = headers

    @property
    def consul_index(self) -> str:
        return self.headers.get("X-Consul-Index", 0)

    @property
    def known_leader(self) -> str:
        return self.headers.get("X-Consul-KnownLeader", "")

    @property
    def last_contact(self) -> str:
        return self.headers.get("X-Consul-LastContact", "")

    @property
    def token(self) -> str:
        return self.headers.get("X-Consul-Token", "")

    @property
    def translate_addresses(self) -> bool:
        return self.headers.get("X-Consul-Translate-Addresses", None)


class Adapter:
    _session: Optional[httpx.Client]

    def __init__(
        self,
        base_url: str,
        *,
        auth: Optional[httpx.Auth] = None,
        namespace: Optional[str] = None,
        timeout: Optional[int] = None,
    ) -> None:
        self._base_url = base_url
        self._auth = auth
        self._timeout = timeout
        self._base_url = base_url
        self._headers: Dict[str, Any] = {}
        if namespace:
            self._headers["X-Consul-Namespace"] = namespace
        self._session = None

    @property
    def session(self) -> httpx.Client:
        if not self._session:
            self._session = httpx.Client(
                base_url=self._base_url,
                timeout=self._timeout,
                headers=self._headers.copy(),
                auth=self._auth,
                follow_redirects=True,
            )
        return self._session

    def __enter__(self) -> Adapter:
        self.session.__enter__()
        return self

    def __exit__(self, *exc_info: Any) -> None:
        self.session.__exit__(*exc_info)
        self._session = None

    def close(self) -> None:
        if self._session:
            self._session.close()
            self._session = None

    def request(self, req: Request, **kwargs: Any) -> Response:
        kwargs = {
            k: v
            for k, v in dict(
                content=req.content,
                params=req.params,
                headers=req.headers,
                json=req.json,
                **kwargs,
            ).items()
            if v is not None
        }

        http_request = self.session.build_request(req.method, req.path, **kwargs)
        http_response = self.session.send(http_request)
        if http_response.status_code == 404:
            http_response.close()
            body = None
        else:
            body = http_response.read()
            if http_response.headers.get("Content-Type") == "application/json":
                body = json.loads(http_response.text)

        utils.raise_for_status_error(http_response)

        return Response(
            method=req.method,
            path=http_response.url.path,
            status=http_response.status_code,
            content=body,
            headers=dict(http_response.headers),
        )


def path_join(path: Union[str, List[str], Tuple[str, ...]]) -> str:
    if isinstance(path, (list, tuple)):
        path = "".join(path_join(p) for p in path)
    path = "/" + path
    return path.replace("//", "/")
