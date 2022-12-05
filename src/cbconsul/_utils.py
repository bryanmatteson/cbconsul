import os
import re
import textwrap
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional, Tuple, Type, TypeVar, Union, cast

import httpx

from . import _errors


def get_consul_address() -> str:
    addr = os.getenv("CONSUL_ADDR") or os.getenv("CONSUL_HTTP_ADDR")
    if not addr:
        host = os.getenv("CONSUL_HOST", "localhost")
        port = os.getenv("CONSUL_PORT", 8500)
        scheme = os.getenv("CONSUL_SCHEME", "http")
        addr = f"{scheme}://{host}:{port}"
    return addr


def get_consul_token() -> Optional[str]:
    token = os.getenv("CONSUL_TOKEN") or os.getenv("CONSUL_HTTP_TOKEN")
    if not token:
        token_file_path = Path("~/.consul-token").expanduser()
        if token_file_path.exists() and token_file_path.is_file():
            token = token_file_path.read_text().strip()

    return token


def raise_for_status_error(response: httpx.Response, allow_404: bool = True):
    err = """There was an error in the request that was made to consul.
    {0.status_code}: "{0.reason_phrase}" for url "{0.url}"
    Response content: {0.text}
    {1}"""

    meta = extract_meta(response.headers)
    if response.is_client_error:
        if response.status_code == 400:
            raise _errors.BadRequest(err.format(response, meta))
        elif response.status_code == 401:
            raise _errors.ACLDisabled(err.format(response, meta))
        elif response.status_code == 403:
            raise _errors.Forbidden(err.format(response, meta))
        elif response.status_code == 404:
            if not allow_404:
                raise _errors.NotFound(err.format(response, meta))
        elif response.status_code == 409:
            raise _errors.ConflictError()
        else:
            raise _errors.RequestError(err.format(response, meta))
    elif response.is_server_error:
        raise _errors.ServerError(err.format(response, meta))


_KT = TypeVar("_KT")
_VT = TypeVar("_VT")


def flatdict_to_dict(dct: Dict[Tuple[_KT, ...], _VT]) -> Dict[_KT, Union[_VT, Dict[_KT, _VT]]]:
    typ = cast(Type[Dict[_KT, _VT]], type(dct))
    result = cast(Dict[_KT, Union[_VT, Dict[_KT, _VT]]], typ())
    for key_tuple, value in dct.items():
        current_dict: Dict[_KT, Union[_VT, Dict[_KT, _VT]]] = result
        for prefix_key in key_tuple[:-1]:
            current_dict = cast(Dict[_KT, Union[_VT, Dict[_KT, _VT]]], current_dict.setdefault(prefix_key, typ()))
        current_dict[key_tuple[-1]] = value

    return result


_CTS_1 = re.compile("(.)([A-Z][a-z]+)", re.ASCII)
_CTS_2 = re.compile("([a-z0-9])([A-Z])", re.ASCII)


def camel_to_snake(name: str) -> str:
    name = re.sub(_CTS_1, r"\1_\2", name)
    return re.sub(_CTS_2, r"\1_\2", name).lower()


@dataclass
class Metadata:
    index: int
    known_leader: str
    last_contact: str
    token: str
    translate_addresses: bool

    def __str__(self) -> str:
        return textwrap.dedent(
            f"""
            X-Consul-Index: {self.index}
            X-Consul-KnownLeader: {self.known_leader}
            X-Consul-LastContact: {self.last_contact}
            X-Consul-Token: {self.token}
            X-Consul-Translate-Addresses: {self.translate_addresses}"""
        )

    __repr__ = __str__


def extract_meta(headers: httpx.Headers) -> Metadata:
    index = headers.get("X-Consul-Index", 0)
    known_leader = headers.get("X-Consul-KnownLeader", "")
    last_contact = headers.get("X-Consul-LastContact", "")
    token = headers.get("X-Consul-Token", "")
    translate_addresses = headers.get("X-Consul-Translate-Addresses", None)

    return Metadata(index, known_leader, last_contact, token, translate_addresses)
