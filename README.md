## cbconsul - Lightweight Consul KV Client

A simple, async/sync consul client library.

## :rocket: Quick Start

```python
from cbconsul import create_sync_consul

with create_sync_consul("localhost:8500", prefix="example-job/config/") as consul:
    test_value = consul.kv.get("test_value")
    config = consul.kv.get_tree("") # everything under `example-job/config` as a python Dict
```

Of course, the libray is async-first, so there's an async equivalent for everything
```python
import asyncio
from cbconsul import Consul

async def main():
    async with Consul("localhost:8500", prefix="example-job/config/") as consul:
        test_value = await consul.kv.get("test_value")
        config = await consul.kv.get_tree("") # everything under `example-job/config` as a python Dict

asyncio.run(main())
```
