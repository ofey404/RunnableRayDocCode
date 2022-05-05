#!/bin/env python

# https://docs.ray.io/en/latest/ray-core/actors/concurrency_group_api.html

# Ray allows methods to be separated into concurrency groups,
# each with its own asyncio event loop.

import ray

ray.init()


@ray.remote(concurrency_groups={"io": 2, "compute": 4})
class AsyncIOActor:
    def __init__(self):
        pass

    @ray.method(concurrency_group="io")
    async def f1(self):
        pass

    @ray.method(concurrency_group="io")
    async def f2(self):
        pass

    @ray.method(concurrency_group="compute")
    async def f3(self):
        pass

    @ray.method(concurrency_group="compute")
    async def f4(self):
        pass

    async def f5(self):
        pass


a = AsyncIOActor.options(lifetime="detached").remote()
a.f1.remote()  # executed in the "io" group.
a.f2.remote()  # executed in the "io" group.
a.f3.remote()  # executed in the "compute" group.
a.f4.remote()  # executed in the "compute" group.
a.f5.remote()  # executed in the default group.


# ## Default Concurrency Group
#
# By default, methods are placed in a default concurrency group
# which has a concurrency limit of 1000 in Python, 1 in Java.

# The following AsyncIOActor has 2 concurrency groups: “io” and “default”.
# The max concurrency of “io” is 2, and the max concurrency of “default” is 10.
@ray.remote(concurrency_groups={"io": 2})
class AsyncIOActor:
    async def f1(self):
        pass


actor = AsyncIOActor.options(max_concurrency=10).remote()
