#!/bin/env python

# https://docs.ray.io/en/latest/ray-core/actors/async_api.html

# Ray natively integrates with asyncio. You can use ray alongside
# with popular async frameworks like aiohttp, aioredis, etc.

import time
import ray
import asyncio

@ray.remote
class AsyncActor:
    # multiple invocation of this method can be running in
    # the event loop at the same time
    async def run_concurrent(self):
        print("started")
        await asyncio.sleep(2) # concurrent workload here
        print("finished")

async def main():
    actor = AsyncActor.remote()

    # regular ray.get
    start = time.time()
    ray.get([actor.run_concurrent.remote() for _ in range(4)])
    print(f"regular ray.get use {time.time() - start} s to return.")

    # async ray.get
    start = time.time()
    await actor.run_concurrent.remote()
    print(f"async ray.get use {time.time() - start} s to return.")


# ## Ray don’t support asyncio for remote tasks.
# 
# Instead, you can wrap the async function with a wrapper to
# run the task synchronously:

ASYNCIO_FOR_REMOTE_TASK=False
if ASYNCIO_FOR_REMOTE_TASK:  # Wrong!
    @ray.remote
    async def this_def_will_fail():
        pass
else:  # Correct method.
    async def run_it_synchronously():
        pass

    @ray.remote
    def wrapper():
        import asyncio
        asyncio.get_event_loop().run_until_complete(run_it_synchronously())

if __name__ == "__main__":
    ray.init()

    asyncio.run(main())
    