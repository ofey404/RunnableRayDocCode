#!/bin/env python

# https://docs.ray.io/en/latest/ray-core/actors/async_api.html#threaded-actors

# You can use the max_concurrency Actor options without
# any async methods, allowng you to achieve threaded
# concurrency (like a thread pool).

# WARNING:
# When there is at least one async def method in actor
# definition, Ray will recognize the actor as AsyncActor
# instead of ThreadedActor.

import ray
import time


@ray.remote
class ThreadedActor:
    def task_1(self):
        print("I'm running in a thread!")
        time.sleep(2)

    def task_2(self):
        print("I'm running in another thread!")
        time.sleep(2)


def main():
    # Each invocation of the threaded actor will be running in a thread pool. 
    a = ThreadedActor.options(max_concurrency=2).remote()

    start = time.time()
    ray.get([a.task_1.remote(), a.task_2.remote()])
    print(f"threaded actor use {time.time() - start} s to return.")


if __name__ == "__main__":
    ray.init()
    main()
