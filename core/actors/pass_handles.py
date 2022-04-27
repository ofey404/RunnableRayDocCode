#!/home/lccww/.conda/envs/conda-env/bin/python

# https://docs.ray.io/en/latest/ray-core/actors.html

import ray
import time

from counter import Counter


@ray.remote
def f(counter):
    for _ in range(1000):
        time.sleep(0.1)
        counter.increment.remote()


if __name__ == "__main__":
    ray.init()

    counter = Counter.remote()

    print("Pass counter to 3 tasks, each task increase it every 0.1 s.")
    # Start some tasks that use the actor.
    [f.remote(counter) for _ in range(3)]

    print("Print counter value every 1 second:")
    # Print the counter value.
    for _ in range(10):
        time.sleep(1)
        print(ray.get(counter.get_counter.remote()))
