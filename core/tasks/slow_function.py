#!/home/lccww/.conda/envs/conda-env/bin/python

# https://docs.ray.io/en/latest/ray-core/tasks.html

# Ray enables arbitrary functions to be executed 
# asynchronously on separate Python workers. These
# asynchronous Ray functions are called “remote 
# functions”. Here is an example.

import ray
import time

# A regular Python function.
def normal_function():
    print("Execute normal_function.")
    return 1


# By adding the `@ray.remote` decorator, a regular Python function
# becomes a Ray remote function.
@ray.remote
def my_function():
    print("Execute remote my_function.")
    return 1


@ray.remote
def slow_function():
    print("Enter slow_function.")
    time.sleep(10)
    print("Exit slow_function.")
    return 1


if __name__ == "__main__":
    ray.init()

    # To invoke this remote function, use the `remote` method.
    # This will immediately return an object ref (a future) and then create
    # a task that will be executed on a worker process.
    obj_ref = my_function.remote()

    # The result can be retrieved with ``ray.get``.
    assert ray.get(obj_ref) == 1

    # Invocations of Ray remote functions happen in parallel.
    # All computation is performed in the background, driven by Ray's internal event loop.
    for _ in range(4):
        # This doesn't block.
        slow_function.remote()
