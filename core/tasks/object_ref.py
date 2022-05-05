#!/home/lccww/.conda/envs/conda-env/bin/python

# https://docs.ray.io/en/latest/ray-core/tasks.html

# Object refs can also be passed into remote functions.
# When the function actually gets executed, the argument
# will be passed as the underlying Python value. For 
# example, take this function:

import ray
import time

from slow_function import my_function, slow_function


@ray.remote
def function_with_an_argument(value):
    print("Called function_with_an_argument.")
    return value + 1


if __name__ == "__main__":

    ray.init()

    obj_ref1 = my_function.remote()

    print(f"obj_ref1 = {obj_ref1}")
    assert ray.get(obj_ref1) == 1

    # You can pass an object ref as an argument to another Ray remote function.
    obj_ref2 = function_with_an_argument.remote(obj_ref1)
    assert ray.get(obj_ref2) == 2

    # At data dependency, remote function would wait for its argument.
    obj_ref3 = slow_function.remote()
    print("slow_function won't block")
    obj_ref4 = function_with_an_argument.remote(obj_ref3)
    assert ray.get(obj_ref4) == 2

    
